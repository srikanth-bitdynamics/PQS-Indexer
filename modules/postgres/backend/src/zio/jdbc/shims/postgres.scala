// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package zio.jdbc.shims

import com.digitalasset.pqs.o11y.metrics.latency
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.postgres.backend.PostgresConfig
import zio.jdbc.*
import zio.metrics.MetricKeyType.Histogram.Boundaries
import zio.{Exit, Schedule, UIO, ZIO, ZLayer, ZPool}

import java.sql.Connection
import scala.language.implicitConversions

object postgres {
  val jdbcUpGauge = zio.metrics.Metric.gauge("jdbc_conn_pool_up", "JDBC connection pool is up")

  private val connectionIsValidLatency = latency("jdbc_conn_isvalid", "Latency of database connection validation")
  private val connectionUsageLatency =
    zio.metrics.Metric
      .histogram(
        "jdbc_conn_use",
        "Latency of database connections usage",
        Boundaries.exponential(0.001, math.pow(10, 1.0 / 3), 13)
      )
      .contramap[Long](in => in / 1e9)
  private val trackSuccess = connectionUsageLatency.tagged("result", "success")
  private val trackFailure = connectionUsageLatency.tagged("result", "failure")

  class PGRestorableConnection(val underlying: Connection) extends ZConnection.Restorable(underlying: Connection) {
    val discarded = new java.util.concurrent.atomic.AtomicBoolean(false)
  }

  private def discardState(connection: Connection): java.util.concurrent.atomic.AtomicBoolean = connection match {
    case pg: PGRestorableConnection => pg.discarded
    case _                          => throw new IllegalStateException("Unexpected PostgreSQL connection wrapper")
  }

  def connectionPool(
      props: Map[String, String]
  ): ZLayer[PostgresConfig, Throwable, ZConnectionPool] = ZLayer.scoped(for
    config <- ZIO.service[PostgresConfig]
    _      <- ZIO.attempt(Class.forName("org.postgresql.Driver"))
    acquire = ZIO.attemptBlocking {
      val properties = new java.util.Properties
      props.foreachEntry((k, v) => properties.put(k, v))
      java.sql.DriverManager
        .getConnection(s"jdbc:postgresql://${config.host}:${config.port}/${config.database}", properties)
    }
    getConn = ZIO.acquireRelease(acquire)(con => ZIO.attemptBlocking(con.close()).ignoreLogged).flatMap { con =>
      ZIO.attemptBlocking {
        con.setAutoCommit(false)
        val connection = ZConnection(PGRestorableConnection(con))
        con.commit()
        connection
      }
    }
    pool <- ZPool.make(getConn, Range(config.maxConnections, config.maxConnections), zio.Duration.Infinity)
    discard = (connection: ZConnection) =>
      connection.access(c => discardState(c).set(true)).orDie *> pool.invalidate(connection)
    tx = ZLayer.scoped {
      traces.span("acquire connection") {
        for
          start      <- zio.Clock.nanoTime
          connection <- pool.get
          _ <- ZIO.addFinalizerExit { exit =>
            connection.access(c => discardState(c).get()).orDie.flatMap { discarded =>
              if discarded then zio.Clock.nanoTime.flatMap(end => trackFailure.update(end - start))
              else
                exit match {
                  case Exit.Success(_) =>
                    for
                      _ <- connection
                        .access(c => if !c.getAutoCommit then c.commit())
                        .onError(_ => discard(connection))
                        .orDie
                      _   <- connection.restore
                      end <- zio.Clock.nanoTime
                      _   <- trackSuccess.update(end - start)
                    yield ()
                  case Exit.Failure(_) =>
                    for
                      _ <- connection
                        .access(c => if !c.getAutoCommit then c.rollback())
                        .foldZIO(
                          _ => discard(connection),
                          _ => connection.restore
                        )
                      end <- zio.Clock.nanoTime
                      _   <- trackFailure.update(end - start)
                    yield ()
                }
            }
          }
        yield connection
      }
    }
    probe = tx(
      ZIO
        .serviceWithZIO[ZConnection](_.isValid() @@ connectionIsValidLatency)
        .filterOrFail(identity)(new java.sql.SQLException("Connection not ready"))
    )
    _ <- probe
    _ <- ZIO.acquireRelease(jdbcUpGauge.set(1))(_ => jdbcUpGauge.set(0))

    // periodic database connectivity probe
    _ <- ZIO.when(!config.probeInterval.isZero) {
      // After a probe failure, drain remaining stale connections from the pool.
      // We iterate maxConnections times, acquiring and testing one connection per iteration.
      // The pool uses a FIFO queue, so under no contention this visits each connection exactly
      // once. Under contention (other fibers also borrowing connections), we may probe some
      // connections more than once. Borrowers also invalidate connections when rollback fails.
      // Acquisition failures are handled gracefully (database might be down).
      // Individual connection testing failures don't stop the overall drain operation.
      // This piece is crucial to prevent the pool from repeatedly handing out stale
      // connections after a database outage while there is no ledger data in flight.
      val drainStaleConnections =
        ZIO.foreachDiscard(0 until config.maxConnections) { _ =>
          ZIO.scoped {
            pool.get
              .flatMap { conn =>
                sql"""select 1"""
                  .query[Int]
                  .selectOne
                  .provide(ZLayer.succeed(conn))
                  .flatMap(_ => conn.rollback)
                  .foldCauseZIO(
                    cause =>
                      ZIO.logWarning(s"Connection test query failed: ${cause.squash.getMessage}") *>
                        discard(conn),
                    _ => ZIO.logDebug("Connection test query succeeded")
                  )
              }
              // If can't acquire, skip
              .catchAll(e => ZIO.logDebug(s"Could not acquire connection from pool: ${e.getMessage}"))
          }
        }

      probe
        .foldCauseZIO(
          cause =>
            jdbcUpGauge.set(0) *>
              ZIO.logWarning(
                s"Database probe failed: ${cause.squash.getMessage}; draining stale connections"
              ) *>
              drainStaleConnections,
          _ =>
            jdbcUpGauge.set(1) *>
              ZIO.logInfo("Database probe successful")
        )
        .repeat(Schedule.spaced(config.probeInterval))
        .forkScoped
    }
  yield new ZConnectionPool {
    def transaction: ZLayer[Any, Throwable, ZConnection] = tx
    def invalidate(conn: ZConnection): UIO[Any]          = discard(conn)
  })
}
