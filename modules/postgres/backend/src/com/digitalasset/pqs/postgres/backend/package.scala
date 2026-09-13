// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres

import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.postgres.backend.TlsConfig.SslMode
import org.postgresql.PGProperty
import zio.jdbc.*
import zio.metrics.Metric
import zio.metrics.MetricKeyType.Histogram.Boundaries
import zio.stream.{ZChannel, ZPipeline, ZSink, ZStream}
import zio.{Chunk, Exit, Queue, ZIO, ZLayer}

import java.io.File
import java.util.concurrent.atomic.AtomicLong

package object backend:
  /** Execute each transaction in parallel */
  def executePar[A](
      n: Int
  ): ZPipeline[ZConnectionPool & PostgresConfig, Throwable, ZIO[ZConnection, Throwable, A], A] =
    ZPipeline.serviceWithPipeline[PostgresConfig](config =>
      ZPipeline[ZIO[ZConnection, Throwable, A]].mapZIOPar(n) { call =>
        transaction(call) @@ traces.span("execute datastore transaction")
      }
    )

  /** Execute each transaction in parallel in breaking the order downstream */
  def executeParUnordered[A](
      n: Int
  ): ZPipeline[ZConnectionPool & PostgresConfig, Throwable, ZIO[ZConnection, Throwable, A], A] =
    ZPipeline.serviceWithPipeline[PostgresConfig](config =>
      ZPipeline[ZIO[ZConnection, Throwable, A]].mapZIOParUnordered(n) { call =>
        transaction(call) @@ traces.span("execute datastore transaction")
      }
    )

  /** Execute all SQL statements in one large transaction */
  // NB: Don't try to parallelize this because postgres uses one thread per connection and since this is one large
  // transaction it uses only one connection and one thread on server side.
  val executeInSingleTransaction: ZSink[ZConnectionPool, Throwable, ZIO[ZConnection, Throwable, Any], Nothing, Unit] =
    ZSink.unwrapScoped(
      transaction.build.map(connection =>
        ZSink.foreach(identity[ZIO[ZConnection, Throwable, Any]]).provideEnvironment(connection)
      )
    )

  val instanceId: ZLayer[Any, Throwable, InstanceId] = ZLayer.fromZIO(
    ZIO.attempt(InstanceId(java.util.UUID.randomUUID.toString))
  )

  val connectionPool: ZLayer[PostgresConfig & InstanceId, Throwable, ZConnectionPool] = ZLayer.fromZIO {
    for
      conf       <- ZIO.service[PostgresConfig]
      instanceId <- ZIO.service[InstanceId]
    yield zio.jdbc.shims.postgres.connectionPool(
      Map(
        PGProperty.USER.getName             -> conf.username,
        PGProperty.PASSWORD.getName         -> conf.password.value,
        PGProperty.TCP_KEEP_ALIVE.getName   -> conf.keepAlive.toString,
        PGProperty.APPLICATION_NAME.getName -> conf.appName,
        PGProperty.CURRENT_SCHEMA.getName   -> conf.schema
      ) ++ sslprops(conf.tls) ++ instanceIdProp(instanceId) ++ conf.properties.view.mapValues(_.value)
    )
  }.flatten

  def instanceIdProp(instanceId: InstanceId): Map[String, String] = Map(
    PGProperty.OPTIONS.getName -> s"-c scribe.instance=${instanceId}"
  )

  def sslprops(conf: TlsConfig): Map[String, String] =
    sslmode(conf) ++ sslrootcert(conf) ++ sslcert(conf) ++ sslkey(conf)

  private def sslmode(conf: TlsConfig) =
    Map(
      PGProperty.SSL_MODE.getName -> (conf.mode match
        case SslMode.Disable    => "disable"
        case SslMode.Require    => "require"
        case SslMode.VerifyCA   => "verify-ca"
        case SslMode.VerifyFull => "verify-full"
      )
    )

  private def sslrootcert(conf: TlsConfig) = fromFile(conf.caCertificate, PGProperty.SSL_ROOT_CERT)
  private def sslcert(conf: TlsConfig)     = fromFile(conf.certificate, PGProperty.SSL_CERT)
  private def sslkey(conf: TlsConfig)      = fromFile(conf.privateKey, PGProperty.SSL_KEY)

  private def fromFile(file: Option[File], key: PGProperty) = file.map(f => key.getName -> f.getCanonicalPath)

  /** Bounded async buffer between upstream and downstream with back-pressure; depth is tracked as `<name>_size`. */
  def waitPoint[A](name: String, capacity: Int = 16, chunkSize: Int = 16): ZPipeline[Any, Nothing, A, A] =
    ZPipeline.fromFunction[Any, Nothing, A, A](inStream =>
      ZStream.unwrapScoped(
        ZIO
          .acquireRelease(
            Queue.bounded[Exit[Option[Nothing], A]](capacity) <&> zio.Ref.make(0)
          )(
            _._1.shutdown
          )
          .flatMap { (queue, qSize) =>
            val size = Metric.histogram(
              s"${name}_size",
              s"Number of in-flight units of work in $name wait point",
              Boundaries.linear(0, capacity.toDouble / 16, 16)
            )
            val counter           = Metric.counter(name, s"Number of units of work processed in $name wait point")
            def inc(as: Chunk[?]) = qSize.updateAndGet(_ + as.size).flatMap(size.update(_)) *> counter.modify(as.size)
            def dec(as: Chunk[?]) = qSize.updateAndGet(_ - as.size).flatMap(size.update(_))

            val outStream = ZStream.fromQueue(queue, chunkSize).flattenExitOption.mapChunksZIO(ch => dec(ch).as(ch))

            lazy val enqueue: ZChannel[Any, Nothing, Chunk[A], Any, Nothing, Nothing, Any] =
              ZChannel.readWithCause[Any, Nothing, Chunk[A], Any, Nothing, Nothing, Any](
                in => ZChannel.fromZIO(inc(in).as(in.map(Exit.succeed)).flatMap(queue.offerAll)) *> enqueue,
                err => ZChannel.fromZIO(queue.offer(Exit.failCause(err))) *> ZChannel.refailCause(err),
                done => ZChannel.fromZIO(queue.offer(Exit.fail(None))) *> ZChannel.succeedNow(done)
              )
            (inStream.channel >>> enqueue).runScoped.forkScoped.as(outStream)
          }
      )
    )

  final case class IdPlaceholder private (id: Long)
  object IdPlaceholder:
    trait Factory { def mk: IdPlaceholder }
    def factory(start: Long): Factory = new Factory:
      private val cnt       = AtomicLong(start)
      def mk: IdPlaceholder = IdPlaceholder(cnt.incrementAndGet())

end backend
