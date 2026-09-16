// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational

import com.digitalasset.pqs.postgres.relational.projection.ProjectionRegistry
import zio.jdbc.*
import zio.{Scope, ZEnvironment, ZIO}

object WriterFence:
  private val key           = ProjectionRegistry.writerLockKey
  val activityLockKey: Long = 0x70716a5f61637476L

  final case class Identity(pid: Int, backendStart: String)

  val identity: ZIO[ZConnection, Throwable, Identity] =
    sql"select pid, backend_start::text from pg_stat_activity where pid = pg_backend_pid()"
      .query[(Int, String)]
      .selectOne
      .someOrFail(new IllegalStateException("missing writer backend identity"))
      .map((pid, started) => Identity(pid, started))

  def check(fence: Identity): ZIO[ZConnection, Throwable, Unit] =
    for
      // Hold through commit so takeover cannot overtake a transaction that passed the liveness check.
      _ <- sql"select 1 from pg_advisory_xact_lock_shared($activityLockKey)".query[Int].selectOne
      live <- sql"""select exists (
                      select 1 from pg_locks l join pg_stat_activity a on a.pid = l.pid
                      where locktype = 'advisory' and granted
                        and l.pid = ${fence.pid} and a.backend_start = ${fence.backendStart}::timestamptz
                        and classid = ${key >>> 32}::oid
                        and objid = ${key & 0xffffffffL}::oid and objsubid = 1
                        and mode = 'ExclusiveLock')""".query[Boolean].selectOne
      _ <- ZIO
        .fail(new java.io.IOException("relational writer liveness connection lost; restart ingestion"))
        .unless(live.contains(true))
      _ <- sql"call __rel_ensure_writer_valid()".execute
    yield ()

  val drain: ZIO[ZConnection, Throwable, Unit] =
    (sql"set local lock_timeout = '30s'".execute *>
      sql"select 1 from pg_advisory_xact_lock($activityLockKey)".query[Int].selectOne).unit

  def acquire(pool: ZConnectionPool, maxConnections: Int): ZIO[Scope, Throwable, ZEnvironment[ZConnection]] =
    for
      _ <- ZIO
        .fail(
          new RuntimeException(
            "relational ingest needs at least two datastore connections so the writer-liveness lock does not starve the pool"
          )
        )
        .when(maxConnections < 2)
      connEnv <- pool.transaction.build
      _       <- sql"set local lock_timeout = '30s'".execute.provideEnvironment(connEnv)
      _ <- ZIO.acquireRelease(
        sql"select 1 from pg_advisory_lock($key)".query[Int].selectOne.provideEnvironment(connEnv)
      )(_ => release(pool, connEnv))
      _ <- drain.provideEnvironment(connEnv)
      _ <- connEnv.get[ZConnection].access(_.commit())
    yield connEnv

  private def release(pool: ZConnectionPool, env: ZEnvironment[ZConnection]): ZIO[Any, Nothing, Unit] =
    val connection = env.get[ZConnection]
    (connection.rollback *>
      sql"select pg_advisory_unlock($key)".query[Boolean].selectOne.provideEnvironment(env) *>
      connection.access(_.commit())).catchAll(_ => pool.invalidate(connection)).unit

  def requireIdle[R, A](pool: ZConnectionPool, maxConnections: Int)(body: ZIO[R, Throwable, A]): ZIO[R, Throwable, A] =
    ZIO.scoped {
      for
        _ <- ZIO
          .fail(
            new RuntimeException(
              "relational schema apply needs at least two datastore connections so the writer-liveness lock does not starve the migration"
            )
          )
          .when(maxConnections < 2)
        connEnv <- pool.transaction.build
        acquired <- ZIO.acquireRelease(
          sql"select case when pg_try_advisory_lock($key) then 1 else 0 end"
            .query[Int]
            .selectOne
            .map(_.getOrElse(0))
            .provideEnvironment(connEnv)
        )(_ => release(pool, connEnv))
        _ <- acquired.compareTo(1) match
          case 0 => drain.provideEnvironment(connEnv) *> connEnv.get[ZConnection].access(_.commit())
          case _ =>
            ZIO.fail(
              new RuntimeException(
                "cannot apply schema: a relational ingest writer is live; stop the writer, then re-run schema apply"
              )
            )
        result <- body
      yield result
    }
end WriterFence
