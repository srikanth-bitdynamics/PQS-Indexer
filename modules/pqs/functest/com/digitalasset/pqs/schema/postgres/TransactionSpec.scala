// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.postgres.backend
import com.digitalasset.pqs.services.postgres.{Database, Postgres, ProductionPool}
import com.digitalasset.pqs.utils.safeequals.===
import zio.*
import zio.jdbc.*
import zio.stream.ZStream
import zio.test.*

import java.sql.SQLException

object TransactionSpec extends FuncTest[Postgres]:
  def shared = Postgres.instance

  private val setup = backend.transact(
    sql"create table commit_test (id int unique deferrable initially deferred)".execute
  )
  private val duplicates = sql"insert into commit_test values (1), (1)".update
  private val count      = backend.transact(sql"select count(*) from commit_test".query[Long].selectOne)
  private def isDeferredViolation(result: Either[Throwable, ?]): Boolean = result match
    case Left(e: SQLException) => e.getSQLState === "23505"
    case _                     => false

  def spec = suite("production JDBC transaction completion")(
    funcTest("commit-time constraint errors reach the caller and leave the pool usable") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _      <- setup
          result <- backend.transact(duplicates).either
          before <- count
          _      <- backend.transact(sql"insert into commit_test values (2)".update)
          after  <- count
        yield assertTrue(isDeferredViolation(result), before.contains(0L), after.contains(1L))
    },
    funcTest("a failed batch commit emits no downstream watermark") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _       <- setup
          emitted <- Ref.make(0)
          result <- ZStream
            .succeed(duplicates.as(99L))
            .via(backend.executeParUnordered(1))
            .tap(_ => emitted.update(_ + 1))
            .runDrain
            .either
          n    <- emitted.get
          rows <- count
        yield assertTrue(isDeferredViolation(result), n == 0, rows.contains(0L))
    },
    funcTest("a connection lost before commit fails and is replaced") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _     <- setup
          admin <- ZIO.service[Database]
          lost <- backend
            .transact(
              sql"insert into commit_test values (3)".update *>
                sql"select pg_backend_pid()"
                  .query[Int]
                  .selectOne
                  .someOrFail(new RuntimeException("Missing backend PID"))
                  .flatMap(pid =>
                    Postgres
                      .query(sql"select pg_terminate_backend($pid)".query[Boolean].selectOne)
                      .provideEnvironment(ZEnvironment(admin))
                  )
            )
            .either
          _    <- backend.transact(sql"insert into commit_test values (4)".update)
          rows <- count
        yield assertTrue(lost.isLeft, rows.contains(1L))
    },
    funcTest("the legacy scoped transaction cannot swallow a commit failure") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _      <- setup
          result <- transaction(duplicates).exit
          rows   <- count
        yield assertTrue(result.isFailure, rows.contains(0L))
    },
    funcTest("the single-transaction sink reports deferred commit failures") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _      <- setup
          result <- ZStream.succeed(duplicates).run(backend.executeInSingleTransaction).either
          rows   <- count
        yield assertTrue(isDeferredViolation(result), rows.contains(0L))
    }
  )
