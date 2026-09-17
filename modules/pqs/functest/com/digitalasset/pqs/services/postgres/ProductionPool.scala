// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.postgres

import com.digitalasset.pqs.configuration.Secret
import com.digitalasset.pqs.docker.Docker
import com.digitalasset.pqs.postgres.backend
import com.digitalasset.pqs.postgres.backend.{PostgresConfig, TlsConfig}
import zio.*
import zio.jdbc.ZConnectionPool
import zio.jdbc.SqlFragment

import scala.language.implicitConversions

object ProductionPool:
  def config(
      schema: String = "public",
      maxConnections: Int = 2
  ): ZLayer[Docker & Postgres & Database, Throwable, PostgresConfig] =
    ZLayer.scoped {
      for
        pg   <- ZIO.service[Postgres]
        db   <- ZIO.service[Database]
        ca   <- Docker.certificateAuthority
        cert <- ca.generate("pqs-test-client")
        root <- temporary(ca.certificate.crt)
        key  <- temporary(cert.certificate.der)
        crt  <- temporary(cert.certificate.crt)
      yield PostgresConfig(
        host = pg.service.exposedAddress,
        port = pg.service.exposedPorts(Postgres.port),
        database = db.name,
        schema = schema,
        username = "postgres",
        password = Secret("postgres"),
        maxConnections = maxConnections,
        tls = TlsConfig(TlsConfig.SslMode.Require, Some(root.toIO), Some(key.toIO), Some(crt.toIO)),
        probeInterval = 0.seconds
      )
    }

  def layer(
      schema: String = "public",
      maxConnections: Int = 2
  ): ZLayer[Docker & Postgres & Database, Throwable, ZConnectionPool] =
    (config(schema, maxConnections) ++ backend.instanceId) >>> backend.connectionPool

  val relationalSchema: ZIO[ZConnectionPool, Throwable, Unit] =
    backend.transact {
      ZIO.foreachDiscard(
        Seq(
          "V001__Create_relational_schema.sql",
          "R__rel_functions.sql",
          "R__rel_views_and_triggers.sql"
        )
      ) { name =>
        ZIO
          .attempt(scala.util.Using.resource(scala.io.Source.fromResource(s"db/relational/$name"))(_.mkString))
          .flatMap(SqlFragment(_).execute)
      }
    }

  private def temporary(contents: os.Source): ZIO[Scope, Throwable, os.Path] =
    ZIO.acquireRelease(ZIO.attemptBlocking(os.temp(contents)))(p => ZIO.attemptBlocking(os.remove(p)).ignoreLogged)
