// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.transcode.schema.*
import com.digitalasset.pqs.utils.safeequals.===
import ujson.Value
import zio.ZIO
import zio.jdbc.*

object ProjectionBackfill:
  private val ChunkSize = 5000

  def run(
      schema: Schema,
      shapes: Map[String, Shape.ResolvedShape],
      version: Long,
      chunkSize: Int = ChunkSize
  ): ZIO[ZConnection, Throwable, Long] =
    withProjectionLock(runLocked(schema, shapes, version, chunkSize))

  private def runLocked(
      schema: Schema,
      shapes: Map[String, Shape.ResolvedShape],
      version: Long,
      chunkSize: Int
  ): ZIO[ZConnection, Throwable, Long] =
    for
      _   <- ZIO.fail(new IllegalArgumentException("backfill chunk size must be positive")).when(chunkSize <= 0)
      row <- ProjectionRegistry.get(version)
      _ <- ZIO
        .fail(new IllegalArgumentException(s"cannot backfill projection $version: it is not a pending draft"))
        .unless(row.exists(_.status === "draft"))
      boundShapes <- ZIO.attempt(ProjectionBinding.rebind(shapes, schema))
      encoding    <- readEncoding
      codec = DescriptorSchemaProcessor
        .assertProcess(
          schema,
          JsonCodec(
            encodeNumericAsString = encoding._1,
            encodeInt64AsString = encoding._2,
            removeTrailingNonesInRecords = encoding._3
          )
        )
        .matchByPackageId
      through <- resolveThrough(version)
      _ <- ZIO.foreachDiscard(boundShapes.values.toSeq)(shape =>
        backfillLineage(codec, shape, through, version, chunkSize)
      )
    yield through

  def runAndPublish(
      schema: Schema,
      shapes: Map[String, Shape.ResolvedShape],
      version: Long,
      chunkSize: Int = ChunkSize
  ): ZIO[ZConnection, Throwable, Long] =
    withProjectionLock(
      runLocked(schema, shapes, version, chunkSize).flatMap(through =>
        ProjectionRegistry.setBackfilledThrough(version, through).as(through)
      )
    )

  private def withProjectionLock[A](body: ZIO[ZConnection, Throwable, A]): ZIO[ZConnection, Throwable, A] =
    ZIO.serviceWithZIO[ZConnection] { connection =>
      // A session lock survives chunk commits and excludes projection changes until backfill finishes.
      ZIO.acquireReleaseWith(
        sql"select pg_try_advisory_lock(${ProjectionRegistry.projectionLockKey})"
          .query[Boolean]
          .selectOne
          .filterOrFail(_.contains(true))(
            new IllegalStateException("another projection operation is running; retry backfill")
          )
      ) { _ =>
        (connection.access(c => if !c.getAutoCommit then c.rollback()) *>
          sql"select pg_advisory_unlock(${ProjectionRegistry.projectionLockKey})".query[Boolean].selectOne *>
          commit).onError(_ => connection.access(_.close()).ignoreLogged).orDie.unit
      }(_ => body <* commit)
    }

  private def readEncoding: ZIO[ZConnection, Throwable, (Boolean, Boolean, Boolean)] =
    sql"select numeric_as_string, int64_as_string, exclude_nulls from __rel_encoding limit 1"
      .query[(Boolean, Boolean, Boolean)]
      .selectOne
      .map(_.getOrElse((true, true, false)))

  private def resolveThrough(version: Long): ZIO[ZConnection, Throwable, Long] =
    for
      watermark <- sql"select tx_ix from latest_checkpoint()".query[Long].selectOne.map(_.getOrElse(0L))
      pinned <- sql"select max(through_ix) from __rel_backfill_progress where projection_version = $version"
        .query[Option[Long]]
        .selectOne
        .map(_.flatten.getOrElse(0L))
    yield math.max(watermark, pinned)

  private def backfillLineage(
      codec: Dictionary[Codec[Value]],
      shape: Shape.ResolvedShape,
      through: Long,
      version: Long,
      chunkSize: Int
  ): ZIO[ZConnection, Throwable, Unit] =
    if shape.promoted.isEmpty then ZIO.unit
    else
      val qualified = shape.lineage.qualified
      resolveBaseTable(shape.lineage).flatMap { (entityPk, tbl) =>
        ensurePackagesPresent(entityPk, through) *>
          initProgress(version, qualified, through) *>
          progressCursor(version, qualified).flatMap {
            case None => ZIO.unit
            case Some((cursorTx, cursorPk)) =>
              chunkLoop(codec, shape, entityPk, tbl, through, version, qualified, cursorTx, cursorPk, chunkSize)
          }
      }

  private def ensurePackagesPresent(entityPk: Long, through: Long): ZIO[ZConnection, Throwable, Unit] =
    sql"""select count(*) from __rel_contracts c
          where c.template_entity_pk = $entityPk and c.created_tx_ix <= $through and c.redaction_id is null
            and not exists (select 1 from __rel_package pkg where pkg.id = c.representative_package_id)"""
      .query[Long]
      .selectOne
      .map(_.getOrElse(0L))
      .flatMap(missing =>
        ZIO
          .when(missing > 0L)(
            ZIO.fail(
              new RuntimeException(
                s"cannot backfill: $missing contract(s) reference a package id absent from __rel_package"
              )
            )
          )
          .unit
      )

  private def resolveBaseTable(l: Shape.Lineage): ZIO[ZConnection, Throwable, (Long, String)] =
    sql"""select pk, base_table from __rel_entity
          where package_name = ${l.packageName} and module_name = ${l.moduleName}
            and entity_name = ${l.entityName} and kind = 'template'"""
      .query[(Long, String)]
      .selectOne
      .someOrFail(
        new RuntimeException(s"relational entity ${l.qualified} (template) is not initialized; cannot backfill")
      )

  private def initProgress(version: Long, qualified: String, through: Long): ZIO[ZConnection, Throwable, Unit] =
    sql"""insert into __rel_backfill_progress (projection_version, qualified, through_ix)
          values ($version, $qualified, $through)
          on conflict (projection_version, qualified) do update
            set through_ix = greatest(__rel_backfill_progress.through_ix, excluded.through_ix), completed = false""".update.unit

  private def progressCursor(version: Long, qualified: String): ZIO[ZConnection, Throwable, Option[(Long, Long)]] =
    sql"""select cursor_tx_ix, cursor_pk from __rel_backfill_progress
          where projection_version = $version and qualified = $qualified and not completed"""
      .query[(Long, Long)]
      .selectOne

  private def chunkLoop(
      codec: Dictionary[Codec[Value]],
      shape: Shape.ResolvedShape,
      entityPk: Long,
      tbl: String,
      through: Long,
      version: Long,
      qualified: String,
      cursorTx: Long,
      cursorPk: Long,
      chunkSize: Int
  ): ZIO[ZConnection, Throwable, Unit] =
    selectChunk(entityPk, tbl, through, cursorTx, cursorPk, chunkSize).flatMap { rows =>
      rows.lastOption match
        case None => complete(version, qualified)
        case Some(last) =>
          ZIO.foreachDiscard(rows)(row => backfillRow(codec, shape, tbl, row)) *>
            advanceCursor(version, qualified, last._1, last._2) *>
            commit *>
            chunkLoop(codec, shape, entityPk, tbl, through, version, qualified, last._1, last._2, chunkSize)
    }

  private def selectChunk(
      entityPk: Long,
      tbl: String,
      through: Long,
      cursorTx: Long,
      cursorPk: Long,
      chunkSize: Int
  ): ZIO[ZConnection, Throwable, Seq[(Long, Long, String, String, String, String)]] =
    sql"""select c.created_tx_ix, p.contract_pk, pkg.id, pkg.name, pkg.version, p.payload_json::text
          from ${SqlFragment(tbl)} p
          join __rel_contracts c on c.contract_pk = p.contract_pk
          join __rel_package pkg on pkg.id = c.representative_package_id
          where c.redaction_id is null and c.template_entity_pk = $entityPk and c.created_tx_ix <= $through
            and (c.created_tx_ix, c.contract_pk) > ($cursorTx, $cursorPk)
          order by c.created_tx_ix, c.contract_pk limit $chunkSize"""
      .query[(Long, Long, String, String, String, String)]
      .selectAll

  private def advanceCursor(
      version: Long,
      qualified: String,
      cursorTx: Long,
      cursorPk: Long
  ): ZIO[ZConnection, Throwable, Unit] =
    sql"""update __rel_backfill_progress set cursor_tx_ix = $cursorTx, cursor_pk = $cursorPk
          where projection_version = $version and qualified = $qualified""".update.unit

  private def complete(version: Long, qualified: String): ZIO[ZConnection, Throwable, Unit] =
    (sql"""update __rel_backfill_progress set completed = true
           where projection_version = $version and qualified = $qualified""".update *> commit).unit

  private val commit: ZIO[ZConnection, Throwable, Unit] =
    ZIO.serviceWithZIO[ZConnection](_.access(c => if !c.getAutoCommit then c.commit()))

  private def backfillRow(
      codec: Dictionary[Codec[Value]],
      shape: Shape.ResolvedShape,
      tbl: String,
      row: (Long, Long, String, String, String, String)
  ): ZIO[ZConnection, Throwable, Unit] =
    val (_, contractPk, packageId, packageName, packageVersion, json) = row
    val id = Identifier(
      PackageId(packageId),
      PackageName(packageName),
      PackageVersion(packageVersion),
      ModuleName(shape.lineage.moduleName),
      EntityName(shape.lineage.entityName)
    )
    ZIO
      .attempt {
        val dv     = codec.template(id).toDynamicValue(ujson.read(json))
        val values = shape.promoted.map(_.name).zip(TypedRowCodec.extract(shape, dv))
        val assigns =
          values.map((name, value) => SqlFragment(quoteIdent(name)) ++ sql" = " ++ bind(value)).mkFragment(sql", ")
        sql"""update ${SqlFragment(tbl)} p set $assigns
              from __rel_contracts c where p.contract_pk = $contractPk
                and c.contract_pk = p.contract_pk and c.redaction_id is null"""
      }
      .flatMap(_.update.unit)

  private def bind(value: TypedRowCodec.SqlValue): SqlFragment =
    value match
      case TypedRowCodec.SqlValue.Null           => sql"null"
      case TypedRowCodec.SqlValue.Bigint(v)      => sql"$v"
      case TypedRowCodec.SqlValue.Numeric(v)     => sql"${v}::numeric"
      case TypedRowCodec.SqlValue.Text(v)        => sql"$v"
      case TypedRowCodec.SqlValue.Bool(v)        => sql"$v"
      case TypedRowCodec.SqlValue.Date(v)        => sql"${v.toString}::date"
      case TypedRowCodec.SqlValue.Timestamptz(v) => sql"${v.toString}::timestamptz"

  private def quoteIdent(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""
end ProjectionBackfill
