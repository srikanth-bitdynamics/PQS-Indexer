// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.transcode.schema.Schema
import com.digitalasset.pqs.utils.safeequals.===
import ujson.Value
import zio.ZIO
import zio.jdbc.*

object ProjectionBinding:
  def activeShapes: ZIO[ZConnection, Throwable, Map[String, Shape.ResolvedShape]] =
    sql"""select resolved_shape::text from __query_projection
          where status = 'active' order by projection_version desc limit 1"""
      .query[String]
      .selectOne
      .map(_.fold(Map.empty)(parse))

  def rebind(saved: Map[String, Shape.ResolvedShape], schema: Schema): Map[String, Shape.ResolvedShape] =
    val current = Shape.resolveAll(schema)
    saved.map { (qualified, shape) =>
      val available = current.getOrElse(
        shape.lineage,
        throw new IllegalArgumentException(s"projection $qualified is absent from the current package dictionary")
      )
      val byPosition = available.promoted.map(field => field.position -> field).toMap
      val fields = shape.promoted.map { field =>
        byPosition
          .get(field.position)
          .filter { candidate =>
            (candidate.name === field.name) &&
            (candidate.pgType === field.pgType) && candidate.nullable == field.nullable &&
            field.damlType.forall(t => candidate.damlType.contains(t)) &&
            ((field.enumCases, candidate.enumCases) match
              case (None, None)           => true
              case (Some(old), Some(now)) => now.startsWith(old)
              case _                      => false
            )
          }
          .getOrElse(
            throw new IllegalArgumentException(
              s"projection $qualified field '${field.name}' is incompatible with the current package dictionary; " +
                "apply and backfill a compatible projection before restarting ingestion"
            )
          )
      }
      qualified -> available.copy(promoted = fields)
    }

  def parse(json: String): Map[String, Shape.ResolvedShape] =
    ujson
      .read(json)
      .obj
      .values
      .flatMap(_.obj)
      .map((qualified, cols) => qualified -> cols.arr.map(fieldOf).toSeq)
      .groupMapReduce(_._1)(_._2)(_ ++ _)
      .map((qualified, fields) => qualified -> toShape(qualified, reconcile(qualified, fields)))

  private def reconcile(qualified: String, fields: Seq[Shape.PromotedField]): Seq[Shape.PromotedField] =
    fields.groupBy(_.position).toSeq.sortBy(_._1).map { (position, group) =>
      group.distinctBy(f => (f.name, f.pgType.sql, f.nullable, f.enumCases, f.damlType)) match
        case Seq(field) => field
        case _ =>
          throw new RuntimeException(
            s"projection binding for $qualified has conflicting definitions at position $position"
          )
    }

  private def fieldOf(v: Value): Shape.PromotedField =
    Shape.PromotedField(
      v("name").str,
      pgType(v("type").str),
      v("nullable").bool,
      v("position").num.toInt,
      v.obj.get("enum").map(_.arr.map(_.str).toSeq),
      v.obj.get("daml_type").map(_.str)
    )

  private def toShape(qualified: String, fields: Seq[Shape.PromotedField]): Shape.ResolvedShape =
    val parts           = qualified.split(":")
    val lineage         = Shape.Lineage(parts(0), parts(1), parts(2), Shape.EntityKind.Template)
    val unionFieldCount = fields.map(_.position).maxOption.map(_ + 1).getOrElse(0)
    Shape.ResolvedShape(lineage, unionFieldCount, fields, Seq.empty, Seq.empty)

  private def pgType(sql: String): Shape.PgType =
    sql match
      case "bigint"      => Shape.PgType.Bigint
      case "boolean"     => Shape.PgType.Bool
      case "text"        => Shape.PgType.Text
      case "date"        => Shape.PgType.Date
      case "timestamptz" => Shape.PgType.Timestamptz
      case other         => Shape.PgType.Numeric(numericScale(other))

  private def numericScale(sql: String): Int =
    sql.substring(sql.indexOf(',') + 1, sql.indexOf(')')).trim.toInt
end ProjectionBinding
