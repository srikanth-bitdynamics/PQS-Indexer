// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.pqs.utils.safeequals.===

object IndexPlanner:
  final case class OrderKey(column: String, ascending: Boolean)

  final case class IndexSpec(equality: Seq[String], ordered: Seq[OrderKey]):
    def columns: Seq[String] = equality ++ ordered.map(_.column)

  final case class Plan(indexes: Seq[IndexSpec], diagnostics: Seq[String])

  def parseOrder(token: String): OrderKey =
    val trimmed = token.trim
    val parts   = trimmed.split("\\s+").toVector
    parts.lastOption match
      case Some(direction) if isDirection(direction) =>
        OrderKey(parts.dropRight(1).mkString(" ").trim, !(direction.toLowerCase === "desc"))
      case _ =>
        OrderKey(trimmed, true)

  def plan(queries: Seq[ProjectionQuery], available: Set[String]): Plan =
    val perQuery = queries.map { query =>
      val equality = query.filter.distinct.sorted
      val ordered  = query.order.map(parseOrder).filterNot(k => equality.contains(k.column)).distinctBy(_.column)
      val cols     = equality ++ ordered.map(_.column)
      val missing  = cols.filterNot(available.contains).distinct
      if missing.nonEmpty then
        (
          Option.empty[IndexSpec],
          Seq(s"query [${cols.mkString(", ")}] references unknown column(s): ${missing.mkString(", ")}; skipped")
        )
      else if equality.isEmpty && ordered.isEmpty then (Option.empty[IndexSpec], Seq.empty[String])
      else (Some(IndexSpec(equality, ordered)), Seq.empty[String])
    }
    val candidates  = perQuery.flatMap(_._1)
    val diagnostics = perQuery.flatMap(_._2)
    val deduped     = candidates.distinct
    val reduced     = deduped.filterNot(candidate => dominated(candidate, deduped))
    val indexes     = reduced.sortBy(_.columns.mkString(","))
    Plan(indexes, diagnostics.sorted)

  private def dominated(candidate: IndexSpec, all: Seq[IndexSpec]): Boolean =
    all.exists { other =>
      candidate.ordered.length < other.ordered.length &&
      (candidate.equality === other.equality) &&
      other.ordered.startsWith(candidate.ordered)
    }

  private def isDirection(word: String): Boolean =
    val lower = word.toLowerCase
    (lower === "asc") || (lower === "desc")
end IndexPlanner
