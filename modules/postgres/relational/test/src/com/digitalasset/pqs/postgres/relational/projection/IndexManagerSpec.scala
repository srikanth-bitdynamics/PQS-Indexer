// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational.projection

import zio.test.*

object IndexManagerSpec extends ZIOSpecDefault:
  private val template = "finance:Main:Asset"
  private val table    = "rel_finance__main__asset"
  private val fields   = Seq("owner", "currency", "amount", "amount_desc")

  private def plan(definitions: Map[String, ProjectionDefinition]): IndexManager.Plan =
    IndexManager.plan(
      definitions,
      definitions.keys.map(name => name -> Map(template -> fields)).toMap,
      Map(template -> table)
    )

  def spec = suite("managed index planning")(
    test("an unconstrained leading equality column cannot cover a narrower filter") {
      val result = plan(
        Map(
          "assets" -> ProjectionDefinition(
            Seq(template),
            fields,
            Seq(
              ProjectionQuery(Seq("owner"), Seq("amount desc")),
              ProjectionQuery(Seq("currency", "owner"), Seq("amount desc"))
            )
          )
        )
      )
      assertTrue(
        result.indexes.map(_.columns).toSet == Set(Seq("owner", "amount"), Seq("currency", "owner", "amount")),
        result.indexes.forall(i => i.covered.forall(s => s.filter.toSet == i.keys.dropRight(1).map(_.column).toSet))
      )
    },
    test("a column ending in _desc cannot collide with descending order on another column") {
      val result = plan(
        Map(
          "assets" -> ProjectionDefinition(
            Seq(template),
            fields,
            Seq(ProjectionQuery(order = Seq("amount desc")), ProjectionQuery(order = Seq("amount_desc")))
          )
        )
      )
      assertTrue(result.indexes.size == 2, result.indexes.map(_.name).distinct.size == 2)
    },
    test("shared physical indexes are planned once across named projections") {
      val definition = ProjectionDefinition(Seq(template), fields, Seq(ProjectionQuery(Seq("owner"))))
      val result     = plan(Map("positions" -> definition, "settlement" -> definition))
      assertTrue(result.indexes.size == 1, result.indexes.flatMap(_.covered).size == 1)
    },
    test("ordering by an equality-constrained field keeps its query attributed") {
      val result = plan(
        Map("assets" -> ProjectionDefinition(Seq(template), fields, Seq(ProjectionQuery(Seq("owner"), Seq("owner")))))
      )
      assertTrue(
        result.indexes.map(_.columns) == Seq(Seq("owner")),
        result.indexes.flatMap(_.covered).exists(_.fullCoverage)
      )
    }
  )
end IndexManagerSpec
