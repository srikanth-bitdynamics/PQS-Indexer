// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational.projection

import com.digitalasset.transcode.schema.*
import zio.test.*

object ProjectionBindingSpec extends ZIOSpecDefault:
  private def schema(cases: Seq[String], version: String = "1.0.0"): Schema =
    val id = Identifier(
      PackageId(version),
      PackageName("Finance"),
      PackageVersion(version),
      ModuleName("Main"),
      EntityName("Asset")
    )
    Dictionary.make(
      Template(
        id,
        Descriptor.constructor(
          id,
          Descriptor.record(
            Seq(
              "status" -> Descriptor.constructor(id, Descriptor.enumeration(cases))
            )
          )
        ),
        None,
        false,
        Seq.empty,
        Seq.empty
      )
    )

  private def saved = ProjectionBinding.parse(
    ProjectionApply
      .plan(
        Map("asset" -> ProjectionDefinition(Seq("Finance:Main:Asset"), Seq("status"))),
        schema(Seq("A", "B"))
      )
      .resolvedShape
      .render()
  )

  private val json =
    """{"asset":{"Finance:Main:Asset":[
         {"name":"owner","type":"text","nullable":false,"position":0},
         {"name":"amount","type":"numeric(38, 10)","nullable":false,"position":1},
         {"name":"active","type":"boolean","nullable":false,"position":2},
         {"name":"status","type":"text","nullable":false,"position":3,"enum":["A","B"]},
         {"name":"when","type":"timestamptz","nullable":true,"position":4},
         {"name":"born","type":"date","nullable":true,"position":6}
       ]}}"""

  def spec = suite("projection binding")(
    test("rebinds an activated enum to appended cases without losing existing values"):
      val bound = ProjectionBinding.rebind(saved, schema(Seq("A", "B", "C"), "2.0.0"))("Finance:Main:Asset")
      assertTrue(
        bound.promoted.head.enumCases.contains(Seq("A", "B", "C")),
        TypedRowCodec.extract(bound, DynamicValue.Record(DynamicValue.Enumeration(2))) == Seq(
          TypedRowCodec.SqlValue.Text("C")
        ),
        TypedRowCodec.extract(bound, DynamicValue.Record(DynamicValue.Enumeration(1))) == Seq(
          TypedRowCodec.SqlValue.Text("B")
        )
      )
    ,
    test("rejects reordered, removed, and renamed enum constructors before decoding"):
      val results = Seq(Seq("B", "A"), Seq("A"), Seq("A", "Renamed")).map(cases =>
        scala.util.Try(ProjectionBinding.rebind(saved, schema(cases, "2.0.0")))
      )
      assertTrue(results.forall(_.failed.toOption.exists(_.getMessage.contains("incompatible"))))
    ,
    test("reconstructs the resolved shape from stored JSON with pg types and union field count"):
      val shape = ProjectionBinding.parse(json)("Finance:Main:Asset")
      assertTrue(
        shape.lineage == Shape.Lineage("Finance", "Main", "Asset", Shape.EntityKind.Template),
        shape.unionFieldCount == 7,
        shape.promoted.map(f => (f.name, f.pgType.sql, f.nullable, f.position)) == Seq(
          ("owner", "text", false, 0),
          ("amount", "numeric(38, 10)", false, 1),
          ("active", "boolean", false, 2),
          ("status", "text", false, 3),
          ("when", "timestamptz", true, 4),
          ("born", "date", true, 6)
        ),
        shape.promoted.flatMap(_.enumCases) == Seq(Seq("A", "B"))
      )
    ,
    test("an empty document binds no shapes"):
      assertTrue(ProjectionBinding.parse("{}").isEmpty)
    ,
    test("unions the columns of two named groups that target the same template, deduped by position"):
      val twoGroups =
        """{"primary":{"Finance:Main:Asset":[
             {"name":"owner","type":"text","nullable":false,"position":0},
             {"name":"amount","type":"numeric(38, 10)","nullable":false,"position":1}
           ]},
           "secondary":{"Finance:Main:Asset":[
             {"name":"owner","type":"text","nullable":false,"position":0},
             {"name":"status","type":"text","nullable":false,"position":2,"enum":["A","B"]}
           ]}}"""
      val shape = ProjectionBinding.parse(twoGroups)("Finance:Main:Asset")
      assertTrue(
        shape.unionFieldCount == 3,
        shape.promoted.map(f => (f.name, f.pgType.sql, f.position)) == Seq(
          ("owner", "text", 0),
          ("amount", "numeric(38, 10)", 1),
          ("status", "text", 2)
        )
      )
    ,
    test("rejects two groups that define conflicting columns at the same position"):
      val conflicting =
        """{"a":{"Finance:Main:Asset":[{"name":"owner","type":"text","nullable":false,"position":0}]},
           "b":{"Finance:Main:Asset":[{"name":"amount","type":"bigint","nullable":false,"position":0}]}}"""
      assertTrue(scala.util.Try(ProjectionBinding.parse(conflicting)).isFailure)
  )
end ProjectionBindingSpec
