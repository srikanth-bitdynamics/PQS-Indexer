// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline.pipeline

import com.digitalasset.canonical.UserRight
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.backend.Datastore
import zio.test.*

object PipelineCoverageSpec extends ZIOSpecDefault:

  private def record(
      datasource: Config.TransactionApi,
      actualStart: Offset,
      dbEnd: Offset
  ): Datastore.CoverageRecord =
    Pipeline.coverageRecord(
      requestedStart = "Latest",
      datasource = datasource,
      contractFilter = "*",
      metadataFilter = "-",
      rights = UserRight.AsAnyParty,
      normalizedStart = Offset.Genesis,
      actualStart = actualStart,
      ledgerStart = Offset.Absolute(2),
      ledgerEnd = Offset.Absolute(17),
      dbEnd = dbEnd
    )

  def spec = suite("Pipeline.coverageRecord")(
    test("maps the transaction-stream datasource"):
      assertTrue(
        record(Config.TransactionApi.TransactionStream, Offset.Genesis, Offset.Genesis).datasource ==
          Datastore.Datasource.TransactionStream
      )
    ,
    test("maps the transaction-tree datasource"):
      assertTrue(
        record(Config.TransactionApi.TransactionTreeStream, Offset.Genesis, Offset.Genesis).datasource ==
          Datastore.Datasource.TransactionTreeStream
      )
    ,
    test("derives the ACS seed offset when the store is empty and seeded at an absolute offset"):
      assertTrue(
        record(Config.TransactionApi.TransactionStream, Offset.Absolute(5), Offset.Genesis).acsSeedOffset ==
          Some(Offset.Absolute(5))
      )
    ,
    test("has no ACS seed offset when starting from Genesis on an empty store"):
      assertTrue(
        record(Config.TransactionApi.TransactionStream, Offset.Genesis, Offset.Genesis).acsSeedOffset == None
      )
    ,
    test("has no ACS seed offset when the store is not empty"):
      assertTrue(
        record(Config.TransactionApi.TransactionStream, Offset.Absolute(5), Offset.Absolute(3)).acsSeedOffset == None
      )
  )
