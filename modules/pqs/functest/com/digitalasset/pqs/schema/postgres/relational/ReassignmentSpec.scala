// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.relational

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.daml.DamlSdk.onlyCantonVersion
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*
import zio.test.*

import scala.language.implicitConversions

object ReassignmentSpec extends FuncTest[Service[Ledger] & Postgres & DeployedDar]:
  private val asset = DamlSource(
    "Asset" -> """module Asset where
                 |template Asset
                 |  with owner : Party
                 |  where signatory owner
                 |""".stripMargin
  )
  private val source = Synchronizer("source")
  private val target = Synchronizer("target")

  val shared =
    DamlSdk.dar(asset) ++ DamlSdk.multiSyncLedger(source, target) ++ Postgres.instance
      >+> DamlSdk.uploadAndVetDar(source, target)

  def spec = suite("relational reassignment conformance")(
    lifecycle(false, "TransactionStream"),
    lifecycle(false, "TransactionTreeStream"),
    lifecycle(true, "TransactionStream"),
    lifecycle(true, "TransactionTreeStream"),
    lifecycle(false, "TransactionStream", assignmentFirst = true),
    lifecycle(false, "TransactionTreeStream", assignmentFirst = true)
  ) @@ onlyCantonVersion(">=3.5")

  private def ingest(alice: Party, start: String, datasource: String) =
    Pqs.runRelationalPipeline(
      s"--pipeline-ledger-start=$start",
      "--pipeline-ledger-stop=Latest",
      s"--pipeline-datasource=$datasource",
      s"--pipeline-filter-parties=${alice.id}"
    )

  private def lifecycle(acs: Boolean, datasource: String, assignmentFirst: Boolean = false) =
    funcTest(
      s"$datasource preserves ${if acs then "ACS-seeded" else if assignmentFirst then "assignment-first" else "replayed"} state across reassignment and archive"
    ) {
      val alice            = Party("Alice")
      val contractId       = Capture[String]
      val unassignedOffset = Capture[Long]
      Given:
        Postgres.database
      And:
        DamlSdk.allocateParties(alice -> Seq(source, target))
      And:
        val args = Record.defaultInstance
          .addFields(RecordField("owner", Some(Value(Value.Sum.Party(alice.id)))))
        Ledger
          .create("Asset:Asset", args, alice, source)
          .map(_.getTransaction.events.headOption.map(_.getCreated.contractId))
          .someOrFail(new IllegalStateException("create returned no event"))
          .is(contractId.capture)
      And:
        Ledger.reassign(contractId.get, alice, source, target).is(unassignedOffset.capture)
      And:
        ingest(
          alice,
          if acs then "Latest" else if assignmentFirst then unassignedOffset.get.toString else "Genesis",
          datasource
        )
      Then:
        Postgres.query {
          for
            _ <- sql"set search_path to pqs_relational".execute
            lifecycle <- sql"""select source_kind::text, created_at_offset is null, archived_tx_ix is null
                                from pqs_relational.__rel_contracts where contract_id = ${contractId.get}"""
              .query[(String, Boolean, Boolean)]
              .selectAll
            active <- sql"select count(*) from pqs_relational.active_contracts".query[Long].selectOne
            events <- sql"""select count(*), count(*) filter (where archive_source is not null)
                             from pqs_relational.__query_events where contract_id = ${contractId.get}"""
              .query[(Long, Long)]
              .selectOne
            coverage <- sql"""select reassignment_history_complete, assignment_origin_state_complete,
                                     acs_seed_offset is not null from pqs_relational.__query_coverage"""
              .query[(Boolean, Boolean, Boolean)]
              .selectAll
          yield assertTrue(
            lifecycle == Seq(
              (
                if acs then "acs_seed" else if assignmentFirst then "assignment" else "stream",
                acs || assignmentFirst,
                true
              )
            ),
            active.contains(1L),
            events.contains((if acs || assignmentFirst then 1L else 3L, 0L)),
            coverage == Seq((true, true, acs || assignmentFirst))
          )
        }
      When:
        Ledger.archive("Asset:Asset", contractId.get, alice, target)
      And:
        ingest(alice, "Oldest", datasource)
      Then:
        Postgres.query {
          for
            _ <- sql"set search_path to pqs_relational".execute
            lifecycle <- sql"""select count(*), count(*) filter (where archived_tx_ix is not null
                                  and archived_at_offset > coalesce(created_at_offset, 0))
                                from pqs_relational.__rel_contracts where contract_id = ${contractId.get}"""
              .query[(Long, Long)]
              .selectOne
            active <- sql"select count(*) from pqs_relational.active_contracts".query[Long].selectOne
            archives <- sql"""select count(*) from pqs_relational.__query_events
                               where contract_id = ${contractId.get} and archive_source is not null"""
              .query[Long]
              .selectOne
          yield assertTrue(lifecycle.contains((1L, 1L)), active.contains(0L), archives.contains(1L))
        }
    }
