// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.SharedLedgerAndPostgresAndAuthTest
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Ledger, Party, User}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.durationInt
import zio.jdbc.*
import zio.test.*

import scala.language.implicitConversions

object RetryCoverageSpec extends SharedLedgerAndPostgresAndAuthTest:
  private val source = DamlSource("Note" -> """module Note where
                                              |import Daml.Script
                                              |template Note
                                              |  with owner : Party
                                              |  where signatory owner
                                              |setup : Party -> Script ()
                                              |setup owner = do
                                              |  _ <- submit owner $ createCmd Note with owner
                                              |  pure ()
                                              |""".stripMargin)

  def spec = suite("coverage across automatic retries")(
    changedRights(false),
    changedRights(true)
  )

  private def changedRights(revoking: Boolean) =
    funcTest(s"${if revoking then "revoked" else "granted"} ledger rights start a new coverage segment on retry") {
      val alice = Party("Alice")
      val bob   = Party("Bob")
      val user  = User(primaryParty = alice, canActAs = if revoking then Seq(bob) else Seq.empty)
      Given:
        DamlSdk.dar(source) ++ DamlSdk.parties(alice, bob) ++ Postgres.database >+> DamlSdk.deploy
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("Note:setup", alice.id)
      When:
        Pqs.attemptRelationalPipeline(
          s"--pipeline-oauth-clientid=${user.id}",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Never",
          "--pipeline-datasource=TransactionTreeStream"
        )
      And:
        Pqs.stdoutContainsWithin("Continuing from offset", duration = 1.minute)
      And:
        FuncTest.retryUntilTimeout(Postgres.query {
          sql"select count(*) from pqs_relational.__rel_contracts"
            .query[Long]
            .selectOne
            .map(n => assertTrue(n.contains(1L)))
        })
      When:
        Postgres.call(sql"""
          create sequence pqs_relational.retry_once;
          create function pqs_relational.fail_once() returns trigger language plpgsql as $$$$
          begin
            if nextval('pqs_relational.retry_once') = 1 then
              raise exception 'injected retry after rights change' using errcode = '08006';
            end if;
            return new;
          end; $$$$;
          create trigger retry_once before insert on pqs_relational.__rel_transactions
            for each row execute function pqs_relational.fail_once();
        """)
      When:
        if revoking then Ledger.revokeRights(alice.id, user.id).unit else Ledger.grantRights(bob.id, user.id).unit
      And:
        DamlSdk.runScript("Note:setup", if revoking then bob.id else alice.id)
      And:
        FuncTest.retryUntilTimeout(Postgres.query {
          sql"""select exists (select 1 from pqs_relational.__query_coverage
                 where completed_at is null and ingested_parties @> array[${bob.id}]::text[]
                   and (ingested_parties @> array[${alice.id}]::text[]) = ${!revoking})"""
            .query[Boolean]
            .selectOne
            .map(result => assertTrue(result.contains(true)))
        })
      And:
        DamlSdk.runScript("Note:setup", if revoking then alice.id else bob.id)
      Then:
        FuncTest.retryUntilTimeout(Postgres.query {
          for
            rows <- sql"""select ingested_parties @> array[${bob.id}]::text[], completed_at is not null, through_offset,
                                 ingested_parties @> array[${alice.id}]::text[]
                          from pqs_relational.__query_coverage order by started_at, coverage_id"""
              .query[(Boolean, Boolean, Long, Boolean)]
              .selectAll
            instances <- sql"select count(distinct instance_id) from pqs_relational.__query_coverage"
              .query[Long]
              .selectOne
            contracts <- sql"select count(*) from pqs_relational.__rel_contracts".query[Long].selectOne
          yield assertTrue(
            rows.size >= 2,
            rows.headOption.exists((bobVisible, closed, _, aliceVisible) =>
              bobVisible == revoking && closed && aliceVisible
            ),
            rows.lastOption.exists((bobVisible, closed, _, aliceVisible) =>
              bobVisible && !closed && aliceVisible != revoking
            ),
            rows.headOption.zip(rows.lastOption).exists((first, last) => first._3 < last._3),
            instances.contains(1L),
            contracts.contains(if revoking then 2L else 3L)
          )
        })
    }
