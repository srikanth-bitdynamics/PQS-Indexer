// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.relational

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.postgres.backend.transact
import com.digitalasset.pqs.postgres.relational.projection.ProjectionRegistry
import com.digitalasset.pqs.services.postgres.{Postgres, ProductionPool}
import zio.jdbc.*
import zio.test.*
import zio.ZIO

object MaintenanceSpec extends FuncTest[Postgres]:
  def shared = Postgres.instance

  private val seed = ProductionPool.relationalSchema *> transact {
    sql"""
      insert into __rel_entity(pk, package_name, module_name, entity_name, kind, base_table)
        values (1, 'Test', 'Main', 'Asset', 'template', 'payload'), (2, 'Test', 'Main', 'View', 'interface', 'view_payload');
      insert into __rel_implements values (1, 2);
      create table payload(contract_pk bigint primary key references __rel_contracts on delete cascade,
                           payload_json jsonb not null, secret text);
      create index secret_idx on payload(secret);
      create table view_payload(contract_pk bigint primary key references __rel_contracts on delete cascade, view_json jsonb not null);
      insert into __rel_transactions(tx_ix, ledger_offset) values (1, 10), (2, 20), (3, 30), (4, 40);
      insert into __rel_contracts(contract_pk, contract_id, template_entity_pk, representative_package_id,
                                 created_tx_ix, archived_tx_ix, created_at_offset, archived_at_offset,
                                 contract_key_json, contract_key_hash, metadata, source_kind)
        values (1, 'active', 1, 'pkg', 1, null, 10, null, null, null, null, 'stream'),
               (2, 'archived', 1, 'pkg', 1, 3, 10, 30, '"secret-key"', '\x1234', '\x5678', 'stream');
      insert into payload values (1, '{"owner":"keep"}', 'keep'), (2, '{"owner":"erase"}', 'erase');
      insert into view_payload values (2, '{"owner":"erase"}');
      insert into __query_events(event_pk, tx_ix, ledger_offset, node_id, contract_id, template_entity_pk,
                                 event_kind, source_kind, visibility_complete)
        values (1,1,10,0,'active',1,'create','stream',true), (2,1,10,1,'archived',1,'create','stream',true),
               (3,2,20,0,'archived',1,'exercise','stream',true), (4,3,30,0,'archived',1,'archive','stream',true),
               (5,2,20,1,'active',1,'unassign','stream',true), (6,3,30,1,'active',1,'assign','stream',true);
      insert into __rel_exercises(event_pk, choice_name, consuming, argument_json, result_json)
        values (3,'Inspect',false,'"secret-arg"','"secret-result"');
      insert into __query_event_visibility select event_pk,'Alice' from __query_events;
      insert into __rel_contract_visibility values (1,'Alice','signatory'), (2,'Alice','signatory');
      insert into __rel_reassignments(event_pk,reassignment_id,source_synchronizer_id,target_synchronizer_id,reassignment_counter)
        values (5,'move','A','B',1), (6,'move','A','B',1);
      update __rel_watermark set tx_ix=4, ledger_offset=40;
      insert into __rel_tmp_lifecycle values ('delayed', 1, 10);
    """.execute
  }

  private def cli(command: String*) = ZIO.scoped {
    for
      config <- ProductionPool.config().build
      c = config.get
      tls = c.tls.caCertificate.toList.flatMap(f => Seq("--postgres-tls-cafile", f.toString)) ++
        c.tls.privateKey.toList.flatMap(f => Seq("--postgres-tls-key", f.toString)) ++
        c.tls.certificate.toList.flatMap(f => Seq("--postgres-tls-cert", f.toString))
      result <- com.digitalasset.pqs.Main
        .run(
          (Seq("datastore", "postgres-relational") ++ command ++ Seq(
            "--postgres-host",
            c.host,
            "--postgres-port",
            c.port.toString,
            "--postgres-database",
            c.database,
            "--postgres-username",
            c.username,
            "--postgres-password",
            "postgres",
            "--postgres-tls-mode",
            "Require"
          ) ++ tls).toArray
        )
        .exit
    yield result
  }

  private val remaining = transact {
    sql"""select (select count(*) from __rel_contracts), (select count(*) from payload),
                  (select count(*) from __query_events), (select count(*) from __rel_transactions)"""
      .query[(Long, Long, Long, Long)]
      .selectOne
  }

  def spec = suite("relational maintenance")(
    funcTest("maintenance CLI defaults to dry run and routes both redaction commands") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _         <- seed
          before    <- remaining
          dry       <- cli("prune", "--prune-offset", "30")
          unchanged <- remaining
          exercise <- cli(
            "redact",
            "exercise",
            "--redact-offset",
            "20",
            "--redact-node",
            "0",
            "--redact-redactionid",
            "exercise"
          )
          contract <- cli("redact", "contract", "--redact-contractid", "archived", "--redact-redactionid", "contract")
          audits   <- transact(sql"select count(*) from __rel_redaction".query[Long].selectOne)
          force    <- cli("prune", "--prune-offset", "30", "--prune-mode", "Force")
          after    <- remaining
        yield assertTrue(
          before == unchanged,
          after.contains((1L, 1L, 1L, 3L)),
          audits.contains(2L),
          Seq(dry, exercise, contract, force).forall(_.isSuccess)
        )
    },
    funcTest("dry run is non-mutating; pruning retains active state and removes related history atomically") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- seed
          _ <- transact(
            sql"""insert into __query_coverage(source_kind,actual_from_offset,ingested_all_parties,tree_stream,
            create_history_complete,exercise_history_complete,archive_history_complete,archive_visibility_complete,
            reassignment_history_complete,assignment_origin_state_complete,started_at)
            select 'stream'::rel_source_kind,boundary,true,true,true,true,true,true,true,true,now()
            from (values (0),(40)) starts(boundary)""".execute
          )
          before <- remaining
          dry <- transact(
            sql"select * from prune_archived_to_offset_dry_run(30)"
              .query[(Option[Long], Long, Long, Long, Long)]
              .selectOne
          )
          unchanged <- remaining
          dryCoverage <- transact(
            sql"select bool_and(reassignment_history_complete) from __query_coverage".query[Boolean].selectOne
          )
          result <- transact(
            sql"select * from prune_archived_to_offset(30)"
              .query[(Option[Long], Long, Long, Long, Long)]
              .selectOne
          )
          after <- remaining
          state <- transact(
            sql"""select (select count(*) from active_contracts),
            (select count(*) from __query_event_visibility), (select count(*) from __rel_exercises),
            (select count(*) from __rel_reassignments), (select count(*) from view_payload),
            (select count(*) from __rel_contract_tombstone), pruned_offset(), latest_offset()"""
              .query[(Long, Long, Long, Long, Long, Long, Long, Long)]
              .selectOne
          )
          repeat <- transact(
            sql"select pruning_boundary_offset is null from prune_archived_to_offset(30)"
              .query[Boolean]
              .selectOne
          )
          coverage <- transact(sql"""select
            (select not (create_history_complete or exercise_history_complete or archive_history_complete or
              archive_visibility_complete or reassignment_history_complete or assignment_origin_state_complete)
              from __query_coverage where actual_from_offset=0),
            (select reassignment_history_complete and assignment_origin_state_complete
              from __query_coverage where actual_from_offset=40)""".query[(Boolean, Boolean)].selectOne)
          oldRead <- transact(sql"select set_latest(20)".query[Long].selectOne).either
        yield assertTrue(
          before == unchanged,
          dryCoverage.contains(true),
          coverage.contains((true, true)),
          dry == result,
          result.contains((Some(30L), 1L, 1L, 5L, 1L)),
          after.contains((1L, 1L, 1L, 3L)),
          state.contains((1L, 1L, 0L, 0L, 0L, 2L, 30L, 40L)),
          repeat.contains(true),
          oldRead.isLeft
        )
    },
    funcTest("contract redaction erases JSON, typed fields, interface values, keys, blobs and exercise payloads") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- seed
          _ <- transact(sql"""insert into __rel_transactions(tx_ix,ledger_offset) values (5,50);
            insert into __query_events(event_pk,tx_ix,ledger_offset,node_id,contract_id,template_entity_pk,event_kind,source_kind,visibility_complete)
              values (7,5,50,0,'archived',1,'exercise','stream',true);
            insert into __rel_exercises(event_pk,choice_name,consuming,argument_json,result_json)
              values (7,'Inspect',false,'"unpublished"','"unpublished"')""".execute)
          rejected <- transact(sql"select redact_contract('active','request')".query[Long].selectOne).either
          first    <- transact(sql"select redact_contract('archived','request')".query[Long].selectOne)
          second   <- transact(sql"select redact_contract('archived','request')".query[Long].selectOne)
          erased <- transact(sql"""select
            (select count(*) from payload where secret='erase'), (select count(*) from view_payload),
            (select contract_key_json is null and contract_key_hash is null and metadata is null and redaction_id='request'
             from __rel_contracts where contract_id='archived'),
            (select bool_and(argument_json is null and result_json is null) from __rel_exercises),
            (select count(*) from __rel_redaction)""".query[(Long, Long, Boolean, Boolean, Long)].selectOne)
        yield assertTrue(
          rejected.isLeft,
          first.contains(1L),
          second.contains(0L),
          erased.contains((0L, 0L, true, true, 1L))
        )
    },
    funcTest("exercise redaction is scoped and retryable, and refuses unknown events") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _        <- seed
          first    <- transact(sql"select redact_exercise(20,0,'request')".query[Long].selectOne)
          second   <- transact(sql"select redact_exercise(20,0,'request')".query[Long].selectOne)
          missing  <- transact(sql"select redact_exercise(20,99,'request')".query[Long].selectOne).either
          payloads <- transact(sql"select count(*) from payload".query[Long].selectOne)
          value <- transact(
            sql"select argument_json is null and result_json is null from __rel_exercises"
              .query[Boolean]
              .selectOne
          )
        yield assertTrue(
          first.contains(1L),
          second.contains(0L),
          missing.isLeft,
          payloads.contains(2L),
          value.contains(true)
        )
    },
    funcTest("maintenance refuses a live writer and never prunes beyond the published boundary") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _      <- seed
          before <- remaining
          tooFar <- transact(
            sql"select * from prune_archived_to_offset(41)".query[(Option[Long], Long, Long, Long, Long)].selectOne
          ).either
          blocked <- ZIO.scoped {
            for
              pool       <- ZIO.service[ZConnectionPool]
              connection <- pool.transaction.build
              _ <- sql"select 1 from pg_advisory_xact_lock(${ProjectionRegistry.writerLockKey})"
                .query[Int]
                .selectOne
                .provideEnvironment(connection)
              result <- transact(sql"select redact_contract('archived','request')".query[Long].selectOne).either
            yield result
          }
          after <- remaining
        yield assertTrue(tooFar.isLeft, blocked.isLeft, before == after)
    },
    funcTest("a failed payload deletion rolls back redaction and pruning together with their audit changes") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- seed
          _ <- transact(sql"""create table dependent_payload(id bigint references payload(contract_pk));
            insert into dependent_payload values (2)""".execute)
          before <- remaining
          redact <- transact(sql"select redact_contract('archived','request')".query[Long].selectOne).either
          prune <- transact(
            sql"select * from prune_archived_to_offset(30)"
              .query[(Option[Long], Long, Long, Long, Long)]
              .selectOne
          ).either
          after <- remaining
          metadata <- transact(
            sql"""select pruned_offset() is null,
            (select count(*) from __rel_redaction), (select count(*) from __rel_contract_tombstone),
            (select count(*) from __rel_contract_visibility),
            (select count(*) from __rel_exercises where argument_json is not null)"""
              .query[(Boolean, Long, Long, Long, Long)]
              .selectOne
          )
        yield assertTrue(redact.isLeft, prune.isLeft, before == after, metadata.contains((true, 0L, 0L, 2L, 1L)))
    },
    funcTest("pruning preserves an active contract's create even if assignment allocated its identity first") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- seed
          _ <- transact(sql"""delete from payload where contract_pk=1;
            update __rel_contracts set contract_pk=7 where contract_id='active';
            update __rel_contract_visibility set contract_pk=7 where contract_pk=1;
            insert into payload values (7,'{"owner":"keep"}','keep');
            insert into __query_events(event_pk,tx_ix,ledger_offset,node_id,contract_id,template_entity_pk,event_kind,source_kind,visibility_complete)
              values (7,2,20,2,'active',1,'assign','stream',true)""".execute)
          _ <- transact(
            sql"select * from prune_archived_to_offset(30)"
              .query[(Option[Long], Long, Long, Long, Long)]
              .selectOne
          )
          events <- transact(sql"select event_pk from __query_events order by event_pk".query[Long].selectAll)
        yield assertTrue(events.toSeq == Seq(1L, 7L))
    },
    funcTest("restart cleanup removes unpublished movement and visibility without changing published rows") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- seed
          _ <- transact(sql"""insert into __rel_transactions(tx_ix,ledger_offset) values (5,50);
            insert into __query_events(event_pk,tx_ix,ledger_offset,node_id,contract_id,template_entity_pk,event_kind,source_kind,visibility_complete)
              values (7,5,50,0,'active',1,'assign','stream',true);
            insert into __rel_reassignments(event_pk,reassignment_id,source_synchronizer_id,target_synchronizer_id,reassignment_counter)
              values (7,'retry','A','B',2);
            insert into __rel_pending_visibility values ('active','Bob','witness',5);
            call __rel_delete_transactions_after(4);
            update __rel_watermark set tx_ix=4,ledger_offset=40""".execute)
          rows <- transact(sql"""select (select count(*) from __rel_reassignments),
            (select count(*) from __rel_pending_visibility),
            (select count(*) from __rel_contract_visibility where party='Bob')""".query[(Long, Long, Long)].selectOne)
        yield assertTrue(rows.contains((2L, 0L, 0L)))
    },
    funcTest("an archive before its create remains pending and never yields an active contract") {
      Given:
        Postgres.database >+> ProductionPool.layer()
      Then:
        for
          _ <- seed
          _ <- transact(sql"update __rel_watermark set tx_ix=4, ledger_offset=40".execute)
          pending <- transact(
            sql"select count(*) from __rel_tmp_lifecycle where contract_id='delayed'".query[Long].selectOne
          )
          _ <- transact(
            sql"""insert into __rel_contracts(contract_pk,contract_id,template_entity_pk,representative_package_id,
                   created_tx_ix,source_kind) values (7,'delayed',1,'pkg',4,'assignment');
                 update __rel_watermark set tx_ix=4, ledger_offset=40""".execute
          )
          archived <- transact(
            sql"select isempty(life_ix), archived_tx_ix from __rel_contracts where contract_id='delayed'"
              .query[(Boolean, Long)]
              .selectOne
          )
        yield assertTrue(pending.contains(1L), archived.contains((true, 1L)))
    }
  )
