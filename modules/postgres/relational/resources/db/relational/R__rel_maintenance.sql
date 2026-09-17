-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Match ProjectionRegistry's locks. Maintenance must exclude writers, backfills and schema changes.
create or replace procedure __rel_begin_maintenance() as
$$
begin
    perform pg_advisory_xact_lock(('x70716a5f70726f6a'::bit(64))::bigint);
    if not pg_try_advisory_xact_lock(('x70716a5f7772746c'::bit(64))::bigint) then
        raise exception 'stop the relational writer before pruning or redaction';
    end if;
    perform pg_advisory_xact_lock(('x70716a5f61637476'::bit(64))::bigint);
end;
$$ language plpgsql;

create or replace function redact_contract(p_contract_id text, p_redaction_id text) returns bigint as
$$
declare
    c __rel_contracts%rowtype;
    entity record;
    boundary bigint;
begin
    call __rel_begin_maintenance();
    if p_redaction_id is null or btrim(p_redaction_id) = '' then
        raise exception 'redaction id must not be empty';
    end if;
    select * into c from __rel_contracts where contract_id = p_contract_id for update;
    if not found then
        if exists (select 1 from __rel_redaction where contract_id = p_contract_id and redaction_id = p_redaction_id) then
            return 0;
        end if;
        raise exception 'cannot find contract %', p_contract_id;
    end if;
    if c.redaction_id = p_redaction_id then return 0; end if;
    if c.redaction_id is not null then raise exception 'contract % is already redacted', p_contract_id; end if;
    select tx_ix into boundary from latest_checkpoint();
    if boundary is null or c.created_tx_ix > boundary or c.archived_tx_ix is null or c.archived_tx_ix > boundary then
        raise exception 'contract % must be archived through the published watermark', p_contract_id;
    end if;

    -- Removing the entire payload row also removes every promoted value and its index entries.
    for entity in select base_table from __rel_entity
                  where pk = c.template_entity_pk or pk in (
                      select interface_pk from __rel_implements where template_pk = c.template_entity_pk)
    loop
        execute format('delete from %I where contract_pk = $1', entity.base_table) using c.contract_pk;
    end loop;
    update __rel_contracts set contract_key_json = null, contract_key_hash = null,
        metadata = null, redaction_id = p_redaction_id where contract_pk = c.contract_pk;
    update __rel_exercises x set argument_json = null, result_json = null, redaction_id = p_redaction_id
    from __query_events e where e.event_pk = x.event_pk and e.contract_id = p_contract_id
      and x.redaction_id is null;
    insert into __rel_redaction(contract_id, redaction_id, redacted_at)
    values (p_contract_id, p_redaction_id, now());
    return 1;
end;
$$ language plpgsql;

create or replace function redact_exercise(p_offset bigint, p_node integer, p_redaction_id text) returns bigint as
$$
declare
    event_id bigint;
    previous text;
begin
    call __rel_begin_maintenance();
    if p_redaction_id is null or btrim(p_redaction_id) = '' then
        raise exception 'redaction id must not be empty';
    end if;
    select x.event_pk, x.redaction_id into event_id, previous
    from __rel_exercises x join __query_events e using (event_pk)
    where e.ledger_offset = p_offset and e.node_id = p_node
      and e.tx_ix <= (select tx_ix from latest_checkpoint()) for update of x;
    if not found then
        if exists (select 1 from __rel_redaction where event_id_offset = p_offset
                   and event_id_node = p_node and redaction_id = p_redaction_id) then return 0; end if;
        raise exception 'cannot find published exercise (%, %)', p_offset, p_node;
    end if;
    if previous = p_redaction_id then return 0; end if;
    if previous is not null then raise exception 'exercise (%, %) is already redacted', p_offset, p_node; end if;
    update __rel_exercises set argument_json = null, result_json = null, redaction_id = p_redaction_id
    where event_pk = event_id;
    insert into __rel_redaction(event_id_offset, event_id_node, redaction_id, redacted_at)
    values (p_offset, p_node, p_redaction_id, now());
    return 1;
end;
$$ language plpgsql;

create or replace function __rel_prune(p_offset bigint, p_dry_run boolean)
returns table (pruning_boundary_offset bigint, deleted_contracts bigint, deleted_exercises bigint,
               deleted_events bigint, deleted_transactions bigint) as
$$
declare
    cutoff bigint;
    watermark rel_checkpoint;
begin
    call __rel_begin_maintenance();
    select * into watermark from latest_checkpoint();
    if p_offset is null or p_offset < 0 or watermark.ledger_offset is null or p_offset > watermark.ledger_offset then
        raise exception 'pruning offset must be within published history';
    end if;
    if p_offset <= coalesce(pruned_offset(), -1) then
        return query select null::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint;
        return;
    end if;
    select max(tx_ix) into cutoff from __rel_transactions where ledger_offset <= p_offset;
    if cutoff is null then
        return query select null::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint;
        return;
    end if;

    with removed_contracts as (
        select contract_pk from __rel_contracts where archived_tx_ix <= cutoff and created_tx_ix <= cutoff
    ), kept_contracts as (
        select * from __rel_contracts where contract_pk not in (select contract_pk from removed_contracts)
    ), removed_events as (
        select e.event_pk from __query_events e where e.tx_ix <= cutoff
          and not exists (select 1 from kept_contracts c where c.contract_id = e.contract_id
                          and (e.event_kind = 'create' or c.contract_pk = e.event_pk))
    )
    select (select count(*) from removed_contracts),
           (select count(*) from __rel_exercises where event_pk in (select event_pk from removed_events)),
           (select count(*) from removed_events),
           (select count(*) from __rel_transactions t where t.tx_ix < cutoff and t.tx_ix <> watermark.tx_ix
              and not exists (select 1 from kept_contracts c where c.created_tx_ix = t.tx_ix or c.archived_tx_ix = t.tx_ix)
              and not exists (select 1 from __query_events e where e.tx_ix = t.tx_ix
                              and e.event_pk not in (select event_pk from removed_events)))
    into deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;

    if not p_dry_run then
        insert into __rel_contract_tombstone(contract_id)
        select contract_id from __rel_contracts where archived_tx_ix <= cutoff and created_tx_ix <= cutoff
        union select contract_id from __rel_tmp_lifecycle where archived_tx_ix <= cutoff
        on conflict do nothing;
        delete from __rel_contract_visibility where contract_pk in (
            select contract_pk from __rel_contracts where archived_tx_ix <= cutoff and created_tx_ix <= cutoff);
        delete from __rel_contracts where archived_tx_ix <= cutoff and created_tx_ix <= cutoff;
        delete from __query_event_visibility where event_pk in (
            select e.event_pk from __query_events e where e.tx_ix <= cutoff
            and not exists (select 1 from __rel_contracts c where c.contract_id = e.contract_id
                            and (e.event_kind = 'create' or c.contract_pk = e.event_pk)));
        delete from __rel_exercises where event_pk in (
            select e.event_pk from __query_events e where e.tx_ix <= cutoff
            and not exists (select 1 from __rel_contracts c where c.contract_id = e.contract_id
                            and (e.event_kind = 'create' or c.contract_pk = e.event_pk)));
        delete from __rel_reassignments where event_pk in (
            select e.event_pk from __query_events e where e.tx_ix <= cutoff
            and not exists (select 1 from __rel_contracts c where c.contract_id = e.contract_id
                            and (e.event_kind = 'create' or c.contract_pk = e.event_pk)));
        delete from __query_events e where e.tx_ix <= cutoff
            and not exists (select 1 from __rel_contracts c where c.contract_id = e.contract_id
                            and (e.event_kind = 'create' or c.contract_pk = e.event_pk));
        delete from __rel_tmp_lifecycle where archived_tx_ix <= cutoff;
        delete from __rel_transactions t where t.tx_ix < cutoff and t.tx_ix <> watermark.tx_ix
            and not exists (select 1 from __rel_contracts c where c.created_tx_ix = t.tx_ix or c.archived_tx_ix = t.tx_ix)
            and not exists (select 1 from __query_events e where e.tx_ix = t.tx_ix);
        update __rel_pruning_metadata set pruned_offset = p_offset;
        update __query_coverage set create_history_complete = false, exercise_history_complete = false,
            archive_history_complete = false, archive_visibility_complete = false,
            reassignment_history_complete = false, assignment_origin_state_complete = false
        where actual_from_offset <= p_offset;
    end if;
    return query select p_offset, deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;
end;
$$ language plpgsql;

create or replace function prune_archived_to_offset(p_offset bigint)
returns table (pruning_boundary_offset bigint, deleted_contracts bigint, deleted_exercises bigint,
               deleted_events bigint, deleted_transactions bigint) as
$$ select * from __rel_prune(p_offset, false); $$ language sql;

create or replace function prune_archived_to_offset_dry_run(p_offset bigint)
returns table (pruning_boundary_offset bigint, deleted_contracts bigint, deleted_exercises bigint,
               deleted_events bigint, deleted_transactions bigint) as
$$ select * from __rel_prune(p_offset, true); $$ language sql;
