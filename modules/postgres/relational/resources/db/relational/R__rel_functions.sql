create or replace function __rel_make_aliases(package_name text, module_name text, entity_name text) returns text[] as
$$
declare
    q_name  text;
    fq_name text;
begin
    q_name := module_name || ':' || entity_name;
    fq_name := package_name || ':' || q_name;
    return array [fq_name, q_name, entity_name];
end;
$$ language plpgsql immutable parallel safe strict;

create or replace procedure __rel_initialize_package(package_name text, package_version text, package_id text) as
$$
declare
    pkg bigint;
begin
    select pk from __rel_package pkgs
    where pkgs.name = package_name and pkgs.version = package_version and pkgs.id = package_id
    into pkg;
    if pkg is null then
        insert into __rel_package(name, version, id) values (package_name, package_version, package_id);
    end if;
end;
$$ language plpgsql;

create or replace procedure __rel_initialize_entity(
    package_name text,
    module_name text,
    entity_name text,
    kind rel_entity_kind
) as
$$
declare
    entity bigint;
begin
    select pk from __rel_entity e
    where e.package_name = __rel_initialize_entity.package_name
      and e.module_name = __rel_initialize_entity.module_name
      and e.entity_name = __rel_initialize_entity.entity_name
      and e.kind = __rel_initialize_entity.kind
    into entity;
    if entity is null then
        insert into __rel_entity(package_name, module_name, entity_name, kind, aliases)
        values (package_name, module_name, entity_name, kind,
                __rel_make_aliases(package_name, module_name, entity_name));
    end if;
end;
$$ language plpgsql;

create or replace procedure __rel_initialize_choice(
    package_name text,
    module_name text,
    entity_name text,
    choice text,
    consuming boolean
) as
$$
declare
    ch bigint;
begin
    select pk from __rel_choice c
    where c.package_name = __rel_initialize_choice.package_name
      and c.module_name = __rel_initialize_choice.module_name
      and c.entity_name = __rel_initialize_choice.entity_name
      and c.choice = __rel_initialize_choice.choice
    into ch;
    if ch is null then
        insert into __rel_choice(package_name, module_name, entity_name, choice, consuming)
        values (package_name, module_name, entity_name, choice, consuming);
    end if;
end;
$$ language plpgsql;

create or replace procedure __rel_initialize_implements(template_fqn text, interface_fqn text) as
$$
declare
    pk_template  bigint;
    pk_interface bigint;
begin
    select pk from __rel_entity e where template_fqn = any (e.aliases) and e.kind = 'template' into pk_template;
    select pk from __rel_entity e where interface_fqn = any (e.aliases) and e.kind = 'interface' into pk_interface;
    if pk_template is not null and pk_interface is not null
        and not exists (select 1 from __rel_implements i
                        where i.template_pk = pk_template and i.interface_pk = pk_interface) then
        insert into __rel_implements(template_pk, interface_pk) values (pk_template, pk_interface);
    end if;
end;
$$ language plpgsql;

create or replace function oldest_checkpoint() returns setof rel_checkpoint as
$$
    select ledger_offset, tx_ix from __rel_transactions order by ledger_offset limit 1;
$$ language sql rows 1 stable parallel safe;

create or replace function latest_checkpoint() returns setof rel_checkpoint as
$$
    select ledger_offset, tx_ix from __rel_watermark where ledger_offset is not null and tx_ix is not null;
$$ language sql rows 1 stable parallel safe;

create or replace function pruned_offset() returns bigint as
$$
    select pruned_offset from __rel_pruning_metadata;
$$ language sql stable parallel safe;

create or replace procedure __rel_delete_transactions_after(cutoff_ix bigint) as
$$
declare
    work_exists boolean;
begin
    select exists(select 1 from __rel_transactions where tx_ix > cutoff_ix) into work_exists;
    if work_exists then
        delete from __query_event_visibility
        where event_pk in (select event_pk from __query_events where tx_ix > cutoff_ix);
        delete from __rel_exercises
        where event_pk in (select event_pk from __query_events where tx_ix > cutoff_ix);
        delete from __query_events where tx_ix > cutoff_ix;
        delete from __rel_contract_visibility
        where contract_pk in (select contract_pk from __rel_contracts where created_tx_ix > cutoff_ix);
        update __rel_contracts set archived_tx_ix = null, archived_at_offset = null where archived_tx_ix > cutoff_ix;
        delete from __rel_contracts where created_tx_ix > cutoff_ix;
        delete from __rel_tmp_lifecycle where archived_tx_ix > cutoff_ix;
        delete from __rel_transactions where tx_ix > cutoff_ix;
    end if;
end;
$$ language plpgsql;

create or replace procedure __rel_cleanup_transactions_after_watermark() as
$$
declare
    latest_ix bigint;
begin
    lock table __rel_watermark in exclusive mode;
    select c.tx_ix from latest_checkpoint() c into latest_ix;
    call __rel_delete_transactions_after(coalesce(latest_ix, 0));
    update __rel_watermark set instance_id = current_setting('scribe.instance');
end;
$$ language plpgsql;
