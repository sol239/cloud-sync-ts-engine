-- Allow host apps to register their own entity types; preserve owner isolation and checkpoint validation.
-- All writes go through this authenticated RPC. Serialize each owner/space
-- before identity allocation so pagination follows commit order.
create or replace function public.lyra_sync_append(p_space text, p_id uuid, p_kind text, p_body jsonb)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  caller uuid := auth.uid();
  existing public.lyra_sync_entries;
begin
  if caller is null then raise insufficient_privilege using message = 'Sign in first'; end if;
  -- Hold through commit: a later visible position cannot overtake this append.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(caller::text || ':' || p_space, 0));
  if p_space is null or length(p_space) not between 1 and 256
     or p_kind is null or p_kind not in ('operation', 'checkpoint')
     or p_id is null or jsonb_typeof(p_body) is distinct from 'object'
     or octet_length(p_body::text) > 67108864 then
    raise invalid_parameter_value using message = 'Invalid sync envelope';
  end if;
  if p_kind = 'operation' and (
    (p_body->>'id') is distinct from p_id::text
    or coalesce(p_body->>'schemaVersion', '') not in ('1', '2')
    or coalesce(p_body->>'action', '') not in ('upsert', 'delete')
    or (jsonb_typeof(p_body->'entityType') is distinct from 'string' or length(btrim(p_body->>'entityType')) not between 1 and 256)
    or coalesce(p_body->>'entityId', '') = '' or coalesce(p_body->>'deviceId', '') = ''
    or coalesce(p_body->>'sequence', '') !~ '^[1-9][0-9]{0,15}$'
  ) then raise invalid_parameter_value using message = 'Invalid sync operation'; end if;
  if p_kind = 'checkpoint' and (
    (p_body->>'format') is distinct from 'full-sync-v1'
    or jsonb_typeof(p_body->'operations') is distinct from 'array'
    or jsonb_typeof(p_body->'coveredOperationIds') is distinct from 'array'
  ) then raise invalid_parameter_value using message = 'Invalid checkpoint'; end if;

  if p_kind = 'checkpoint' then
    if exists (
      select 1 from jsonb_array_elements(p_body->'operations') op
      where jsonb_typeof(op) is distinct from 'object'
         or coalesce(op->>'schemaVersion', '') not in ('1', '2')
         or coalesce(op->>'action', '') not in ('upsert', 'delete')
         or (jsonb_typeof(op->'entityType') is distinct from 'string' or length(btrim(op->>'entityType')) not between 1 and 256)
         or coalesce(op->>'entityId', '') = ''
    ) then raise invalid_parameter_value using message = 'Invalid checkpoint operation'; end if;
    if exists (
      select 1 from jsonb_array_elements(p_body->'coveredOperationIds') covered
      where jsonb_typeof(covered) is distinct from 'string'
         or not exists (
           select 1 from public.lyra_sync_entries e
           where e.owner_id = caller and e.space_id = p_space and e.kind = 'operation'
             and e.id::text = covered #>> '{}'
         )
    ) then raise invalid_parameter_value using message = 'Checkpoint covers unknown history'; end if;
    -- Every covered entity needs a state or tombstone in the checkpoint.
    if exists (
      select 1 from public.lyra_sync_entries e
      where e.owner_id = caller and e.space_id = p_space and e.kind = 'operation'
        and (p_body->'coveredOperationIds') ? e.id::text
        and not exists (
          select 1 from jsonb_array_elements(p_body->'operations') op
          where op->>'entityType' = e.body->>'entityType'
            and op->>'entityId' = e.body->>'entityId'
        )
    ) then raise invalid_parameter_value using message = 'Checkpoint omits covered entity'; end if;
    -- Covered effects must be represented by equal/newer field and record clocks,
    -- or superseded by an entity tombstone. A list of IDs alone is not proof.
    if exists (
      with history as (
        select e.body from public.lyra_sync_entries e
        where e.owner_id = caller and e.space_id = p_space and e.kind = 'operation'
          and (p_body->'coveredOperationIds') ? e.id::text
      ), effects as (
        select h.body, field.name,
          coalesce(h.body->'fieldStamps'->>field.name, (h.body->>'createdAt') || '|' || (h.body->>'id')) as stamp
        from history h
        cross join lateral (
          select '__deleted__' as name where h.body->>'action' = 'delete'
          union all
          select value #>> '{}' from jsonb_array_elements(coalesce(h.body->'changedFields',
            (select coalesce(jsonb_agg(key), '[]'::jsonb) from jsonb_object_keys(coalesce(nullif(h.body->'payload', 'null'::jsonb), '{}'::jsonb)) key
             where key not in ('id', 'attributeRecords')))) where h.body->>'action' = 'upsert'
          union all
          select 'record:' || (value #>> '{}') from jsonb_array_elements(coalesce(h.body->'recordChanges'->'deletedKeys', '[]'::jsonb))
          union all
          select 'record:' || array_to_json(array[
            value->>'attributeId', value->>'datetimeUTC', value->>'routineId', value->>'routineActivityId'
          ])::text from jsonb_array_elements(coalesce(h.body->'recordChanges'->'upserts', h.body->'payload'->'attributeRecords', '[]'::jsonb))
        ) field
      )
      select 1 from effects effect
      where not exists (
        select 1 from jsonb_array_elements(p_body->'operations') op
        where op->>'entityType' = effect.body->>'entityType'
          and op->>'entityId' = effect.body->>'entityId'
          and (
            (op->>'action' = 'delete' and op->'fieldStamps'->>'__deleted__' >= effect.stamp)
            or (op->>'action' = 'upsert' and coalesce(op->'fieldStamps'->>effect.name,
              case when effect.name = '__deleted__' then op->'fieldStamps'->>'__entity__' end) >= effect.stamp)
          )
      )
    ) then raise invalid_parameter_value using message = 'Checkpoint omits covered effects'; end if;
  end if;

  insert into public.lyra_sync_entries(owner_id, space_id, id, kind, body)
  values (caller, p_space, p_id, p_kind, p_body)
  on conflict (owner_id, space_id, id) do nothing;
  select * into existing from public.lyra_sync_entries
    where owner_id = caller and space_id = p_space and id = p_id;
  if existing.kind is distinct from p_kind or existing.body is distinct from p_body then
    raise unique_violation using message = 'Operation ID already has different content';
  end if;
  return jsonb_build_object('id', p_id);
end;
$$;


revoke all on function public.lyra_sync_append(text, uuid, text, jsonb) from public, anon;
grant execute on function public.lyra_sync_append(text, uuid, text, jsonb) to authenticated;

-- Refresh PostgREST after removing the obsolete overloaded RPC.
notify pgrst, 'reload schema';
