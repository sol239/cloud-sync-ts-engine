-- Apply to the remote Supabase project, not the local SQLite database.
create table public.lyra_sync_entries (
  owner_id uuid not null references auth.users(id) on delete cascade,
  space_id text not null check (length(space_id) between 1 and 256),
  id uuid not null,
  kind text not null check (kind in ('operation', 'checkpoint')),
  body jsonb not null check (jsonb_typeof(body) = 'object'),
  created_at timestamptz not null default now(),
  primary key (owner_id, space_id, id)
);
create unique index lyra_sync_device_sequence on public.lyra_sync_entries
  (owner_id, space_id, (body->>'deviceId'), ((body->>'sequence')::bigint))
  where kind = 'operation';
alter table public.lyra_sync_entries enable row level security;
revoke all on public.lyra_sync_entries from anon, authenticated;
grant select on public.lyra_sync_entries to authenticated;
create policy own_sync_entries on public.lyra_sync_entries for select to authenticated
  using (owner_id = (select auth.uid()));

-- Only the RPC may insert. Owner identity always comes from the verified JWT.
create function public.lyra_sync_append(p_space text, p_id uuid, p_kind text, p_body jsonb)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  caller uuid := auth.uid();
  existing public.lyra_sync_entries;
begin
  if caller is null then raise insufficient_privilege using message = 'Sign in first'; end if;
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
    or coalesce(p_body->>'entityType', '') not in ('tag', 'attribute', 'trait', 'activity', 'routine', 'goal', 'diary', 'metadata', 'library-user', 'library-entry', 'user-library-entry')
    or coalesce(p_body->>'entityId', '') = '' or coalesce(p_body->>'deviceId', '') = ''
    or coalesce(p_body->>'sequence', '') !~ '^[1-9][0-9]{0,15}$'
  ) then raise invalid_parameter_value using message = 'Invalid sync operation'; end if;
  if p_kind = 'checkpoint' and (
    (p_body->>'format') is distinct from 'full-sync-v1'
    or jsonb_typeof(p_body->'operations') is distinct from 'array'
    or jsonb_typeof(p_body->'coveredOperationIds') is distinct from 'array'
  ) then raise invalid_parameter_value using message = 'Invalid checkpoint'; end if;

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

-- Keyset pagination is rescanned each run; no cursor can hide a late commit.
create function public.lyra_sync_list(p_space text, p_after text default '')
returns table(id uuid, kind text) language sql stable security invoker set search_path = '' as $$
  select e.id, e.kind from public.lyra_sync_entries e
  where e.owner_id = auth.uid() and e.space_id = p_space
    and (p_after = '' or e.id > p_after::uuid)
  order by e.id limit 200;
$$;
create function public.lyra_sync_read(p_space text, p_id uuid)
returns jsonb language sql stable security invoker set search_path = '' as $$
  select body from public.lyra_sync_entries
  where owner_id = auth.uid() and space_id = p_space and id = p_id;
$$;
revoke all on function public.lyra_sync_append(text, uuid, text, jsonb) from public, anon;
revoke all on function public.lyra_sync_list(text, text) from public, anon;
revoke all on function public.lyra_sync_read(text, uuid) from public, anon;
grant execute on function public.lyra_sync_append(text, uuid, text, jsonb) to authenticated;
grant execute on function public.lyra_sync_list(text, text) to authenticated;
grant execute on function public.lyra_sync_read(text, uuid) to authenticated;
