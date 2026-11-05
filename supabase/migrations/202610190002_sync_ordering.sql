-- Fix keyset pagination ordering and ingestion tracking.
-- Replace UUID-ordered pagination (which is pseudo-random for v4 UUIDs) with
-- a stable ingestion sequence so lyra_sync_list returns entries in insertion order.

-- Add an auto-incrementing ingestion position so pagination is deterministic.
alter table public.lyra_sync_entries
  add column seq bigint generated always as identity;

create unique index lyra_sync_seq on public.lyra_sync_entries (owner_id, space_id, seq);

-- Replace the list function to paginate by seq (stable insertion order).
-- p_after is now the last seq value received, defaulting to 0.
create or replace function public.lyra_sync_list(p_space text, p_after bigint default 0)
returns table(id uuid, kind text, seq bigint) language sql stable security invoker set search_path = '' as $$
  select e.id, e.kind, e.seq
  from public.lyra_sync_entries e
  where e.owner_id = auth.uid()
    and e.space_id = p_space
    and e.seq > p_after
  order by e.seq
  limit 200;
$$;

revoke all on function public.lyra_sync_list(text, bigint) from public, anon;
grant execute on function public.lyra_sync_list(text, bigint) to authenticated;

