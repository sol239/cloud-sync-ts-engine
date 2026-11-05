-- Explicit deletion is limited to the authenticated owner's selected sync space.
create function public.lyra_sync_delete_all(p_space text)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  caller uuid := auth.uid();
  deleted_count bigint;
begin
  if caller is null then raise insufficient_privilege using message = 'Sign in first'; end if;
  if p_space is null or length(p_space) not between 1 and 256 then
    raise invalid_parameter_value using message = 'Invalid sync space';
  end if;
  -- Share the append lock so a deletion cannot interleave with a publication.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(caller::text || ':' || p_space, 0));
  delete from public.lyra_sync_entries where owner_id = caller and space_id = p_space;
  get diagnostics deleted_count = row_count;
  return jsonb_build_object('deleted', deleted_count);
end;
$$;
revoke all on function public.lyra_sync_delete_all(text) from public, anon;
grant execute on function public.lyra_sync_delete_all(text) to authenticated;
notify pgrst, 'reload schema';
