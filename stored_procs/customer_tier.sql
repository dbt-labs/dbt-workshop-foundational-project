/* customer_tier // dmitri k // 2021/03/22 // #DA-417
   WIP - thresholds are guesses, confirm w/ finance
   pg port: same alias-ref issue as customer_flags, the tier CASE keyed off
            the lifetime_value alias in the same select. aggregate moved out
            one level so the outer select can see it.

   !! READ BEFORE EDITING !!
   the ladder is ORDER DEPENDENT. tier1 is first and it swallows everything
   <= 200000, which means the tier4 arm never sees anything below that even
   though its range says 0. that is the existing behaviour, reporting has
   been reconciling against it for two years, and if you sort the arms into
   a "sensible" order you will silently re-tier about a third of the book.
   two more things that look like bugs and are not (yet):
     - 1999999 -> 2000000 falls through every arm and comes out NULL
     - 999999  -> 1000000 same
   -- dk, then re-flagged 2023-06-04 a.ferreira, still not fixed
   TODO: tier2/tier3 ranges overlap weird, the calcualtion needs review
*/
create or replace procedure retail_datamart.build_customer_tier()
language plpgsql
set search_path = retail_datamart, public
as
$$
declare
  v_null_tier bigint;
begin
  drop table if exists retail_datamart.customer_tier;
  create table retail_datamart.customer_tier (customer_key numeric(38,0), lifetime_value numeric(38,2), tier_name varchar);

  insert into retail_datamart.customer_tier (customer_key, lifetime_value, tier_name)
  select t.ck, t.ltv,
         case when t.ltv <= 200000 then 'tier1' when t.ltv > 2000000 then 'tier2' when t.ltv between 1000000 and 1999999 then 'tier3' when t.ltv between 0 and 999999 then 'tier4' end
    from (select cust.customer_key ck, sum(ord.total_price) ltv
            from stg_tpch_customers cust
                 inner join stg_tpch_orders ord on cust.customer_key = ord.customer_key
           group by cust.customer_key) t;

  -- the NULL bucket from the header. counted, not fixed.
  select count(*) into v_null_tier from retail_datamart.customer_tier where tier_name is null;
  if v_null_tier > 0 then
    raise notice 'customer_tier: % customers landed between bands (expected, see header)', v_null_tier;
  end if;

  raise notice 'customer_tier built';
end;
$$;
