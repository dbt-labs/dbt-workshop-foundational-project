HOL 2026 Snippets 10 of 10 files
Filter by filename or content…
Theme
Project setup
dbt_project.yml
sources.yml
Staging models
stg_nations.sql
stg_regions.sql
Marts
dim_customers.sql
monthly_sales.sql
monthly_sales.yml
Tests
assert_net_item_sales_amount_positive.sql
Checks
all_models_have_descriptions.sql
Other
customer_tier.sql
Project setup
dbt_project.yml
42 lines
Gist ↗
Copy

# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'foundational_project'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: 'default'

# These configurations specify where dbt should look for different types of files.
# The `source-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
    - "target"
    - "dbt_modules"

info_schema: 
  version: 1


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the jaffle_shop project
# as tables. These settings can be overridden in the individual model files
# using the `{{ config(...) }}` macro.

models:
  foundational_project:
    +static_analysis: strict
    +materialized: view
    +access: public
sources.yml
18 lines
Gist ↗
Copy
sources:
  - name: tpch
    description: "TPCH data source from Snowflake Sample Data"
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        description: "One record per order"
      - name: customer
        description: "One record per customer"
      - name: lineitem
        description: "One record per line item within a single order (1 -> n)"
      - name: nation 
      - name: part 
      - name: partsupp 
      - name: region 
      - name: supplier
      
Staging models
stg_nations.sql
21 lines
Gist ↗
Copy
with

source as (

   select * from {{ source('tpch', 'nation') }}

),

renamed as (

   select
       n_nationkey as nation_key,
       n_name as nation,
       n_regionkey as region_key,
       n_comment as nation_comment

   from source

)

select * from renamed
stg_regions.sql
20 lines
Gist ↗
Copy
with

source as (

   select * from {{ source('tpch', 'region') }}

),

renamed as (

   select
       r_regionkey as region_key,
       r_name as region,
       r_comment as region_comment

   from source

)

select * from renamed
Marts
dim_customers.sql
19 lines
Gist ↗
Copy
with
    customer as (select * from {{ ref("stg_customers") }}),

    nation as (select * from {{ ref("stg_nations") }}),

    region as (select * from {{ ref("stg_regions") }})

select
    customer.customer_key,
    customer.name,
    customer.address,
    nation,
    region,
    customer.phone_number,
    customer.account_balance,
    customer.market_segment
from customer
inner join nation on customer.nation_key = nation.notion_key
inner join region on nation.region_key = region.region_key
monthly_sales.sql
10 lines
Gist ↗
Copy
SELECT
   DATE_TRUNC('month', order_date) AS order_month,
   market_segment,
   SUM(gross_item_sales_amount) AS total_revenue
FROM
   {{ ref('fct_orders') }}
GROUP BY
   1, 2
ORDER BY
   1, 2
monthly_sales.yml
18 lines
Gist ↗
Copy
models:
- name: monthly_sales
  description: This model aggregates sales data on a monthly basis, providing insights
    into total revenue generated per market segment each month.
  columns:
  - name: order_month
    description: The month in which the order was placed.
    data_tests:
      - not_null
  - name: market_segment
    description: The market segment to which the sale belongs.
    data_tests:
      - not_null
  - name: total_revenue
    description: The total revenue generated from sales in the specified month and
      market segment.
    data_tests:
      - not_null
Tests
assert_net_item_sales_amount_positive.sql
6 lines
Gist ↗
Copy
select
   order_key,
   sum(net_item_sales_amount) as amount
from {{ ref('fct_orders') }}
group by 1
having not(amount >= 0)
Checks
all_models_have_descriptions.sql
3 lines
Gist ↗
Copy
select unique_id
from {{ info_schema('models') }}
where description = ''
Other
customer_tier.sql
46 lines
Gist ↗
Copy
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