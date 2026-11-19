# Foundational Project

A Snowflake dbt project that stages all tables in `TPCH_NOW.TPCH` and produces
order-level and monthly sales reporting models.

## Model flow

- `stg_customers`: one row per customer, including market segment.
- `stg_orders`: one row per order with source order attributes.
- `stg_line_items`: one row per order and line number, with discount and tax calculations.
- `stg_regions`: one row per region.
- `stg_nations`: one row per nation, including its parent region key.
- `stg_parts`: one row per part, with manufacturer, brand, packaging, and retail price.
- `stg_suppliers`: one row per supplier, including nation, contact details, and account balance.
- `stg_part_suppliers`: one row per part/supplier pair, with availability and supply cost.
- `stg_tbl_test`: unmodified rows from the auxiliary ID-only table; business purpose and key guarantees are unspecified.
- `fct_orders`: one row per order with matching customers and line items; materialized as a table.
- `monthly_sales`: one row per calendar order month and market segment.

Other models inherit the project's view materialization. Model and column descriptions
live in `models/staging/schema.yml`, `models/staging/reference_tables.yml`, and
`models/marts/schema.yml`; source descriptions live in `models/staging/sources.yml`.
The new business staging models include key and parent-relationship tests; part/supplier
pairs have a composite uniqueness test. No business-key assertions are imposed on `stg_tbl_test`.


## Sales definitions

`monthly_sales.total_revenue` sums **gross sales before discounts and tax**.
It does not represent net or recognized revenue. No status or return filtering is applied.
Line-item discounts are modeled as negative amounts; `net_item_sales_amount` includes tax.
Orders without a matching customer or line item are excluded from `fct_orders` and monthly reporting.

## Validate changes

Run these through Studio's command bar:

```sh
dbt parse
dbt build
```

`dbt build` builds selected models and executes their tests. To validate just the
monthly reporting dependency chain:

```sh
dbt build --select +monthly_sales
```

For an existing build, run its data tests with:

```sh
dbt test
```

Coverage includes unique and non-null customer/order keys, required reporting keys,
order-to-customer and line-item-to-order relationships, source status/return-code domains,
and composite uniqueness for line items and monthly sales. New codes fail domain tests
so changes can be reviewed before updating expectations.

If a test fails, inspect its failed keys or values and trace them to the staging source.
Relationship failures identify missing parents; composite-grain failures identify repeated
order/line-number or month/segment pairs. Fix the underlying issue before relaxing a test.

## Documentation

Browse model and column descriptions in dbt Catalog after deploying the updated project.
When adding a model, document its grain, inclusion rules, and calculations alongside its SQL,
and add tests for keys and any new join or aggregation risks.

[dbt data tests](https://docs.getdbt.com/docs/build/data-tests) ·
[dbt documentation](https://docs.getdbt.com/docs/build/documentation)
