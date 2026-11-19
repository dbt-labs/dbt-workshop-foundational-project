# Foundational Project

A dbt project on Snowflake that stages TPCH transactions, products, suppliers, and geography, plus an auxiliary test table. Order-level and monthly sales marts build on the transactional staging models.

## Project inventory

| Location | Contents and purpose |
| --- | --- |
| `models/staging/` | Nine source tables and nine staging views, with source/model column docs and structural data tests. |
| `models/marts/` | Two reporting models with column docs, data tests, and aggregation/join unit tests. |
| `analysis/` | Urgent-order reporting query and legacy SQL Server procedure, documented in `analysis/schema.yml`. Analyses are not executed by `dbt build`. |
| `tests/` | Singular composite-grain assertions and descriptions. |
| `macros/`, `seeds/`, `snapshots/` | Configured directories that currently contain no dbt resources. |
| `dbt_project.yml` | Configured paths, strict model analysis, and Information Schema version 1. Models default to public views; `fct_orders` is a table. |

## Sources and staging grains

The `tpch` source points to `tpch_now.tpch`. The declared tables match the schema inventory inspected during development; newly created warehouse tables must be explicitly added to dbt.

| Source table | Staging model | Grain |
| --- | --- | --- |
| `customer` | `stg_customers` | One row per customer key. |
| `orders` | `stg_orders` | One row per order key. |
| `lineitem` | `stg_line_items` | One row per order key and line number. |
| `region` | `stg_regions` | One row per region key; zero is valid. |
| `nation` | `stg_nations` | One row per nation key; zero is valid. |
| `supplier` | `stg_suppliers` | One row per supplier key. |
| `part` | `stg_parts` | One row per part key. |
| `partsupp` | `stg_part_suppliers` | One row per part key and supplier key. |
| `tbl_test` | `stg_tbl_test` | Source rows retained unchanged; ID contract and business meaning are unconfirmed. |

Staging does not filter, deduplicate, or join source rows. New reference models retain every source column with readable aliases. Existing transactional output columns are preserved. Orders and lineitem also contain source `loaded_at` metadata, documented without adding it to the existing model outputs or assuming its load semantics or a freshness SLA.

## Relationships and marts

- Nations reference regions. Customers and suppliers reference nations.
- Part-supplier pairs reference parts and suppliers. Line items reference orders, parts, and suppliers.
- Customers, orders, and line items feed `fct_orders`, which aggregates line amounts to one row per order and joins customer attributes. Inner joins require matching line items and customers.
- `fct_orders` feeds `monthly_sales`, grouped by calendar month and market segment, and the urgent-order analysis, which selects priority `1-URGENT`.

Foreign-key assertions validate these relationships; the new lookup tables are not joined into the existing marts. Reporting does not generate missing month/segment combinations. Order status and return flag do not exclude records from the marts.

## Sales definitions

- **Gross sales**: source line-item extended price for the entire line, before discounts and tax.
- **Discount amount**: negative gross sales multiplied by the discount fraction.
- **Tax amount**: tax rate multiplied by the sum of gross sales and the negative discount amount.
- **Net sales**: gross sales plus the negative discount amount plus tax.
- **Monthly total_revenue**: sum of gross order sales, before discounts and tax.

Derived line-item monetary values are cast to four decimal places. Per-unit prices are null when quantity is zero. Source order `total_price` is retained in staging; mart amounts are calculated independently from line items. Part retail price, supplier account balance, and part-supplier supply cost are separate source attributes. No reporting currency or historical inventory semantics are inferred.

## Project-wide validation

Run these commands in the dbt Studio command bar:

```sh
dbt parse
dbt build --no-defer
```

The build covers all enabled models and tests. `--no-defer` keeps model references on the development target; sources must remain accessible. State-aware execution may reuse unchanged nodes.

```sh
dbt test --no-defer
dbt test --select test_type:unit --no-defer
```

### Coverage and troubleshooting

- Primary keys have `unique` and `not_null` tests. Composite keys have component null checks and singular uniqueness tests.
- Foreign keys have `relationships` and required-key checks in staging, covering incoming key integrity without duplicating the same assertions on sources.
- Auxiliary `tbl_test.id` checks use warning severity because uniqueness and nullability are observed patterns, not a confirmed business contract.
- Unit fixtures protect discount-before-tax arithmetic, zero quantities, order aggregation/enrichment and inner-join exclusions, and monthly gross-revenue grouping. Pure source renames are covered by data tests.
- Freshness thresholds and categorical accepted-value lists are not assumed.

For relationship failures, inspect unmatched keys. For composite-grain failures, inspect the key pairs returned by tests in `tests/`. For unit failures, compare actual/expected fixture results against the documented calculations and joins.

## Analyses and legacy SQL

`analysis/urgent_order_analysis.sql` is an ad hoc SELECT query. Compile it with:

```sh
dbt compile --select path:analysis/urgent_order_analysis.sql
```

Compilation validates rendering, not query results. Analyses do not support dbt data or unit tests directly; reusable reporting that needs runtime tests should be promoted to a model.

`analysis/usp_build_fct_orders.sql` is legacy SQL Server T-SQL with destructive table-rebuild statements. It is reference material, is not Snowflake-compatible, and is not executed or runtime-tested by this project's dbt build. Its hardcoded references are outside dbt lineage.

## Documentation

- `models/staging/sources.yml`: every discovered source table and column.
- `models/staging/schema.yml`: transactional staging docs, tests, and arithmetic unit fixtures.
- `models/staging/reference_schema.yml`: geography, supplier, product, part-supplier, and auxiliary staging docs and tests.
- `models/marts/schema.yml`: reporting model docs, data tests, and unit fixtures.
- `analysis/schema.yml` and `tests/schema.yml`: analysis and singular-test descriptions.

Standard descriptions are available in dbt Catalog when project metadata is published by your documentation job.

References: [data tests](https://docs.getdbt.com/docs/build/data-tests), [unit tests](https://docs.getdbt.com/docs/build/unit-tests), and [documentation](https://docs.getdbt.com/docs/build/documentation).
