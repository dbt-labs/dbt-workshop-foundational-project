# Foundational Project

A Snowflake dbt project that stages TPC-H sales, product, supplier, and geographic data and produces order-level and monthly sales models. It also exposes `tbl_test` as a pass-through staging view with unconfirmed business semantics.

## Data flow

The `tpch` source is configured in `models/staging/sources.yml` and currently reads from `tpch_now.tpch`. Every table discovered in that schema has a staging view.

- `stg_customers`: one row per customer, including customer market segment.
- `stg_orders`: one row per order, with customer references and order-header attributes.
- `stg_line_items`: one row per order and line number, with per-unit prices, discounts, tax, and line totals.
- `stg_parts`: one row per part, retaining product attributes and source retail price.
- `stg_suppliers`: one row per supplier, retaining contact details, nation, and account balance.
- `stg_part_suppliers`: one row per part/supplier pair with available quantity and per-unit supply cost. Join using both keys to avoid fan-out.
- `stg_nations`: one row per nation, linking customer and supplier geography to a region.
- `stg_regions`: one row per geographic region.
- `stg_tbl_test`: every source row with its ID unchanged; business purpose, uniqueness, and nullability are unconfirmed.
- `fct_orders`: one row per order, combining aggregated line-item amounts with customer attributes. Inner joins exclude orders without matching line items or customers.
- `monthly_sales`: one row per order month and customer market segment, summing gross sales. Missing month/segment combinations are not zero-filled.

Model and column descriptions live beside the SQL in `models/staging/schema.yml`, `models/staging/additional_tables.yml`, and `models/marts/schema.yml`, where dbt can display them in Catalog. The new staging views preserve every source column with readable aliases and do not filter, aggregate, or deduplicate source rows. The existing sales marts remain unchanged.


## Sales definitions

- **Gross sales**: line-item extended prices before discounts and tax.
- **Discount amounts**: stored as negative values, so gross sales plus discounts gives discounted sales.
- **Tax**: calculated on discounted line-item sales.
- **Net sales**: gross sales plus negative discounts plus tax; this includes tax.
- **Monthly `total_revenue`**: gross sales before discounts and tax, despite the column name. All order statuses and return flags are included.

Calculated line-item monetary fields are cast to four decimal places before order-level aggregation. No currency conversion is performed by these models.

## Validation

Run the following commands through Studio's command bar:

```text
dbt parse
dbt build
```

The build evaluates primary-key uniqueness and null checks, staging customer/order relationships, required monthly grain fields, and singular tests for the composite grains of line items and monthly sales. Tests use dbt's built-in macros and project SQL, with no additional package dependency.

To run tests against existing relations without rebuilding models:

```text
dbt test
```

A failing test identifies a violated expectation; investigate the data before changing the test or model. The singular test descriptions in `tests/schema.yml` include initial debugging guidance.

## Project layout

- `models/staging/`: source definitions, staging SQL, descriptions, and generic tests.
- `models/marts/`: order and monthly sales models, descriptions, and generic tests.
- `tests/`: composite-grain SQL tests and their descriptions.
- `analysis/`, `macros/`, `seeds/`, `snapshots/`: additional configured dbt resource paths.

## References

- [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
- [dbt documentation](https://docs.getdbt.com/docs/build/documentation)
