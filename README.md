# Foundational Project

A dbt project on Snowflake that turns TPC-H customer, order, and line-item data into order-level facts and monthly gross-sales reporting.

## Project structure

- `models/staging/sources.yml` declares the three source tables in `TPCH_NOW.TPCH`.
- `stg_customers`, `stg_orders`, and `stg_line_items` rename source columns. Line-item staging also derives prices, discounts, tax, and net sales.
- `fct_orders` is a table with one row per order, enriched with customer attributes and aggregated line-item amounts.
- `monthly_sales` is a view with one row per order month and customer market segment.
- `models/schema.yml` contains model and column documentation and generic data tests.
- `tests/` contains focused grain, amount-range, and financial-reconciliation data tests.

## Reading the sales measures

`monthly_sales.total_revenue` is **gross sales before discounts and tax**, summed from extended line prices. It includes all order statuses and return flags; it is not a recognized-revenue measure. Monetary values retain source units without currency conversion.

Line-item discount and tax rates are fractions (0.10 means 10%). Discounts are stored as negative amounts. Net sales equal gross sales plus the negative discount amount plus tax. Calculated line-item amounts are cast to four decimal places before order-level aggregation.

`fct_orders` uses inner joins, so orders without matching customers or line items are excluded. Monthly sales includes only month-segment combinations present in those facts; missing combinations are not zero-filled, and the current month can be partial. A null market segment is retained as its own reporting group.

## Validation

Run the complete project and its data tests:

```sh
dbt build
```

For development-only validation, without production deferral:

```sh
dbt build --no-defer
```

Validate YAML and project structure without executing warehouse queries:

```sh
dbt parse
```

Tests cover primary keys, required join keys and order dates, customer/order relationships, observed order and line-item status codes, composite grains, line-item amount/rate bounds, and order-level financial reconciliation. Tests use built-in dbt checks and singular SQL; no external package is required.

If a test fails, inspect its failing keys and trace them into staging and the source tables. For reconciliation failures, inspect the order's contributing line-item amounts and their four-decimal rounding. For status failures, confirm the upstream code's meaning before extending the allowed list.

## Documentation and limitations

Model grains, inclusion rules, and all model output columns are documented in `models/schema.yml` for dbt Catalog. Source context is documented alongside source declarations. No source freshness SLA is defined because this project has no configured ingestion timestamp.
