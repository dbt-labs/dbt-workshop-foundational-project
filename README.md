# Foundational Project

A Snowflake dbt project that transforms TPC-H customer, order, and line-item data into order-level facts and monthly gross sales by customer market segment.

## Data flow and model grains

The `tpch` source reads `customer`, `orders`, and `lineitem` from `tpch_now.tpch`, as configured in `models/staging/sources.yml`.

| Model | Grain | Purpose |
| --- | --- | --- |
| `stg_customers` | One row per `customer_key` | Standardize customer fields for order enrichment. |
| `stg_orders` | One row per `order_key` | Standardize order-header fields. |
| `stg_line_items` | One row per `order_key`, `line_number` | Standardize order lines and calculate sales, discounts, and tax. |
| `fct_orders` | One row per `order_key` | Aggregate line amounts and enrich orders with customer attributes. |
| `monthly_sales` | One row per `order_month`, `market_segment` | Summarize gross sales by calendar month and customer segment. |

Staging models and `monthly_sales` inherit view materialization. `fct_orders` explicitly materializes as a table.

## Interpreting sales amounts

- Gross item sales are the source extended line price before discounts and tax.
- Discounts are negative amounts. Tax is calculated on discounted sales.
- Net item sales include discounts and tax. Calculated line-item monetary fields are rounded to four decimal places.
- `monthly_sales.total_revenue` sums **gross** item sales; it excludes the effects of discounts and tax.
- `fct_orders` uses inner joins, so orders without line items or a matching customer are excluded. No order-status filter is applied.
- Amounts retain source units; the project does not perform currency conversion.

Detailed model and column descriptions live in `models/staging/schema.yml` and `models/marts/schema.yml` for use in dbt documentation and Catalog.

## Validation

Run `dbt build` in Studio to build all five models and execute their data tests. Run `dbt test` to execute tests against already-built relations.

Coverage includes unique and non-null customer/order keys, required join keys, customer-to-order and order-to-line-item relationships, non-null reporting dates, and composite-grain checks for line items and monthly sales. Composite-grain tests live in `tests/`; generic tests are defined alongside column documentation.

Tests on source-derived keys run in staging. The fact-table key is checked again because aggregation and joins can change its grain. Source part, supplier, and nation keys have no modeled parent tables, so this project does not assert relationships for them.

[dbt data test documentation](https://docs.getdbt.com/docs/build/data-tests)
