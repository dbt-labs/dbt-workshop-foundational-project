# Foundational Project

A Snowflake dbt project that stages TPC-H customer, order, and line-item data
and produces order-level and monthly sales reporting.

## Data flow and grain

The `tpch` source is configured in `models/staging/sources.yml`.

| Model | Grain | Purpose |
| --- | --- | --- |
| `stg_customers` | One row per `customer_key` | Customer attributes and market segmentation. |
| `stg_orders` | One row per `order_key` | Order attributes and source order total. |
| `stg_line_items` | One row per `order_key`, `line_number` | Line-item attributes, prices, discounts, tax, and net sales. |
| `fct_orders` | One row per `order_key` | Line-item totals enriched with customer and order attributes. |
| `monthly_sales` | One row per `order_month`, `market_segment` | Monthly gross sales by customer segment. |

The staging models are views by default. `fct_orders` is explicitly materialized
as a table; `monthly_sales` uses the default view materialization.
Model and column descriptions live alongside the SQL in each layer's `schema.yml`.

## Sales definitions and caveats

- **Gross sales** use the source extended line price, which is the whole line's
  amount, not its per-unit price.
- **Discounts** are negative amounts. Discount and tax rates are fractional:
  `0.05` means 5%.
- **Tax** applies to the discounted line amount. **Net sales** equal gross sales
  plus negative discounts plus tax. Calculated line amounts use four decimals.
- `monthly_sales.total_revenue` means **gross sales before discounts and tax**.
  It is not net revenue and does not use the source order's `total_price`.
- Months follow the order date, not the shipment or receipt date. All order
  statuses are included. Missing month/segment combinations are not zero-filled.
- `fct_orders` uses inner joins: orders without a matching customer or any line
  items are excluded. Customer attributes reflect the source record; this
  project does not track customer attribute history.
- Source monetary units are retained; the project does not convert currencies.

## Tests and validation

The project uses built-in dbt tests plus dependency-free singular SQL tests.
Coverage includes:

- Unique, non-null customer and order primary keys.
- Order-to-customer and line-item-to-order relationships.
- Unique composite grains for line items and monthly sales.
- Fact-order primary keys and relationships back to staged orders and customers.
- Non-null monthly order dates and revenue.
- Order-level reconciliation of gross sales, discounts, tax, and net sales,
  with a rounding tolerance of `0.0001` and explicit checks for missing amounts.

Build the reporting dependency slice and run its attached tests in Studio:

```sh
dbt build --select +monthly_sales --defer
```

Validate YAML structure without executing warehouse models:

```sh
dbt parse
```

Tests fail when their SQL returns violating rows. For composite-key failures,
inspect duplicate source records or changed grouping keys. For relationship
failures, inspect missing parent records. For sales reconciliation failures,
inspect the order's staged line items and their rounding conventions. Singular
test descriptions in `tests/schema.yml` provide additional debugging context.

## Documentation

Browse model and column descriptions and lineage in dbt Catalog after the
project is deployed. Keep the layer-level `schema.yml` descriptions in sync
when adding columns or changing business definitions.

References: [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
and [dbt documentation](https://docs.getdbt.com/docs/build/documentation).
