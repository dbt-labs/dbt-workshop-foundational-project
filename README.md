# Foundational Project

A dbt v2 Stable project on Snowflake that stages TPCH customers, orders, and
line items, then builds an order-grain revenue fact table.

## Models and grain

| Model | Grain | Purpose |
| --- | --- | --- |
| `stg_customers` | One row per `customer_key` | Renamed customer attributes, including market segment. |
| `stg_orders` | One row per `order_key` | Renamed order headers and source header totals. |
| `stg_line_items` | One row per (`order_key`, `line_number`) | Renamed order lines with derived prices, discounts, taxes, and net sales. |
| `fct_orders` | One row per matched `order_key` | Line-item amounts aggregated by order and enriched with order and customer attributes. |

Sources are defined in `models/staging/sources.yml` and point to the `tpch`
schema in the `tpch_now` database. Staging models use the project's default view
materialization; `fct_orders` is explicitly materialized as a table.

## Revenue definitions

- **Gross sales:** the extended line amount before discounts and tax, not a unit price.
- **Discount amount:** negative gross sales multiplied by the source discount rate.
- **Tax amount:** (gross sales + discount amount) multiplied by the source tax rate.
- **Net sales:** gross sales + discount amount + tax amount.

Calculated line-level amounts and prices are cast to `decimal(16,4)`. The fact
table sums line amounts; it does not use the source order-header `total_price`
as its revenue measure. Source monetary units are preserved without currency
conversion. `base_price` is null when quantity is zero or null.

`fct_orders` uses inner joins: orders without line items or a matching customer
are excluded. Customer attributes reflect records used at build time, not
historical snapshots. No status or return-flag filters are applied.

## Tests and documentation

Model and column descriptions live beside the SQL in:

- `models/staging/schema.yml`
- `models/marts/schema.yml`

The test suite checks primary-key uniqueness and completeness, order/customer
relationships, the composite line-item grain, required order dates, non-null
fact revenue fields, and reconciliation of net sales to gross sales plus
discounts plus tax. The reconciliation allows a `0.0001` rounding tolerance.
Singular SQL tests and their descriptions live in `tests/`.

Run commands from the dbt Studio command bar:

```sh
dbt parse
dbt build --select +fct_orders+ --no-defer
dbt test --select +fct_orders+ --no-defer
```

The build command builds the complete order-fact dependency slice and runs its
tests. The test command checks already-built relations. `--no-defer` keeps
validation on development relations. Descriptions are available in dbt's model
and column documentation after the project is parsed and deployed.

Keep analytical queries that read `fct_orders` in the configured `analysis/`
directory; the fact model itself must never reference `fct_orders`.

## References

- [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
- [dbt documentation](https://docs.getdbt.com/docs/build/documentation)
