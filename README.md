# Foundational Project

A dbt project on Snowflake that stages all eight core TPCH tables, then
produces order-level facts and monthly gross sales by customer segment.

## Model guide

| Model | Grain | Purpose |
| --- | --- | --- |
| `stg_customers` | One row per `customer_key` | Rename customer attributes for analytics. |
| `stg_orders` | One row per `order_key` | Rename order attributes without filtering statuses. |
| `stg_line_items` | One row per `order_key`, `line_number` | Calculate line-level prices, discounts, tax, and sales. |
| `stg_parts` | One row per `part_key` | Stage the complete part catalog and source retail prices. |
| `stg_suppliers` | One row per `supplier_key` | Stage supplier contact details, nation, and account balance. |
| `stg_part_suppliers` | One row per `part_key`, `supplier_key` | Stage supplier-specific part availability and supply cost. |
| `stg_nations` | One row per `nation_key` | Relate customer and supplier nations to regions. |
| `stg_regions` | One row per `region_key` | Stage the parent geographic lookup for nations. |
| `fct_orders` | One row per `order_key` | Aggregate line items and enrich orders with customer attributes. |
| `monthly_sales` | One row per `order_month`, `market_segment` | Sum gross sales by calendar order month and segment. |

The `tpch` source is configured in `models/staging/sources.yml`. Model and
column definitions, including monetary formulas and join exclusions, live in
`models/staging/schema.yml`, `models/staging/tpch_lookups.yml`, and
`models/marts/schema.yml`. The five catalog, supply, and geography models
preserve all their source columns, rename fields, and apply no filters or joins.
They inherit the project's view materialization. Supply availability is not a
dated inventory history, and retail prices and supply costs are distinct from
realized selling prices.

## Sales definitions and caveats

- `monthly_sales.total_revenue` is **gross sales before discounts and tax**,
  summed from line-item extended prices. It is not net revenue.
- Discounts are negative monetary amounts. Net line sales equal gross sales
  plus discount amounts plus tax; tax is calculated after discounts.
- `fct_orders` uses inner joins, so an order must have both line items and a
  matching customer to appear in the marts.
- No status or return-flag filter is applied. Monthly sales use the order date,
  not shipment or receipt dates, and missing months are not zero-filled.
- Monetary fields retain source units; the project does not perform currency
  conversion. Calculated line-level amounts use `decimal(16,4)`.

## Build and validation

Run these commands from the dbt Studio command bar or a configured dbt CLI:

```bash
dbt parse
dbt build
```

`dbt build` builds the ten models and runs their data tests. To rerun tests
against already-built relations:

```bash
dbt test
```

Coverage includes primary-key uniqueness and completeness; order/customer,
line-item/order, line-item/part, line-item/supplier, part-supplier/part,
part-supplier/supplier, customer/nation, supplier/nation, and nation/region
relationships; required reporting grain fields; and composite uniqueness for
order lines, part/supplier pairs, and monthly sales. Composite-grain tests are
in `tests/`; generic column tests are alongside model documentation in YAML.
Pass-through key tests live on staging models instead of being duplicated on sources.

If a relationship test fails, inspect unmatched keys in the child model and
its parent. If a grain test fails, investigate the duplicate keys before
trusting downstream aggregates. Tests use error severity and will fail the
build when their assertions are violated.

When extending the project, document each model's grain, filters, join
exclusions, and calculated fields in its schema YAML. Add tests for new keys
or relationships and confirm allowed enum values before constraining them.

## dbt documentation

- [Data tests](https://docs.getdbt.com/docs/build/data-tests)
- [Project documentation](https://docs.getdbt.com/docs/build/documentation)
