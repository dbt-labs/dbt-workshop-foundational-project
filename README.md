# Foundational Project

A Snowflake dbt project that stages all nine tables in `tpch_now.tpch` and produces order-level and monthly sales reporting.

## Model flow and grain

| Model | Grain | Purpose |
| --- | --- | --- |
| `stg_customers` | One row per customer | Rename customer attributes for joins and segmentation. |
| `stg_orders` | One row per order | Rename source order attributes without filtering. |
| `stg_line_items` | One row per order and line number | Calculate unit prices, negative discounts, tax, and net line sales. |
| `stg_regions` | One row per region | Stage geographic region names and comments. |
| `stg_nations` | One row per nation | Stage nations and their region keys. |
| `stg_parts` | One row per part | Stage catalog attributes and source retail prices. |
| `stg_suppliers` | One row per supplier | Stage supplier attributes and nation keys. |
| `stg_part_suppliers` | One row per part and supplier | Stage available quantities and supply costs. |
| `stg_tbl_test` | One row per ID in observed data | Preserve the auxiliary table; business purpose is unspecified. |
| `fct_orders` | One row per qualifying order | Aggregate line items and join customer attributes. |
| `month_sales` | One row per calendar month and market segment | Sum gross sales for monthly segment reporting. |

The `tpch` source is configured in `models/staging/sources.yml`, pointing to `tpch_now.tpch`. It supplies `customer`, `orders`, `lineitem`, `nation`, `region`, `part`, `supplier`, `partsupp`, and `tbl_test`.

Staging views use `source()` and follow the existing `stg_` naming convention. The six added views retain every source column and row, rename TPCH-prefixed fields, and preserve source data types. They do not filter, deduplicate, aggregate, or join records. Existing reporting calculations are unchanged.

Geographic joins follow customer/supplier → nation → region. Part-supplier offerings join to both parts and suppliers; line items also reference parts and suppliers. Each part-supplier pair is a composite key, so neither key alone is unique. Availability is a source attribute, not a history of inventory movements.

## Reporting definitions

- `fct_orders` uses inner joins: orders without line items or without a matching customer are excluded.
- Gross sales use the source line-item extended price and exclude discounts and tax.
- Discount amounts are negative. Net sales equal gross sales plus the negative discount amount plus tax.
- Unit-price calculations return null when quantity is zero. Calculated monetary values in staging are cast to four decimal places.
- `month_sales.total_revenue` is **gross sales before discounts and tax**, not net sales or the source order total.
- Monthly reporting includes only month/segment combinations present in `fct_orders`; missing periods are not zero-filled.
- Currency and cross-currency conversion are not defined by the model SQL. Confirm source conventions before presenting monetary values with a currency symbol.

## Tests and model documentation

Source descriptions live in `models/staging/sources.yml`. Model and column descriptions live in `models/staging/schema.yml`, `models/staging/tpch_lookups.yml`, and `models/marts/schema.yml`.

Coverage includes:

- Unique, non-null customer, order, region, nation, part, supplier, and auxiliary ID keys.
- Relationships from orders to customers and from line items to orders, parts, and suppliers.
- Relationships from customers and suppliers to nations, and nations to regions.
- Relationships from part-supplier offerings to parts and suppliers.
- Order-fact relationships to staging orders and customers.
- Non-null foreign keys, order dates, and monthly reporting dates where configured.
- Composite-grain checks for `(order_key, line_number)` in line items, `(part_key, supplier_key)` in part-supplier offerings, and `(order_month, market_segment)` in monthly reporting.

A failing relationship test indicates unmatched non-null keys; inspect the child records and source loading completeness. A failing uniqueness test indicates duplicate identifiers or an unexpected grain change. Singular composite-grain tests are in `tests/` and return duplicate groups. Model-specific exceptions should be investigated before relaxing a test.

`tbl_test` is documented conservatively: its identifier is tested for uniqueness and nulls, with no inferred business relationships. No freshness thresholds are assumed for the newly added sources because a load timestamp and delivery SLA have not been established.

## Validation in dbt Platform

Use Studio's command interface to run:

```sh
dbt parse
dbt build --select +path:models/staging+ --defer
```

`parse` validates project structure and YAML. `build` builds the staging dependency slice and downstream reporting models in development and executes their tests; state-aware execution may reuse unchanged models. To rerun only data tests against the relations resolved by your environment:

```sh
dbt test --select path:models/staging
```

Keep model and column descriptions in schema YAML as the project grows. Add key and relationship tests when introducing a new grain or join, and add business-rule tests only when the rule is confirmed.

## Configured project paths

- `models/`: staging and reporting models, source definitions, and schema documentation.
- `tests/`: standalone SQL data tests.
- `analysis/`: ad hoc analyses.
- `macros/`, `seeds/`, `snapshots/`: configured extension paths.

## References

- [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
- [Resource descriptions](https://docs.getdbt.com/reference/resource-properties/description)
