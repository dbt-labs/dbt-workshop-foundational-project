CREATE OR ALTER PROCEDURE [dbo].[usp_build_fct_orders]
AS
BEGIN
    SET NOCOUNT ON;

    -- This procedure mirrors the dbt staging models (stg_customers, stg_orders, stg_line_items)
    -- and the mart model (fct_orders). It reads from [tpch].[customer], [tpch].[orders],
    -- and [tpch].[lineitem], then materializes the final result into [dbo].[fct_orders].

    DROP TABLE IF EXISTS [dbo].[fct_orders];

    ;WITH stg_customers AS (
        SELECT
            c_custkey   AS customer_key,
            c_name      AS name,
            c_address   AS address,
            c_nationkey AS nation_key,
            c_phone     AS phone_number,
            c_acctbal   AS account_balance,
            c_mktsegment AS market_segment,
            c_comment   AS comment
        FROM [tpch].[customer]
    ),
    stg_orders AS (
        SELECT
            o_orderkey     AS order_key,
            o_custkey      AS customer_key,
            o_orderstatus  AS status_code,
            o_totalprice   AS total_price,
            o_orderdate    AS order_date,
            o_clerk        AS clerk_name,
            o_orderpriority AS priority_code,
            o_shippriority AS ship_priority,
            o_comment      AS comment
        FROM [tpch].[orders]
    ),
    stg_line_items AS (
        SELECT
            l_orderkey AS order_key,
            l_partkey  AS part_key,
            l_suppkey  AS supplier_key,
            l_linenumber AS line_number,
            l_quantity AS quantity,
            l_extendedprice AS gross_item_sales_amount,
            l_discount AS discount_percentage,
            l_tax      AS tax_rate,
            l_returnflag AS return_flag,
            l_linestatus AS status_code,
            l_shipdate AS ship_date,
            l_commitdate AS commit_date,
            l_receiptdate AS receipt_date,
            l_shipinstruct AS ship_instructions,
            l_shipmode AS ship_mode,
            l_comment AS comment,

            CAST(l_extendedprice / NULLIF(l_quantity, 0) AS decimal(16,4)) AS base_price,
            CAST((l_extendedprice / NULLIF(l_quantity, 0)) * (1 - l_discount) AS decimal(16,4)) AS discounted_price,
            CAST(l_extendedprice * (1 - l_discount) AS decimal(16,4)) AS discounted_item_sales_amount,
            CAST(-1 * l_extendedprice * l_discount AS decimal(16,4)) AS item_discount_amount,
            CAST((l_extendedprice + (-1 * l_extendedprice * l_discount)) * l_tax AS decimal(16,4)) AS item_tax_amount,
            CAST(l_extendedprice + (-1 * l_extendedprice * l_discount) + ((l_extendedprice + (-1 * l_extendedprice * l_discount)) * l_tax) AS decimal(16,4)) AS net_item_sales_amount
        FROM [tpch].[lineitem]
    ),
    order_item_summary AS (
        SELECT
            order_key,
            SUM(gross_item_sales_amount) AS gross_item_sales_amount,
            SUM(item_discount_amount)    AS item_discount_amount,
            SUM(item_tax_amount)         AS item_tax_amount,
            SUM(net_item_sales_amount)   AS net_item_sales_amount
        FROM stg_line_items
        GROUP BY order_key
    ),
    final AS (
        SELECT
            o.order_key,
            o.order_date,
            o.customer_key,
            o.status_code,
            o.priority_code,
            o.ship_priority,
            o.clerk_name,
            c.name,
            c.market_segment,
            s.gross_item_sales_amount,
            s.item_discount_amount,
            s.item_tax_amount,
            s.net_item_sales_amount
        FROM stg_orders AS o
        INNER JOIN order_item_summary AS s
            ON o.order_key = s.order_key
        INNER JOIN stg_customers AS c
            ON o.customer_key = c.customer_key
    )
    SELECT
        order_key,
        order_date,
        customer_key,
        status_code,
        priority_code,
        ship_priority,
        clerk_name,
        name,
        market_segment,
        gross_item_sales_amount,
        item_discount_amount,
        item_tax_amount,
        net_item_sales_amount
    INTO [dbo].[fct_orders]
    FROM final
    ORDER BY order_date;
END
GO


