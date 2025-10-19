CREATE OR ALTER PROCEDURE [dbo].[usp_build_fct_orders]
AS
BEGIN
    SET NOCOUNT ON;

    -- This procedure mirrors the dbt staging models (stg_customers, stg_orders, stg_line_items)
    -- and the mart model (fct_orders). It reads from [tpch].[customer], [tpch].[orders],
    -- and [tpch].[lineitem], then materializes the final result into [dbo].[fct_orders].

    DECLARE @now DATETIME = SYSDATETIME();
    PRINT 'usp_build_fct_orders start: ' + CONVERT(VARCHAR(30), @now, 121);

    -- Remove any previous temp/staging artifacts
    IF OBJECT_ID('tempdb..#stg_final') IS NOT NULL DROP TABLE #stg_final;
    IF OBJECT_ID('dbo.fct_orders') IS NOT NULL
    BEGIN
        PRINT 'Dropping existing dbo.fct_orders';
        DROP TABLE [dbo].[fct_orders];
    END

    -- Attempt to clean up rows before recreate (legacy pattern)
    BEGIN TRY
        IF OBJECT_ID('dbo.fct_orders', 'U') = 1
        BEGIN
            PRINT 'Existing dbo.fct_orders detected, will recreate';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Non-fatal: pre-check failed: ' + ERROR_MESSAGE();
    END CATCH

    BEGIN TRANSACTION;
    BEGIN TRY

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

        -- Materialize into temp table first (common SSIS pattern)
        SELECT
            CAST(order_key AS BIGINT)                         AS order_key,
            CAST(order_date AS DATE)                         AS order_date,
            CAST(customer_key AS BIGINT)                     AS customer_key,
            CAST(status_code AS VARCHAR(10))                  AS status_code,
            CAST(priority_code AS VARCHAR(50))                AS priority_code,
            CAST(ship_priority AS INT)                       AS ship_priority,
            CAST(clerk_name AS VARCHAR(200))                  AS clerk_name,
            CAST(name AS VARCHAR(200))                        AS name,
            CAST(market_segment AS VARCHAR(100))              AS market_segment,
            CAST(gross_item_sales_amount AS DECIMAL(16,4))    AS gross_item_sales_amount,
            CAST(item_discount_amount AS DECIMAL(16,4))       AS item_discount_amount,
            CAST(item_tax_amount AS DECIMAL(16,4))            AS item_tax_amount,
            CAST(net_item_sales_amount AS DECIMAL(16,4))      AS net_item_sales_amount
        INTO #stg_final
        FROM final;

    -- validation: capture rows with NULL order_key for inspection, then remove them
    IF OBJECT_ID('dbo.fct_orders_validation_null_keys','U') IS NULL
    BEGIN
        CREATE TABLE dbo.fct_orders_validation_null_keys (
            capture_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            capture_time DATETIME NOT NULL DEFAULT (SYSDATETIME()),
            order_date DATE NULL,
            customer_key BIGINT NULL,
            status_code VARCHAR(10) NULL,
            priority_code VARCHAR(50) NULL,
            ship_priority INT NULL,
            clerk_name VARCHAR(200) NULL,
            name VARCHAR(200) NULL,
            market_segment VARCHAR(100) NULL,
            gross_item_sales_amount DECIMAL(16,4) NULL,
            item_discount_amount DECIMAL(16,4) NULL,
            item_tax_amount DECIMAL(16,4) NULL,
            net_item_sales_amount DECIMAL(16,4) NULL
        );
    END

    INSERT INTO dbo.fct_orders_validation_null_keys (
        order_date, customer_key, status_code, priority_code, ship_priority,
        clerk_name, name, market_segment, gross_item_sales_amount, item_discount_amount,
        item_tax_amount, net_item_sales_amount
    )
    SELECT
        order_date, customer_key, status_code, priority_code, ship_priority,
        clerk_name, name, market_segment, gross_item_sales_amount, item_discount_amount,
        item_tax_amount, net_item_sales_amount
    FROM #stg_final
    WHERE order_key IS NULL;

    IF @@ROWCOUNT > 0 PRINT CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows captured to dbo.fct_orders_validation_null_keys';

    DELETE FROM #stg_final WHERE order_key IS NULL;
    IF @@ROWCOUNT > 0 PRINT 'Removed rows with null order_key from staging';

        -- Explicit CREATE TABLE for final target with schema/constraints
        CREATE TABLE [dbo].[fct_orders] (
            order_key BIGINT NOT NULL,
            order_date DATE NULL,
            customer_key BIGINT NULL,
            status_code VARCHAR(10) NULL,
            priority_code VARCHAR(50) NULL,
            ship_priority INT NULL,
            clerk_name VARCHAR(200) NULL,
            name VARCHAR(200) NULL,
            market_segment VARCHAR(100) NULL,
            gross_item_sales_amount DECIMAL(16,4) NULL,
            item_discount_amount DECIMAL(16,4) NULL,
            item_tax_amount DECIMAL(16,4) NULL,
            net_item_sales_amount DECIMAL(16,4) NULL
        );

    -- Safety check before insert (pattern from legacy loads)
    PRINT 'Preparing to insert into dbo.fct_orders';

        -- Insert from staging
        INSERT INTO dbo.fct_orders (
            order_key, order_date, customer_key, status_code, priority_code, ship_priority,
            clerk_name, name, market_segment, gross_item_sales_amount, item_discount_amount,
            item_tax_amount, net_item_sales_amount
        )
        SELECT
            order_key, order_date, customer_key, status_code, priority_code, ship_priority,
            clerk_name, name, market_segment, gross_item_sales_amount, item_discount_amount,
            item_tax_amount, net_item_sales_amount
        FROM #stg_final;

        -- Create indexes (one clustered, one nonclustered) to mimic SSIS post-load steps
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'PK_fct_orders_order_key' AND object_id = OBJECT_ID('dbo.fct_orders'))
        BEGIN
            ALTER TABLE dbo.fct_orders ADD CONSTRAINT PK_fct_orders_order_key PRIMARY KEY CLUSTERED (order_key);
        END

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fct_orders_order_date' AND object_id = OBJECT_ID('dbo.fct_orders'))
        BEGIN
            CREATE NONCLUSTERED INDEX IX_fct_orders_order_date ON dbo.fct_orders(order_date);
        END

        -- Attempt to grant read permissions to reporting users (may be skipped if caller lacks rights)
        BEGIN TRY
            GRANT SELECT ON dbo.fct_orders TO PUBLIC;
        END TRY
        BEGIN CATCH
            PRINT 'Permission grant skipped: ' + ERROR_MESSAGE();
        END CATCH

        DROP TABLE IF EXISTS #stg_final; -- cleanup

        COMMIT TRANSACTION;

        DECLARE @end DATETIME = SYSDATETIME();
        PRINT 'usp_build_fct_orders completed: ' + CONVERT(VARCHAR(30), @end, 121) + ' duration(s): ' +
              CONVERT(VARCHAR(10), DATEDIFF(SECOND, @now, @end));

    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @sev INT = ERROR_SEVERITY();
        DECLARE @state INT = ERROR_STATE();
        PRINT 'ERROR: ' + @err;
        ROLLBACK TRANSACTION;
    -- cleanup temp staging on error
    IF OBJECT_ID('tempdb..#stg_final') IS NOT NULL DROP TABLE #stg_final;
        THROW; -- re-raise
    END CATCH
END
GO


