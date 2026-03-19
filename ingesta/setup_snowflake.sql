-- =============================================================================
-- SNEAKER POINT RETAIL ANALYTICS — Setup Snowflake (capa RAW / Landing)
-- =============================================================================
-- Ejecutar en orden:
--   1. Sección 1: crear database, schemas y warehouse (si no existen)
--   2. Sección 2: crear stage interno
--   3. Sección 3: subir CSVs al stage desde la UI de Snowflake o con snowsql:
--                 PUT file://C:/ruta/ingesta/data/brands.csv @RAW_STAGE/brands/
--   4. Sección 4: crear todas las tablas RAW
--   5. Sección 5: COPY INTO para cargar datos desde el stage
-- =============================================================================


-- =============================================================================
-- SECCIÓN 1: Database, Schemas y Warehouse
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS PROYECTO_2T;
USE DATABASE PROYECTO_2T;

CREATE SCHEMA IF NOT EXISTS RAW          COMMENT = 'Capa Landing: datos brutos ingeridos desde PostgreSQL via CSV';
CREATE SCHEMA IF NOT EXISTS SILVER       COMMENT = 'Capa Silver: datos limpios y estandarizados (modelos stg_*)';
CREATE SCHEMA IF NOT EXISTS GOLD         COMMENT = 'Capa Gold: dimensiones, hechos y agregados orientados a negocio';
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS    COMMENT = 'Snapshots SCD Type 2 de entidades con historico relevante';

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse para el proyecto Sneaker Point';

USE WAREHOUSE COMPUTE_WH;
USE SCHEMA RAW;


-- =============================================================================
-- SECCIÓN 2: Stage interno para carga de CSVs
-- =============================================================================

CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null', 'None')
    TRIM_SPACE = TRUE
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE RAW.RAW_STAGE
    FILE_FORMAT = RAW.CSV_FORMAT
    COMMENT = 'Stage interno para carga de CSVs extraidos desde PostgreSQL';


-- =============================================================================
-- SECCIÓN 3: Subir archivos (ejecutar en snowsql o desde la UI)
-- =============================================================================
-- Desde snowsql:
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/brands.csv              @RAW.RAW_STAGE/brands/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/categories.csv          @RAW.RAW_STAGE/categories/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/stores.csv              @RAW.RAW_STAGE/stores/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/store_business_hours.csv @RAW.RAW_STAGE/store_business_hours/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/employees.csv           @RAW.RAW_STAGE/employees/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/suppliers.csv           @RAW.RAW_STAGE/suppliers/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/products.csv            @RAW.RAW_STAGE/products/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/product_variants.csv    @RAW.RAW_STAGE/product_variants/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/customers.csv           @RAW.RAW_STAGE/customers/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/discounts.csv           @RAW.RAW_STAGE/discounts/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/discount_categories.csv @RAW.RAW_STAGE/discount_categories/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/discount_customers.csv  @RAW.RAW_STAGE/discount_customers/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/discount_products.csv   @RAW.RAW_STAGE/discount_products/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/discount_stores.csv     @RAW.RAW_STAGE/discount_stores/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/inventory_stock.csv     @RAW.RAW_STAGE/inventory_stock/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/sales.csv               @RAW.RAW_STAGE/sales/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/sale_items.csv          @RAW.RAW_STAGE/sale_items/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/sale_item_discounts.csv @RAW.RAW_STAGE/sale_item_discounts/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/payments.csv            @RAW.RAW_STAGE/payments/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/returns.csv             @RAW.RAW_STAGE/returns/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/return_items.csv        @RAW.RAW_STAGE/return_items/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/refunds.csv             @RAW.RAW_STAGE/refunds/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/stock_movements.csv     @RAW.RAW_STAGE/stock_movements/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/purchase_orders.csv     @RAW.RAW_STAGE/purchase_orders/ AUTO_COMPRESS=TRUE;
--   PUT file://C:/ruta/proyecto_2T_dbt/ingesta/data/purchase_order_items.csv @RAW.RAW_STAGE/purchase_order_items/ AUTO_COMPRESS=TRUE;


-- =============================================================================
-- SECCIÓN 4: CREATE TABLE (capa RAW)
-- =============================================================================

USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW.BRANDS (
    brand_id        NUMBER(38,0)    NOT NULL,
    brand_name      VARCHAR(255)    NOT NULL,
    country_code    CHAR(2),
    created_at      TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.CATEGORIES (
    category_id         NUMBER(38,0)  NOT NULL,
    parent_category_id  NUMBER(38,0),
    category_name       VARCHAR(255)  NOT NULL,
    category_code       VARCHAR(50)   NOT NULL,
    created_at          TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.STORES (
    store_id        NUMBER(38,0)    NOT NULL,
    store_code      VARCHAR(50)     NOT NULL,
    store_name      VARCHAR(255)    NOT NULL,
    address_line_1  VARCHAR(500)    NOT NULL,
    address_line_2  VARCHAR(500),
    city            VARCHAR(100)    NOT NULL,
    province        VARCHAR(100)    NOT NULL,
    postal_code     VARCHAR(20)     NOT NULL,
    country_code    CHAR(2)         NOT NULL,
    phone           VARCHAR(50),
    email           VARCHAR(255),
    opened_on       DATE            NOT NULL,
    closed_on       DATE,
    timezone_name   VARCHAR(100)    NOT NULL,
    status          VARCHAR(50)     NOT NULL,
    created_at      TIMESTAMP_TZ,
    updated_at      TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.STORE_BUSINESS_HOURS (
    store_id     NUMBER(38,0)  NOT NULL,
    weekday_iso  NUMBER(2,0)   NOT NULL,
    is_open      BOOLEAN       NOT NULL,
    open_time    TIME,
    close_time   TIME
);

CREATE OR REPLACE TABLE RAW.EMPLOYEES (
    employee_id       NUMBER(38,0)  NOT NULL,
    employee_code     VARCHAR(50)   NOT NULL,
    store_id          NUMBER(38,0)  NOT NULL,
    first_name        VARCHAR(100)  NOT NULL,
    last_name         VARCHAR(100)  NOT NULL,
    email             VARCHAR(255),
    phone             VARCHAR(50),
    role              VARCHAR(50)   NOT NULL,
    hire_date         DATE          NOT NULL,
    termination_date  DATE,
    is_active         BOOLEAN       NOT NULL,
    created_at        TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.SUPPLIERS (
    supplier_id    NUMBER(38,0)  NOT NULL,
    supplier_code  VARCHAR(50)   NOT NULL,
    supplier_name  VARCHAR(255)  NOT NULL,
    contact_name   VARCHAR(255),
    phone          VARCHAR(50),
    email          VARCHAR(255),
    country_code   CHAR(2)       NOT NULL,
    is_active      BOOLEAN       NOT NULL,
    created_at     TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.PRODUCTS (
    product_id     NUMBER(38,0)  NOT NULL,
    product_code   VARCHAR(50)   NOT NULL,
    brand_id       NUMBER(38,0)  NOT NULL,
    category_id    NUMBER(38,0)  NOT NULL,
    product_name   VARCHAR(255)  NOT NULL,
    model_name     VARCHAR(255)  NOT NULL,
    description    VARCHAR(2000),
    target_gender  VARCHAR(20),
    sport_type     VARCHAR(100),
    material       VARCHAR(255),
    launch_date    DATE,
    is_active      BOOLEAN       NOT NULL,
    created_at     TIMESTAMP_TZ,
    updated_at     TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.PRODUCT_VARIANTS (
    product_variant_id  NUMBER(38,0)    NOT NULL,
    product_id          NUMBER(38,0)    NOT NULL,
    variant_sku         VARCHAR(100)    NOT NULL,
    barcode             VARCHAR(100),
    color               VARCHAR(100)    NOT NULL,
    size_eu             NUMBER(4,1)     NOT NULL,
    unit_cost           NUMBER(12,2)    NOT NULL,
    list_price          NUMBER(12,2)    NOT NULL,
    is_active           BOOLEAN         NOT NULL,
    created_at          TIMESTAMP_TZ,
    updated_at          TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.CUSTOMERS (
    customer_id      NUMBER(38,0)  NOT NULL,
    customer_code    VARCHAR(50)   NOT NULL,
    first_name       VARCHAR(100)  NOT NULL,
    last_name        VARCHAR(100)  NOT NULL,
    email            VARCHAR(255),
    phone            VARCHAR(50),
    birth_date       DATE,
    gender           VARCHAR(20),
    city             VARCHAR(100),
    province         VARCHAR(100),
    country_code     CHAR(2)       NOT NULL,
    signup_store_id  NUMBER(38,0),
    signup_ts        TIMESTAMP_TZ  NOT NULL,
    status           VARCHAR(50)   NOT NULL,
    marketing_opt_in BOOLEAN       NOT NULL,
    loyalty_points   NUMBER(10,0)  NOT NULL,
    created_at       TIMESTAMP_TZ,
    updated_at       TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.DISCOUNTS (
    discount_id          NUMBER(38,0)  NOT NULL,
    discount_code        VARCHAR(50),
    discount_name        VARCHAR(255)  NOT NULL,
    discount_type        VARCHAR(50)   NOT NULL,
    scope                VARCHAR(50)   NOT NULL,
    value_amount         NUMBER(12,2)  NOT NULL,
    min_ticket_amount    NUMBER(12,2),
    max_discount_amount  NUMBER(12,2),
    start_ts             TIMESTAMP_TZ  NOT NULL,
    end_ts               TIMESTAMP_TZ  NOT NULL,
    is_stackable         BOOLEAN       NOT NULL,
    is_active            BOOLEAN       NOT NULL,
    created_at           TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.DISCOUNT_CATEGORIES (
    discount_id  NUMBER(38,0)  NOT NULL,
    category_id  NUMBER(38,0)  NOT NULL
);

CREATE OR REPLACE TABLE RAW.DISCOUNT_CUSTOMERS (
    discount_id  NUMBER(38,0)  NOT NULL,
    customer_id  NUMBER(38,0)  NOT NULL
);

CREATE OR REPLACE TABLE RAW.DISCOUNT_PRODUCTS (
    discount_id  NUMBER(38,0)  NOT NULL,
    product_id   NUMBER(38,0)  NOT NULL
);

CREATE OR REPLACE TABLE RAW.DISCOUNT_STORES (
    discount_id  NUMBER(38,0)  NOT NULL,
    store_id     NUMBER(38,0)  NOT NULL
);

CREATE OR REPLACE TABLE RAW.INVENTORY_STOCK (
    store_id            NUMBER(38,0)  NOT NULL,
    product_variant_id  NUMBER(38,0)  NOT NULL,
    on_hand_qty         NUMBER(10,0)  NOT NULL,
    reserved_qty        NUMBER(10,0)  NOT NULL,
    reorder_point       NUMBER(10,0)  NOT NULL,
    reorder_qty         NUMBER(10,0)  NOT NULL,
    last_counted_at     TIMESTAMP_TZ,
    updated_at          TIMESTAMP_TZ  NOT NULL
);

CREATE OR REPLACE TABLE RAW.SALES (
    sale_id          NUMBER(38,0)  NOT NULL,
    sale_number      VARCHAR(50)   NOT NULL,
    store_id         NUMBER(38,0)  NOT NULL,
    customer_id      NUMBER(38,0),
    employee_id      NUMBER(38,0),
    sale_ts          TIMESTAMP_TZ  NOT NULL,
    status           VARCHAR(50)   NOT NULL,
    subtotal_amount  NUMBER(12,2)  NOT NULL,
    discount_amount  NUMBER(12,2)  NOT NULL,
    total_amount     NUMBER(12,2)  NOT NULL,
    items_count      NUMBER(10,0)  NOT NULL,
    notes            VARCHAR(1000),
    created_at       TIMESTAMP_TZ,
    updated_at       TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.SALE_ITEMS (
    sale_item_id          NUMBER(38,0)  NOT NULL,
    sale_id               NUMBER(38,0)  NOT NULL,
    product_variant_id    NUMBER(38,0)  NOT NULL,
    quantity              NUMBER(10,0)  NOT NULL,
    unit_list_price       NUMBER(12,2)  NOT NULL,
    unit_discount_amount  NUMBER(12,2)  NOT NULL,
    unit_final_price      NUMBER(12,2)  NOT NULL,
    line_subtotal         NUMBER(12,2)  NOT NULL,
    line_discount_total   NUMBER(12,2)  NOT NULL,
    line_total            NUMBER(12,2)  NOT NULL,
    created_at            TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.SALE_ITEM_DISCOUNTS (
    sale_item_discount_id  NUMBER(38,0)  NOT NULL,
    sale_item_id           NUMBER(38,0)  NOT NULL,
    discount_id            NUMBER(38,0)  NOT NULL,
    discount_amount        NUMBER(12,2)  NOT NULL,
    created_at             TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.PAYMENTS (
    payment_id          NUMBER(38,0)  NOT NULL,
    sale_id             NUMBER(38,0)  NOT NULL,
    payment_ts          TIMESTAMP_TZ  NOT NULL,
    payment_method      VARCHAR(50)   NOT NULL,
    status              VARCHAR(50)   NOT NULL,
    amount              NUMBER(12,2)  NOT NULL,
    external_reference  VARCHAR(255),
    created_at          TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.RETURNS (
    return_id                  NUMBER(38,0)  NOT NULL,
    return_number              VARCHAR(50)   NOT NULL,
    sale_id                    NUMBER(38,0)  NOT NULL,
    store_id                   NUMBER(38,0)  NOT NULL,
    customer_id                NUMBER(38,0),
    processed_by_employee_id   NUMBER(38,0),
    return_ts                  TIMESTAMP_TZ  NOT NULL,
    status                     VARCHAR(50)   NOT NULL,
    reason                     VARCHAR(500),
    refund_amount              NUMBER(12,2)  NOT NULL,
    created_at                 TIMESTAMP_TZ,
    updated_at                 TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.RETURN_ITEMS (
    return_item_id      NUMBER(38,0)  NOT NULL,
    return_id           NUMBER(38,0)  NOT NULL,
    sale_item_id        NUMBER(38,0)  NOT NULL,
    quantity            NUMBER(10,0)  NOT NULL,
    unit_refund_amount  NUMBER(12,2)  NOT NULL,
    line_refund_amount  NUMBER(12,2)  NOT NULL,
    return_reason       VARCHAR(500),
    condition_notes     VARCHAR(500),
    created_at          TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.REFUNDS (
    refund_id           NUMBER(38,0)  NOT NULL,
    return_id           NUMBER(38,0)  NOT NULL,
    refund_ts           TIMESTAMP_TZ  NOT NULL,
    refund_method       VARCHAR(50)   NOT NULL,
    status              VARCHAR(50)   NOT NULL,
    amount              NUMBER(12,2)  NOT NULL,
    external_reference  VARCHAR(255),
    created_at          TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.STOCK_MOVEMENTS (
    stock_movement_id       NUMBER(38,0)  NOT NULL,
    store_id                NUMBER(38,0)  NOT NULL,
    product_variant_id      NUMBER(38,0)  NOT NULL,
    movement_ts             TIMESTAMP_TZ  NOT NULL,
    movement_type           VARCHAR(50)   NOT NULL,
    quantity                NUMBER(10,0)  NOT NULL,
    unit_cost               NUMBER(12,2),
    sale_item_id            NUMBER(38,0),
    return_item_id          NUMBER(38,0),
    purchase_order_item_id  NUMBER(38,0),
    notes                   VARCHAR(500),
    created_at              TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.PURCHASE_ORDERS (
    purchase_order_id       NUMBER(38,0)  NOT NULL,
    po_number               VARCHAR(50)   NOT NULL,
    supplier_id             NUMBER(38,0)  NOT NULL,
    destination_store_id    NUMBER(38,0)  NOT NULL,
    order_ts                TIMESTAMP_TZ  NOT NULL,
    expected_delivery_date  DATE,
    received_ts             TIMESTAMP_TZ,
    status                  VARCHAR(50)   NOT NULL,
    notes                   VARCHAR(1000),
    created_at              TIMESTAMP_TZ,
    updated_at              TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.PURCHASE_ORDER_ITEMS (
    purchase_order_item_id  NUMBER(38,0)  NOT NULL,
    purchase_order_id       NUMBER(38,0)  NOT NULL,
    product_variant_id      NUMBER(38,0)  NOT NULL,
    quantity_ordered        NUMBER(10,0)  NOT NULL,
    quantity_received       NUMBER(10,0)  NOT NULL,
    unit_cost               NUMBER(12,2)  NOT NULL,
    created_at              TIMESTAMP_TZ
);


-- =============================================================================
-- SECCIÓN 5: COPY INTO desde el stage (CORREGIDO)
-- =============================================================================

COPY INTO RAW.BRANDS                FROM @RAW.RAW_STAGE/brands.csv                FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.CATEGORIES            FROM @RAW.RAW_STAGE/categories.csv            FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.STORES                FROM @RAW.RAW_STAGE/stores.csv                FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.STORE_BUSINESS_HOURS  FROM @RAW.RAW_STAGE/store_business_hours.csv  FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.EMPLOYEES             FROM @RAW.RAW_STAGE/employees.csv             FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.SUPPLIERS             FROM @RAW.RAW_STAGE/suppliers.csv             FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.PRODUCTS              FROM @RAW.RAW_STAGE/products.csv              FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.PRODUCT_VARIANTS      FROM @RAW.RAW_STAGE/product_variants.csv      FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.CUSTOMERS             FROM @RAW.RAW_STAGE/customers.csv             FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.DISCOUNTS             FROM @RAW.RAW_STAGE/discounts.csv             FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.DISCOUNT_CATEGORIES   FROM @RAW.RAW_STAGE/discount_categories.csv   FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.DISCOUNT_CUSTOMERS    FROM @RAW.RAW_STAGE/discount_customers.csv    FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.DISCOUNT_PRODUCTS     FROM @RAW.RAW_STAGE/discount_products.csv     FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.DISCOUNT_STORES       FROM @RAW.RAW_STAGE/discount_stores.csv       FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.INVENTORY_STOCK       FROM @RAW.RAW_STAGE/inventory_stock.csv       FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.SALES                 FROM @RAW.RAW_STAGE/sales.csv                 FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.SALE_ITEMS            FROM @RAW.RAW_STAGE/sale_items.csv            FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.SALE_ITEM_DISCOUNTS   FROM @RAW.RAW_STAGE/sale_item_discounts.csv   FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.PAYMENTS              FROM @RAW.RAW_STAGE/payments.csv              FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.RETURNS               FROM @RAW.RAW_STAGE/returns.csv               FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.RETURN_ITEMS          FROM @RAW.RAW_STAGE/return_items.csv          FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.REFUNDS               FROM @RAW.RAW_STAGE/refunds.csv               FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.STOCK_MOVEMENTS       FROM @RAW.RAW_STAGE/stock_movements.csv       FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.PURCHASE_ORDERS       FROM @RAW.RAW_STAGE/purchase_orders.csv       FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RAW.PURCHASE_ORDER_ITEMS  FROM @RAW.RAW_STAGE/purchase_order_items.csv  FILE_FORMAT = RAW.CSV_FORMAT ON_ERROR = 'CONTINUE';


-- Verificar carga
SELECT 'BRANDS'              AS tabla, COUNT(*) AS filas FROM RAW.BRANDS              UNION ALL
SELECT 'CATEGORIES'          ,         COUNT(*)          FROM RAW.CATEGORIES          UNION ALL
SELECT 'STORES'              ,         COUNT(*)          FROM RAW.STORES              UNION ALL
SELECT 'EMPLOYEES'           ,         COUNT(*)          FROM RAW.EMPLOYEES           UNION ALL
SELECT 'CUSTOMERS'           ,         COUNT(*)          FROM RAW.CUSTOMERS           UNION ALL
SELECT 'PRODUCTS'            ,         COUNT(*)          FROM RAW.PRODUCTS            UNION ALL
SELECT 'PRODUCT_VARIANTS'    ,         COUNT(*)          FROM RAW.PRODUCT_VARIANTS    UNION ALL
SELECT 'SALES'               ,         COUNT(*)          FROM RAW.SALES               UNION ALL
SELECT 'SALE_ITEMS'          ,         COUNT(*)          FROM RAW.SALE_ITEMS          UNION ALL
SELECT 'PAYMENTS'            ,         COUNT(*)          FROM RAW.PAYMENTS            UNION ALL
SELECT 'RETURNS'             ,         COUNT(*)          FROM RAW.RETURNS             UNION ALL
SELECT 'STOCK_MOVEMENTS'     ,         COUNT(*)          FROM RAW.STOCK_MOVEMENTS
ORDER BY tabla;
