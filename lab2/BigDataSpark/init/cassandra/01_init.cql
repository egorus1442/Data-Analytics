CREATE KEYSPACE IF NOT EXISTS sparkdb
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE sparkdb;

CREATE TABLE IF NOT EXISTS report_products_sales (
    product_id             int PRIMARY KEY,
    product_name           text,
    category               text,
    total_quantity_sold    bigint,
    total_revenue          double,
    avg_rating             double,
    total_reviews          bigint,
    category_total_revenue double,
    sales_rank             int
);

CREATE TABLE IF NOT EXISTS report_customers_sales (
    customer_id        int PRIMARY KEY,
    first_name         text,
    last_name          text,
    country            text,
    total_spent        double,
    order_count        bigint,
    avg_order_value    double,
    customers_in_country bigint,
    customer_rank      int
);

CREATE TABLE IF NOT EXISTS report_time_sales (
    year            int,
    month           int,
    total_revenue   double,
    total_orders    bigint,
    avg_order_value double,
    PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS report_stores_sales (
    store_id        int PRIMARY KEY,
    store_name      text,
    store_city      text,
    store_country   text,
    total_revenue   double,
    total_orders    bigint,
    avg_order_value double,
    store_rank      int
);

CREATE TABLE IF NOT EXISTS report_suppliers_sales (
    supplier_id       int PRIMARY KEY,
    supplier_name     text,
    supplier_country  text,
    total_revenue     double,
    avg_product_price double,
    total_orders      bigint,
    supplier_rank     int
);

CREATE TABLE IF NOT EXISTS report_product_quality (
    product_id          int PRIMARY KEY,
    product_name        text,
    category            text,
    rating              double,
    review_count        bigint,
    total_quantity_sold bigint,
    total_revenue       double,
    quality_rank        int
);
