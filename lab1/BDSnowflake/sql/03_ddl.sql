CREATE TABLE dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_city (
    city_id    SERIAL PRIMARY KEY,
    city_name  VARCHAR(100) NOT NULL,
    state      VARCHAR(100),
    country_id INTEGER NOT NULL REFERENCES dim_country(country_id)
);

CREATE TABLE dim_pet_category (
    pet_category_id SERIAL PRIMARY KEY,
    category_name   VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_pet (
    pet_id          SERIAL PRIMARY KEY,
    pet_name        VARCHAR(100),
    pet_type        VARCHAR(50),
    pet_breed       VARCHAR(100),
    pet_category_id INTEGER REFERENCES dim_pet_category(pet_category_id)
);

CREATE TABLE dim_product_category (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_brand (
    brand_id   SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    contact     VARCHAR(200),
    email       VARCHAR(255),
    phone       VARCHAR(50),
    address     VARCHAR(200),
    city_id     INTEGER REFERENCES dim_city(city_id)
);

CREATE TABLE dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   DATE        NOT NULL UNIQUE,
    day         SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    year        SMALLINT    NOT NULL,
    quarter     SMALLINT    NOT NULL,
    day_of_week SMALLINT    NOT NULL
);

CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    age         SMALLINT,
    email       VARCHAR(255),
    postal_code VARCHAR(20),
    country_id  INTEGER REFERENCES dim_country(country_id),
    pet_id      INTEGER REFERENCES dim_pet(pet_id)
);

CREATE TABLE dim_seller (
    seller_id   INTEGER PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(255),
    postal_code VARCHAR(20),
    country_id  INTEGER REFERENCES dim_country(country_id)
);

CREATE TABLE dim_product (
    product_id     INTEGER PRIMARY KEY,
    name           VARCHAR(200),
    price          NUMERIC(10, 2),
    stock_quantity INTEGER,
    weight         NUMERIC(10, 2),
    color          VARCHAR(50),
    size           VARCHAR(50),
    material       VARCHAR(100),
    description    TEXT,
    rating         NUMERIC(3, 1),
    reviews        INTEGER,
    release_date   DATE,
    expiry_date    DATE,
    category_id    INTEGER REFERENCES dim_product_category(category_id),
    brand_id       INTEGER REFERENCES dim_brand(brand_id),
    supplier_id    INTEGER REFERENCES dim_supplier(supplier_id)
);

CREATE TABLE dim_store (
    store_id  SERIAL PRIMARY KEY,
    name      VARCHAR(200) NOT NULL,
    location  VARCHAR(200),
    phone     VARCHAR(50),
    email     VARCHAR(255),
    city_id   INTEGER REFERENCES dim_city(city_id)
);

CREATE TABLE fact_sales (
    sale_id     SERIAL PRIMARY KEY,
    date_id     INTEGER      NOT NULL REFERENCES dim_date(date_id),
    customer_id INTEGER      NOT NULL REFERENCES dim_customer(customer_id),
    seller_id   INTEGER      NOT NULL REFERENCES dim_seller(seller_id),
    product_id  INTEGER      NOT NULL REFERENCES dim_product(product_id),
    store_id    INTEGER      REFERENCES dim_store(store_id),
    quantity    INTEGER      NOT NULL,
    total_price NUMERIC(10, 2) NOT NULL
);
