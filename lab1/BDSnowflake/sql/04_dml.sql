INSERT INTO dim_country (country_name)
SELECT DISTINCT t.cn
FROM (
    SELECT customer_country AS cn FROM mock_data WHERE customer_country IS NOT NULL AND customer_country <> ''
    UNION
    SELECT seller_country   FROM mock_data WHERE seller_country   IS NOT NULL AND seller_country   <> ''
    UNION
    SELECT store_country    FROM mock_data WHERE store_country    IS NOT NULL AND store_country    <> ''
    UNION
    SELECT supplier_country FROM mock_data WHERE supplier_country IS NOT NULL AND supplier_country <> ''
) t;

INSERT INTO dim_city (city_name, state, country_id)
SELECT t.city_name, t.state, dc.country_id
FROM (
    SELECT DISTINCT ON (city_name, country_name)
        city_name,
        state,
        country_name
    FROM (
        SELECT
            store_city                AS city_name,
            NULLIF(store_state, '')   AS state,
            store_country             AS country_name
        FROM mock_data
        WHERE store_city IS NOT NULL AND store_city <> ''
          AND store_country IS NOT NULL AND store_country <> ''
        UNION ALL
        SELECT
            supplier_city,
            NULL,
            supplier_country
        FROM mock_data
        WHERE supplier_city IS NOT NULL AND supplier_city <> ''
          AND supplier_country IS NOT NULL AND supplier_country <> ''
    ) sub
    ORDER BY city_name, country_name, state NULLS LAST
) t
JOIN dim_country dc ON dc.country_name = t.country_name;

INSERT INTO dim_pet_category (category_name)
SELECT DISTINCT pet_category
FROM mock_data
WHERE pet_category IS NOT NULL AND pet_category <> '';

INSERT INTO dim_product_category (category_name)
SELECT DISTINCT product_category
FROM mock_data
WHERE product_category IS NOT NULL AND product_category <> '';

INSERT INTO dim_brand (brand_name)
SELECT DISTINCT product_brand
FROM mock_data
WHERE product_brand IS NOT NULL AND product_brand <> '';

INSERT INTO dim_supplier (name, contact, email, phone, address, city_id)
SELECT DISTINCT ON (m.supplier_name)
    m.supplier_name,
    m.supplier_contact,
    m.supplier_email,
    m.supplier_phone,
    m.supplier_address,
    dc.city_id
FROM mock_data m
LEFT JOIN dim_city dc
    ON dc.city_name = m.supplier_city
    AND dc.country_id = (
        SELECT country_id FROM dim_country WHERE country_name = m.supplier_country LIMIT 1
    )
WHERE m.supplier_name IS NOT NULL AND m.supplier_name <> ''
ORDER BY m.supplier_name;

INSERT INTO dim_pet (pet_name, pet_type, pet_breed, pet_category_id)
SELECT DISTINCT
    m.customer_pet_name,
    m.customer_pet_type,
    m.customer_pet_breed,
    dpc.pet_category_id
FROM mock_data m
JOIN dim_pet_category dpc ON dpc.category_name = m.pet_category
WHERE m.customer_pet_name IS NOT NULL AND m.customer_pet_name <> '';

INSERT INTO dim_date (full_date, day, month, year, quarter, day_of_week)
SELECT DISTINCT
    TO_DATE(sale_date, 'MM/DD/YYYY'),
    EXTRACT(DAY     FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT,
    EXTRACT(MONTH   FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT,
    EXTRACT(YEAR    FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT,
    EXTRACT(QUARTER FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT,
    EXTRACT(DOW     FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::SMALLINT
FROM mock_data
WHERE sale_date IS NOT NULL AND sale_date <> '';

INSERT INTO dim_customer (customer_id, first_name, last_name, age, email, postal_code, country_id, pet_id)
SELECT DISTINCT ON (m.sale_customer_id)
    m.sale_customer_id::INTEGER,
    m.customer_first_name,
    m.customer_last_name,
    NULLIF(m.customer_age, '')::SMALLINT,
    m.customer_email,
    NULLIF(m.customer_postal_code, ''),
    dc.country_id,
    dp.pet_id
FROM mock_data m
LEFT JOIN dim_country dc ON dc.country_name = m.customer_country
LEFT JOIN dim_pet_category dpc ON dpc.category_name = m.pet_category
LEFT JOIN dim_pet dp
    ON dp.pet_name        = m.customer_pet_name
   AND dp.pet_type        = m.customer_pet_type
   AND dp.pet_breed       = m.customer_pet_breed
   AND dp.pet_category_id = dpc.pet_category_id
WHERE m.sale_customer_id IS NOT NULL AND m.sale_customer_id <> ''
ORDER BY m.sale_customer_id;

INSERT INTO dim_seller (seller_id, first_name, last_name, email, postal_code, country_id)
SELECT DISTINCT ON (m.sale_seller_id)
    m.sale_seller_id::INTEGER,
    m.seller_first_name,
    m.seller_last_name,
    m.seller_email,
    NULLIF(m.seller_postal_code, ''),
    dc.country_id
FROM mock_data m
LEFT JOIN dim_country dc ON dc.country_name = m.seller_country
WHERE m.sale_seller_id IS NOT NULL AND m.sale_seller_id <> ''
ORDER BY m.sale_seller_id;

INSERT INTO dim_product (product_id, name, price, stock_quantity, weight, color, size, material, description, rating, reviews, release_date, expiry_date, category_id, brand_id, supplier_id)
SELECT DISTINCT ON (m.sale_product_id)
    m.sale_product_id::INTEGER,
    m.product_name,
    NULLIF(m.product_price, '')::NUMERIC(10, 2),
    NULLIF(m.product_quantity, '')::INTEGER,
    NULLIF(m.product_weight, '')::NUMERIC(10, 2),
    m.product_color,
    m.product_size,
    m.product_material,
    m.product_description,
    NULLIF(m.product_rating, '')::NUMERIC(3, 1),
    NULLIF(m.product_reviews, '')::INTEGER,
    TO_DATE(NULLIF(m.product_release_date, ''), 'MM/DD/YYYY'),
    TO_DATE(NULLIF(m.product_expiry_date, ''),  'MM/DD/YYYY'),
    dpc.category_id,
    db.brand_id,
    ds.supplier_id
FROM mock_data m
LEFT JOIN dim_product_category dpc ON dpc.category_name = m.product_category
LEFT JOIN dim_brand db             ON db.brand_name      = m.product_brand
LEFT JOIN dim_supplier ds          ON ds.name            = m.supplier_name
WHERE m.sale_product_id IS NOT NULL AND m.sale_product_id <> ''
ORDER BY m.sale_product_id;

INSERT INTO dim_store (name, location, phone, email, city_id)
SELECT DISTINCT ON (m.store_name)
    m.store_name,
    m.store_location,
    m.store_phone,
    m.store_email,
    dc.city_id
FROM mock_data m
LEFT JOIN dim_city dc
    ON dc.city_name  = m.store_city
   AND dc.country_id = (
        SELECT country_id FROM dim_country WHERE country_name = m.store_country LIMIT 1
   )
WHERE m.store_name IS NOT NULL AND m.store_name <> ''
ORDER BY m.store_name;

INSERT INTO fact_sales (date_id, customer_id, seller_id, product_id, store_id, quantity, total_price)
SELECT
    dd.date_id,
    m.sale_customer_id::INTEGER,
    m.sale_seller_id::INTEGER,
    m.sale_product_id::INTEGER,
    ds.store_id,
    m.sale_quantity::INTEGER,
    m.sale_total_price::NUMERIC(10, 2)
FROM mock_data m
JOIN dim_date dd    ON dd.full_date = TO_DATE(m.sale_date, 'MM/DD/YYYY')
LEFT JOIN dim_store ds ON ds.name   = m.store_name
WHERE m.sale_customer_id IS NOT NULL AND m.sale_customer_id <> ''
  AND m.sale_seller_id   IS NOT NULL AND m.sale_seller_id   <> ''
  AND m.sale_product_id  IS NOT NULL AND m.sale_product_id  <> ''
  AND m.sale_date        IS NOT NULL AND m.sale_date        <> '';
