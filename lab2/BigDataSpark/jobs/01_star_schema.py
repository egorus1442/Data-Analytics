from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/sparkdb"
PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}


def write_pg(df, table):
    df.write.jdbc(url=PG_URL, table=table, mode="overwrite", properties=PG_PROPS)


def read_pg(spark, table):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


spark = SparkSession.builder.appName("StarSchema").getOrCreate()

raw = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPS)
raw.cache()

dim_customer = (
    raw.select(
        F.col("sale_customer_id").alias("customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_age").alias("age"),
        F.col("customer_email").alias("email"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
        F.col("customer_pet_type").alias("pet_type"),
        F.col("customer_pet_name").alias("pet_name"),
        F.col("customer_pet_breed").alias("pet_breed"),
    )
    .dropDuplicates(["customer_id"])
)
write_pg(dim_customer, "dim_customer")

dim_seller = (
    raw.select(
        F.col("sale_seller_id").alias("seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code"),
    )
    .dropDuplicates(["seller_id"])
)
write_pg(dim_seller, "dim_seller")

dim_product = (
    raw.select(
        F.col("sale_product_id").alias("product_id"),
        F.col("product_name").alias("name"),
        F.col("product_category").alias("category"),
        F.col("product_price").alias("price"),
        F.col("pet_category"),
        F.col("product_weight").alias("weight"),
        F.col("product_color").alias("color"),
        F.col("product_size").alias("size"),
        F.col("product_brand").alias("brand"),
        F.col("product_material").alias("material"),
        F.col("product_description").alias("description"),
        F.col("product_rating").alias("rating"),
        F.col("product_reviews").alias("reviews"),
        F.col("product_release_date").alias("release_date"),
        F.col("product_expiry_date").alias("expiry_date"),
    )
    .dropDuplicates(["product_id"])
)
write_pg(dim_product, "dim_product")

w_store = Window.orderBy("store_name", "store_city")
dim_store = (
    raw.select(
        "store_name", "store_location", "store_city",
        "store_state", "store_country", "store_phone", "store_email",
    )
    .dropDuplicates(["store_name", "store_city"])
    .withColumn("store_id", F.row_number().over(w_store))
    .select("store_id", "store_name", "store_location", "store_city",
            "store_state", "store_country", "store_phone", "store_email")
)
dim_store.cache()
write_pg(dim_store, "dim_store")

w_sup = Window.orderBy("supplier_name", "supplier_email")
dim_supplier = (
    raw.select(
        "supplier_name", "supplier_contact", "supplier_email",
        "supplier_phone", "supplier_address", "supplier_city", "supplier_country",
    )
    .dropDuplicates(["supplier_name", "supplier_email"])
    .withColumn("supplier_id", F.row_number().over(w_sup))
    .select("supplier_id", "supplier_name", "supplier_contact", "supplier_email",
            "supplier_phone", "supplier_address", "supplier_city", "supplier_country")
)
dim_supplier.cache()
write_pg(dim_supplier, "dim_supplier")

dim_date = (
    raw.select(F.col("sale_date").alias("date_str"))
    .dropDuplicates()
    .withColumn("full_date", F.to_date("date_str", "M/d/yyyy"))
    .withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("day",     F.dayofmonth("full_date"))
    .withColumn("month",   F.month("full_date"))
    .withColumn("year",    F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .select("date_id", "full_date", "day", "month", "year", "quarter")
    .dropDuplicates(["date_id"])
)
write_pg(dim_date, "dim_date")

store_ref    = read_pg(spark, "dim_store").select("store_id", "store_name", "store_city")
supplier_ref = read_pg(spark, "dim_supplier").select("supplier_id", "supplier_name", "supplier_email")

fact_sales = (
    raw
    .join(store_ref,    ["store_name", "store_city"])
    .join(supplier_ref, ["supplier_name", "supplier_email"])
    .select(
        F.col("id").alias("sale_id"),
        F.col("sale_customer_id").alias("customer_id"),
        F.col("sale_seller_id").alias("seller_id"),
        F.col("sale_product_id").alias("product_id"),
        F.col("store_id"),
        F.col("supplier_id"),
        F.date_format(F.to_date("sale_date", "M/d/yyyy"), "yyyyMMdd")
         .cast("int").alias("date_id"),
        F.col("sale_quantity").alias("quantity"),
        F.col("sale_total_price").alias("total_price"),
    )
)
write_pg(fact_sales, "fact_sales")

spark.stop()
