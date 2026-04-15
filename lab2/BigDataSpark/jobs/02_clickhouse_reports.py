from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/sparkdb"
PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

CH_DB = "sparkdb"


def read_pg(spark, table):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def write_ch(df, table):
    df.writeTo(f"clickhouse.{CH_DB}.{table}").createOrReplace()


spark = (
    SparkSession.builder
    .appName("ClickHouseReports")
    .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    .config("spark.sql.catalog.clickhouse.host", "clickhouse")
    .config("spark.sql.catalog.clickhouse.protocol", "http")
    .config("spark.sql.catalog.clickhouse.http_port", "8123")
    .config("spark.sql.catalog.clickhouse.user", "spark")
    .config("spark.sql.catalog.clickhouse.password", "spark")
    .config("spark.sql.catalog.clickhouse.database", CH_DB)
    .getOrCreate()
)

fact         = read_pg(spark, "fact_sales")
dim_product  = read_pg(spark, "dim_product")
dim_customer = read_pg(spark, "dim_customer")
dim_store    = read_pg(spark, "dim_store")
dim_supplier = read_pg(spark, "dim_supplier")
dim_date     = read_pg(spark, "dim_date")

# -------------------------------------------------------------------
# Report 1: Витрина продаж по продуктам
# -------------------------------------------------------------------
w_qty = Window.orderBy(F.desc("total_quantity_sold"))
w_cat = Window.partitionBy("category")

report1 = (
    fact.join(dim_product, "product_id")
    .groupBy(
        "product_id",
        dim_product["name"].alias("product_name"),
        dim_product["category"],
        dim_product["rating"],
        dim_product["reviews"],
    )
    .agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("total_price").alias("total_revenue"),
    )
    .withColumn("category_total_revenue", F.sum("total_revenue").over(w_cat))
    .withColumn("sales_rank", F.rank().over(w_qty))
    .select(
        "product_id", "product_name", "category",
        "total_quantity_sold", "total_revenue",
        F.col("rating").cast("double").alias("avg_rating"),
        F.col("reviews").cast("long").alias("total_reviews"),
        "category_total_revenue", "sales_rank",
    )
)
write_ch(report1, "report_products_sales")

# -------------------------------------------------------------------
# Report 2: Витрина продаж по клиентам
# -------------------------------------------------------------------
w_spent   = Window.orderBy(F.desc("total_spent"))
w_country = Window.partitionBy("country")

report2 = (
    fact.join(dim_customer, "customer_id")
    .groupBy(
        "customer_id",
        dim_customer["first_name"],
        dim_customer["last_name"],
        dim_customer["country"],
    )
    .agg(
        F.sum("total_price").alias("total_spent"),
        F.count("*").alias("order_count"),
        F.avg("total_price").alias("avg_order_value"),
    )
    .withColumn("customers_in_country", F.count("customer_id").over(w_country))
    .withColumn("customer_rank", F.rank().over(w_spent))
)
write_ch(report2, "report_customers_sales")

# -------------------------------------------------------------------
# Report 3: Витрина продаж по времени
# -------------------------------------------------------------------
report3 = (
    fact.join(dim_date, "date_id")
    .groupBy("year", "month")
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.avg("total_price").alias("avg_order_value"),
    )
    .orderBy("year", "month")
)
write_ch(report3, "report_time_sales")

# -------------------------------------------------------------------
# Report 4: Витрина продаж по магазинам
# -------------------------------------------------------------------
w_store = Window.orderBy(F.desc("total_revenue"))

report4 = (
    fact.join(dim_store, "store_id")
    .groupBy("store_id", "store_name", "store_city", "store_country")
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.avg("total_price").alias("avg_order_value"),
    )
    .withColumn("store_rank", F.rank().over(w_store))
)
write_ch(report4, "report_stores_sales")

# -------------------------------------------------------------------
# Report 5: Витрина продаж по поставщикам
# -------------------------------------------------------------------
w_sup = Window.orderBy(F.desc("total_revenue"))

report5 = (
    fact.join(dim_supplier, "supplier_id")
    .join(dim_product, "product_id")
    .groupBy(
        "supplier_id",
        dim_supplier["supplier_name"],
        dim_supplier["supplier_country"],
    )
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.avg(dim_product["price"]).alias("avg_product_price"),
        F.count("*").alias("total_orders"),
    )
    .withColumn("supplier_rank", F.rank().over(w_sup))
)
write_ch(report5, "report_suppliers_sales")

# -------------------------------------------------------------------
# Report 6: Витрина качества продукции
# -------------------------------------------------------------------
w_qual = Window.orderBy(F.desc("rating"))

report6 = (
    fact.join(dim_product, "product_id")
    .groupBy(
        "product_id",
        dim_product["name"].alias("product_name"),
        dim_product["category"],
        dim_product["rating"],
        dim_product["reviews"],
    )
    .agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("total_price").alias("total_revenue"),
    )
    .select(
        "product_id", "product_name", "category",
        F.col("rating").cast("double"),
        F.col("reviews").cast("long").alias("review_count"),
        "total_quantity_sold", "total_revenue",
    )
    .withColumn("quality_rank", F.rank().over(w_qual))
)
write_ch(report6, "report_product_quality")

spark.stop()
