from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import clickhouse_connect

PG_URL = "jdbc:postgresql://postgres:5432/sparkdb"
PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

CH_HOST = "clickhouse"
CH_PORT = 8123
CH_USER = "spark"
CH_PASS = "spark"
CH_DB   = "sparkdb"


def read_pg(spark, table):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def write_ch(df, table, create_sql):
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS,
        database=CH_DB,
    )
    client.command(f"DROP TABLE IF EXISTS {CH_DB}.{table}")
    client.command(create_sql)
    pdf = df.toPandas()
    client.insert_df(table, pdf, database=CH_DB)


spark = SparkSession.builder.appName("ClickHouseReports").getOrCreate()

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
    .withColumn("sales_rank", F.rank().over(w_qty).cast("int"))
    .select(
        "product_id", "product_name", "category",
        "total_quantity_sold", "total_revenue",
        F.col("rating").cast("double").alias("avg_rating"),
        F.col("reviews").cast("long").alias("total_reviews"),
        "category_total_revenue", "sales_rank",
    )
)
write_ch(report1, "report_products_sales", """
    CREATE TABLE sparkdb.report_products_sales (
        product_id             Int32,
        product_name           String,
        category               String,
        total_quantity_sold    Int64,
        total_revenue          Float64,
        avg_rating             Float64,
        total_reviews          Int64,
        category_total_revenue Float64,
        sales_rank             Int32
    ) ENGINE = MergeTree() ORDER BY product_id
""")

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
    .withColumn("customer_rank", F.rank().over(w_spent).cast("int"))
)
write_ch(report2, "report_customers_sales", """
    CREATE TABLE sparkdb.report_customers_sales (
        customer_id          Int32,
        first_name           String,
        last_name            String,
        country              String,
        total_spent          Float64,
        order_count          Int64,
        avg_order_value      Float64,
        customers_in_country Int64,
        customer_rank        Int32
    ) ENGINE = MergeTree() ORDER BY customer_id
""")

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
write_ch(report3, "report_time_sales", """
    CREATE TABLE sparkdb.report_time_sales (
        year            Int32,
        month           Int32,
        total_revenue   Float64,
        total_orders    Int64,
        avg_order_value Float64
    ) ENGINE = MergeTree() ORDER BY (year, month)
""")

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
    .withColumn("store_rank", F.rank().over(w_store).cast("int"))
)
write_ch(report4, "report_stores_sales", """
    CREATE TABLE sparkdb.report_stores_sales (
        store_id        Int32,
        store_name      String,
        store_city      String,
        store_country   String,
        total_revenue   Float64,
        total_orders    Int64,
        avg_order_value Float64,
        store_rank      Int32
    ) ENGINE = MergeTree() ORDER BY store_id
""")

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
    .withColumn("supplier_rank", F.rank().over(w_sup).cast("int"))
)
write_ch(report5, "report_suppliers_sales", """
    CREATE TABLE sparkdb.report_suppliers_sales (
        supplier_id       Int32,
        supplier_name     String,
        supplier_country  String,
        total_revenue     Float64,
        avg_product_price Float64,
        total_orders      Int64,
        supplier_rank     Int32
    ) ENGINE = MergeTree() ORDER BY supplier_id
""")

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
    .withColumn("quality_rank", F.rank().over(w_qual).cast("int"))
)
write_ch(report6, "report_product_quality", """
    CREATE TABLE sparkdb.report_product_quality (
        product_id          Int32,
        product_name        String,
        category            String,
        rating              Float64,
        review_count        Int64,
        total_quantity_sold Int64,
        total_revenue       Float64,
        quality_rank        Int32
    ) ENGINE = MergeTree() ORDER BY product_id
""")

spark.stop()
