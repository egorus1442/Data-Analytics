from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/sparkdb"
PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

NEO4J_URL  = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "spark_password"


def read_pg(spark, table):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def cast_decimals(df):
    from pyspark.sql.types import DecimalType
    for field in df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            df = df.withColumn(field.name, F.col(field.name).cast("double"))
    return df


def write_neo4j(df, label, node_keys):
    df = cast_decimals(df)
    (
        df.write
        .format("org.neo4j.spark.DataSource")
        .mode("overwrite")
        .option("url", NEO4J_URL)
        .option("authentication.type", "basic")
        .option("authentication.basic.username", NEO4J_USER)
        .option("authentication.basic.password", NEO4J_PASS)
        .option("labels", f":{label}")
        .option("node.keys", node_keys)
        .save()
    )


spark = SparkSession.builder.appName("Neo4jReports").getOrCreate()

fact         = read_pg(spark, "fact_sales")
dim_product  = read_pg(spark, "dim_product")
dim_customer = read_pg(spark, "dim_customer")
dim_store    = read_pg(spark, "dim_store")
dim_supplier = read_pg(spark, "dim_supplier")
dim_date     = read_pg(spark, "dim_date")

# -------------------------------------------------------------------
# Report 1
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
        F.sum("total_price").cast("double").alias("total_revenue"),
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
write_neo4j(report1, "ReportProductsSales", "product_id")

# -------------------------------------------------------------------
# Report 2
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
        F.sum("total_price").cast("double").alias("total_spent"),
        F.count("*").alias("order_count"),
        F.avg("total_price").cast("double").alias("avg_order_value"),
    )
    .withColumn("customers_in_country", F.count("customer_id").over(w_country))
    .withColumn("customer_rank", F.rank().over(w_spent).cast("int"))
)
write_neo4j(report2, "ReportCustomersSales", "customer_id")

# -------------------------------------------------------------------
# Report 3
# -------------------------------------------------------------------
report3 = (
    fact.join(dim_date, "date_id")
    .groupBy("year", "month")
    .agg(
        F.sum("total_price").cast("double").alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.avg("total_price").cast("double").alias("avg_order_value"),
    )
    .orderBy("year", "month")
)
write_neo4j(report3, "ReportTimeSales", "year,month")

# -------------------------------------------------------------------
# Report 4
# -------------------------------------------------------------------
w_store = Window.orderBy(F.desc("total_revenue"))

report4 = (
    fact.join(dim_store, "store_id")
    .groupBy("store_id", "store_name", "store_city", "store_country")
    .agg(
        F.sum("total_price").cast("double").alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.avg("total_price").cast("double").alias("avg_order_value"),
    )
    .withColumn("store_rank", F.rank().over(w_store).cast("int"))
)
write_neo4j(report4, "ReportStoresSales", "store_id")

# -------------------------------------------------------------------
# Report 5
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
        F.sum("total_price").cast("double").alias("total_revenue"),
        F.avg(dim_product["price"]).cast("double").alias("avg_product_price"),
        F.count("*").alias("total_orders"),
    )
    .withColumn("supplier_rank", F.rank().over(w_sup).cast("int"))
)
write_neo4j(report5, "ReportSuppliersSales", "supplier_id")

# -------------------------------------------------------------------
# Report 6
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
        F.sum("total_price").cast("double").alias("total_revenue"),
    )
    .select(
        "product_id", "product_name", "category",
        F.col("rating").cast("double"),
        F.col("reviews").cast("long").alias("review_count"),
        "total_quantity_sold", "total_revenue",
    )
    .withColumn("quality_rank", F.rank().over(w_qual).cast("int"))
)
write_neo4j(report6, "ReportProductQuality", "product_id")

spark.stop()
