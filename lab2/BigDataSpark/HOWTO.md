# Инструкция запуска

## Требования

- Docker >= 24
- Docker Compose >= 2.20

---

## 1. Запуск инфраструктуры

```bash
docker-compose up -d --build
```

PostgreSQL автоматически создаст таблицу `mock_data` и загрузит все 10 файлов CSV при первом старте.  
Cassandra автоматически создаст keyspace `sparkdb` и 6 таблиц отчётов через сервис `cassandra-init`.

Дождаться готовности (примерно 2–3 минуты):

```bash
docker-compose ps
```

Все сервисы должны быть в статусе `healthy` / `running`.

---

## 2. Spark-джобы

Spark запускается в режиме `local[*]` внутри контейнера `spark`.

### Переменные (для удобства)

```bash
PG_PKG="org.postgresql:postgresql:42.7.3"
CAS_PKG="com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
NEO_PKG="org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3"
MDB_PKG="org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
```

---

### Джоб 1 — ETL в модель «звезда» (PostgreSQL → PostgreSQL)

```bash
docker exec spark spark-submit \
  --master "local[*]" \
  --packages "org.postgresql:postgresql:42.7.3" \
  /opt/spark-jobs/01_star_schema.py
```

Создаёт таблицы: `dim_customer`, `dim_seller`, `dim_product`, `dim_store`,
`dim_supplier`, `dim_date`, `fact_sales`.

---

### Джоб 2 — Отчёты в ClickHouse (обязательно)

```bash
docker exec spark spark-submit \
  --master "local[*]" \
  --packages "org.postgresql:postgresql:42.7.3" \
  /opt/spark-jobs/02_clickhouse_reports.py
```

Создаёт таблицы в базе `sparkdb` ClickHouse:
`report_products_sales`, `report_customers_sales`, `report_time_sales`,
`report_stores_sales`, `report_suppliers_sales`, `report_product_quality`.

> Отчёты записываются через `clickhouse-connect` (HTTP API), без дополнительных JAR.

---

### Джоб 3 — Отчёты в Cassandra (опционально)

```bash
docker exec spark spark-submit \
  --master "local[*]" \
  --packages "org.postgresql:postgresql:42.7.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0" \
  --conf "spark.cassandra.connection.host=cassandra" \
  /opt/spark-jobs/03_cassandra_reports.py
```

Записывает данные в keyspace `sparkdb` Cassandra.

---

### Джоб 4 — Отчёты в Neo4j (опционально)

```bash
docker exec spark spark-submit \
  --master "local[*]" \
  --packages "org.postgresql:postgresql:42.7.3,org.neo4j:neo4j-connector-apache-spark_2.12:5.3.2_for_spark_3" \
  /opt/spark-jobs/04_neo4j_reports.py
```

Создаёт узлы Neo4j с метками:
`ReportProductsSales`, `ReportCustomersSales`, `ReportTimeSales`,
`ReportStoresSales`, `ReportSuppliersSales`, `ReportProductQuality`.

---

### Джоб 5 — Отчёты в MongoDB (опционально)

```bash
docker exec spark spark-submit \
  --master "local[*]" \
  --packages "org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0" \
  /opt/spark-jobs/05_mongodb_reports.py
```

Создаёт коллекции в базе `sparkdb` MongoDB.

---

### Джоб 6 — Отчёты в Valkey (опционально)

```bash
docker exec spark spark-submit \
  --master "local[*]" \
  --packages "org.postgresql:postgresql:42.7.3" \
  /opt/spark-jobs/06_valkey_reports.py
```

Каждый отчёт хранится как список Redis-хешей.  
Ключи: `report_products_sales:<id>`, индекс — список `report_products_sales`.

---

## 3. Проверка результатов

### ClickHouse (DBeaver / HTTP)

Подключение: `jdbc:clickhouse://localhost:8123/sparkdb`, логин `spark` / `spark`.

```sql
SELECT * FROM report_products_sales ORDER BY sales_rank LIMIT 10;
SELECT * FROM report_time_sales     ORDER BY year, month;
```

### Cassandra (CQL)

```bash
docker exec -it cassandra cqlsh
```
```cql
USE sparkdb;
SELECT * FROM report_products_sales LIMIT 10;
```

### Neo4j (Cypher — DBeaver или браузер http://localhost:7474)

Логин `neo4j` / `spark_password`.

```cypher
MATCH (n:ReportProductsSales) RETURN n ORDER BY n.sales_rank LIMIT 10;
MATCH (n:ReportTimeSales)     RETURN n ORDER BY n.year, n.month;
```

### MongoDB (Compass или mongosh)

```bash
docker exec -it mongodb mongosh -u spark -p spark --authenticationDatabase admin
```
```js
use sparkdb
db.report_products_sales.find().sort({sales_rank:1}).limit(10)
```

### Valkey (redis-cli)

```bash
docker exec -it valkey valkey-cli
```
```
LRANGE report_products_sales 0 9
HGETALL report_products_sales:1
```

---

## 4. Остановка

```bash
docker-compose down -v
```
