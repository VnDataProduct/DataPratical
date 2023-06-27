from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import Window

import tempfile

spark = SparkSession.builder.getOrCreate()

path = "./vn_stock_data/"

df = spark.read.option("header", True).csv(path)

# with tempfile.TemporaryDirectory() as d:
#     df.write.parquet(path="./test.parquet", mode="overwrite")

df = df.withColumnsRenamed({'<Ticker>': 'ticker', '<DTYYYYMMDD>': 'date', '<Open>': 'open', '<High>': 'high',
                            '<Low>': 'low', '<Close>': 'close', "<Volume>": 'volume'})

df = df.withColumn('date', func.to_date(df.date, "yyyyMMdd"))

window7 = Window.partitionBy("ticker").orderBy(func.desc("date")).rowsBetween(0, 7)
window28 = Window.partitionBy("ticker").orderBy(func.desc("date")).rowsBetween(0, 28)
window90 = Window.partitionBy("ticker").orderBy(func.desc("date")).rowsBetween(0, 90)

df = df.withColumn("SMA7", func.avg("close").over(window7)) \
    .withColumn("SMA28", func.avg("close").over(window28)) \
    .withColumn("SMA90", func.avg("close").over(window90)) \
    .withColumn("EMA7", func.lit(0))

yesterdayEMA = Window.partitionBy("ticker").orderBy("date").rowsBetween(-1, -1)

k7 = 2 / (7 + 1)
k28 = 2 / (28 + 1)
k90 = 2 / (90 + 1)

df = df.withColumn("EMA7", df.close * k7 + func.max("EMA7").over(yesterdayEMA) * (1 - k7))

df.show(n=100)

# df = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/databasename") \
#     .option("dbtable", "tablename") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()
