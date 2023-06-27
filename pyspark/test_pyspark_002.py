from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .config("spark.sql.ansi.enabled", "true") \
    .getOrCreate()

path = "/opt/spark-data/vn_stock_data/"


def ema_calc(curr_value, last_ema, period):
    if last_ema:
        k = 2 / (period + 1)
        return float(curr_value) * k + float(last_ema) * (1 - k)
    else:
        return 0


ema_calc_udf = udf(lambda x, y, z: ema_calc(x, y, z), FloatType())
registered_udf = spark.udf.register("ema_calc_udf", ema_calc_udf)

df = spark.read.option("header", True).csv(path)
df = df.withColumnsRenamed({'<Ticker>': 'ticker', '<DTYYYYMMDD>': 'date', '<Open>': 'open', '<High>': 'high',
                            '<Low>': 'low', '<Close>': 'close', "<Volume>": 'volume'})
df = df.withColumn('date', to_date(df.date, "yyyyMMdd"))
df.createOrReplaceTempView("df")

#Calculate SMA first
spark.sql('CREATE OR REPLACE TEMPORARY VIEW df_1 AS (SELECT *, '
          'ROUND(AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), 4) as SMA7, '
          'ROUND(AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW), 4) as SMA30, '
          'ROUND(AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 60 PRECEDING AND CURRENT ROW), 4) as SMA60 '
          'from df)')

#We store it to a dataframe
df = spark.sql("SELECT * FROM df_1")

#Calculate EMA from SMA and select to a Dataframe
cal_df = spark.sql(
    "SELECT ticker, date, "
    "coalesce(ema_calc_udf(close, LAG(ema_7, 1) OVER (PARTITION BY ticker ORDER BY date), 7), t.ema_7) as EMA7, "
    "coalesce(ema_calc_udf(close, LAG(ema_30, 1) OVER (PARTITION BY ticker ORDER BY date), 30), t.ema_30) as EMA30, "
    "coalesce(ema_calc_udf(close, LAG(ema_60, 1) OVER (PARTITION BY ticker ORDER BY date), 60), t.ema_30) as EMA60 "
    "FROM (SELECT ticker, close, date, "
    "NTH_VALUE(SMA7, 7) OVER (PARTITION BY ticker ORDER BY date) as ema_7, "
    "NTH_VALUE(SMA30, 30) OVER (PARTITION BY ticker ORDER BY date) as ema_30, "
    "NTH_VALUE(SMA60, 60) OVER (PARTITION BY ticker ORDER BY date) as ema_60 "
    "FROM df_1) as t")

#Join two dataframes
df = df.alias("a").join(cal_df.alias('b'), [df.ticker == cal_df.ticker, df.date == cal_df.date], how='inner') \
    .select('a.*', round('b.EMA7', 4), round('b.EMA30', 4), round('b.EMA60', 4))

# df.show(100)
df.write.csv('/opt/spark-data/vn_stock_enrichment', mode="overwrite")
