from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import pandas as pd
from pyspark.ml.feature import StringIndexer, VectorAssembler

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('./vn_stock_enrichment', header=True, inferSchema=True)

pd.DataFrame(df.take(5), columns=df.columns).transpose()

print(df)

numeric_features = ['ticker', 'date', 'open', 'close', 'volume', 'SMA7', 'SMA30', 'SMA60', 'EMA7', 'EMA30', 'EMA60']
df = df.select(numeric_features).describe().toPandas().transpose()

assembler = VectorAssembler(inputCols=numeric_features, outputCol="features")
df = assembler.transform(df)
df.show()

print('----features')
print(numeric_features)
print(df)

