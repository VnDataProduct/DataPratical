#Download dataset
https://www.kaggle.com/datasets/nguyenngocphung/stock-prices-vn30-indexvietnam

#Deploy
./bin/spark-submit \
--master spark://192.168.0.4:7077 \
../DataPratical/pyspark/test_pyspark_002.py \
1000