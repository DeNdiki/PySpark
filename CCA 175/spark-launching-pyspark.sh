pyspark --master yarn \
  --conf spark.ui.port=12890 \
  --num-executors 2 \
  --executor-memory 512M \
  --packages com.databricks:spark-avro_2.10:2.0.1