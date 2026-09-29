import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, to_date, avg, max, count, sum, window
from pyspark.sql.types import(
    StructType,
    StructField,
    StringType,
    DoubleType
)

aws_region = os.getenv("AWS_REGION")
bucket_name = os.getenv("S3_BUCKET_NAME")

#Start Spark
spark = (
    SparkSession.builder
    .appName("TelemetryGoldStreaming")
    .master("local[*]")
    .config(
        "spark.sql.streaming.schemaInference","true"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"s3.{aws_region}.amazonaws.com"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


#Define schema
silver_schema = StructType([
    StructField("machine_id", StringType()),
    StructField("equipment_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_percent", DoubleType()),
    StructField("vibration_mm_s", DoubleType()),
    StructField("pressure_bar", DoubleType()),
    StructField("equipment_status", StringType()),
])

#Read silver
silver_df=(
    spark.readStream
    .schema(silver_schema)
    .parquet(
        f"s3a://{bucket_name}/silver/"
    )
    .withColumn("timestamp", to_timestamp("timestamp"))
)

if "equipment_status" not in silver_df.columns:
    silver_df = silver_df.withColumn(
        "equipment_status",
        when((col("temperature_c") > 85) | (col("vibration_mm_s") > 5), "WARNING").otherwise("NORMAL")
    )

#transform silver to gold
gold_df = (
    silver_df
    .withWatermark("timestamp","1 hour")
    .withColumn("date", to_date("timestamp"))
    .groupBy(
        "machine_id",
        "equipment_type",
        "date",
        window("timestamp", "1 day")
    )
    .agg(
        avg("temperature_c").alias("avg_temperature_c"),
        max("temperature_c").alias("max_temperature_c"),
        avg("vibration_mm_s").alias("avg_vibration_mm_s"),
        max("vibration_mm_s").alias("max_vibration_mm_s"),
        avg("pressure_bar").alias("avg_pressure_bar"),
        count("*").alias("total_readings"),
        sum(
            when(col("equipment_status") == "WARNING", 1)
            .otherwise(0)
        ).alias("warning_count")

    )
    .drop("window")
)

#save gold
query = (
    gold_df.writeStream
    .format("parquet")
    .partitionBy("date")
    .outputMode("append")
    .option(
        "path",
        f"s3a://{bucket_name}/gold/"
    )
    .option(
        "checkpointLocation",
        f"s3a://{bucket_name}/checkpoints/gold/"
    )
    .start()
)
query.awaitTermination()
