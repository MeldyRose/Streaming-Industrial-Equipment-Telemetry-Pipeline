import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp
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
    .appName("TelemetrySilverStreaming")
    .master("local[*]")
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

#Define schema
schema = StructType([
    StructField("machine_id", StringType()),
    StructField("equipment_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_percent", DoubleType()),
    StructField("vibration_mm_s", DoubleType()),
    StructField("pressure_bar", DoubleType()),
])

#Read bronze
bronze_df=(
    spark.readStream
    .schema(schema)
    .json(
        f"s3a://{bucket_name}/bronze/"
    )
)

#transform silver
silver_df = (
    bronze_df
    .withColumn(
        "timestamp",
        to_timestamp("timestamp")
    )
    .withColumn(
        "equipment_status",
        when(
            (col("temperature_c") > 85) | (col("vibration_mm_s") > 5),
            "WARNING"
        ).otherwise("NORMAL")
    )
)

#save silver
query = (
    silver_df.writeStream
    .format("parquet")
    .partitionBy("equipment_type")
    .outputMode("append")
    .option(
        "path",
        f"s3a://{bucket_name}/silver/"
    )
    .option(
        "checkpointLocation",
        f"s3a://{bucket_name}/checkpoints/silver/"
    )
    .start()
)
query.awaitTermination()
