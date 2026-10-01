CREATE EXTERNAL TABLE IF NOT EXISTS `silver_db`.`telemetry_data` (
  `machine_id` string,
  `equipment_type` string,
  `timestamp` timestamp,
  `temperature_c` double,
  `humidity_percent` double,
  `vibration_mm_s` double,
  `pressure_bar` double,
  `equipment_status` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://your-bucket/silver/'
TBLPROPERTIES ('classification' = 'parquet');