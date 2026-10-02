CREATE EXTERNAL TABLE IF NOT EXISTS `silver_db`.`telemetry_data` (
  `machine_id` string,
  `timestamp` timestamp,
  `temperature_c` double,
  `humidity_percent` double,
  `vibration_mm_s` double,
  `pressure_bar` double,
  `equipment_status` string
)
PARTITIONED BY (
    `equipment_type` string
)

ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://your-bucket/silver/'
TBLPROPERTIES ('classification' = 'parquet');

--If your table already result 'None' in equipment_type column, then it means that the partitioning is not working properly. You can fix this by running the following command to repair the table and update the partitions:
MSCK REPAIR TABLE `silver_db`.`telemetry_data`;