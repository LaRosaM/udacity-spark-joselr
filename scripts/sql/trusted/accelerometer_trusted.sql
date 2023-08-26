CREATE EXTERNAL TABLE IF NOT EXISTS `joselr-spark`.`accelerometer_trusted` (
  `user` string,
  `timeStamp` bigint,
  `x` float,
  `y` float,
  `z` float
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-spark-joselr/accelerometer/trusted/'
TBLPROPERTIES ('classification' = 'json');