CREATE EXTERNAL TABLE `accelerometer_trusted`(
  `serialnumber` string COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bucket-milenko/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Accelerometer Landing to Trusted', 
  'CreatedByJobRun'='jr_d94952203cbe295dd6cb6ca56e1b88b4e32e866e20423f61cbc40e8093b4351d', 
  'classification'='json')
