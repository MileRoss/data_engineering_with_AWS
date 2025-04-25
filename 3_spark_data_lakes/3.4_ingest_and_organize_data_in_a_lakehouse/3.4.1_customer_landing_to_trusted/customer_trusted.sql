CREATE EXTERNAL TABLE `customer_trusted`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bucket-milenko/customer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Customer Landing to Trusted', 
  'CreatedByJobRun'='jr_05d4ba02323badc11e77ef69f25e11f90ed49de0b0868fbf4de37e37cdcc47f2', 
  'classification'='json')