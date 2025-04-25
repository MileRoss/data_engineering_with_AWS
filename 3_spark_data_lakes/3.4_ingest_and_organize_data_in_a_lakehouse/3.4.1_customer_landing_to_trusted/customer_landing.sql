CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `registrationdate` string COMMENT 'from deserializer', 
  `lastupdatedate` string COMMENT 'from deserializer', 
  `sharewithresearchasofdate` string COMMENT 'from deserializer', 
  `sharewithpublicasofdate` string COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bucket-milenko/customer/landing/'
TBLPROPERTIES (
  'classification'='json')