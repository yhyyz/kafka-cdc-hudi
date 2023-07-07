#### glue job conf
```shell
# --aws_region  eg. us-east-1 
* job 相关的配置,写入到配置文件,上传到S3,Job会从S3读取配置文件
# --config_s3_path eg. s3://panchao-data/conf/job.properties
* hudi-0.12的jar, 下载后上传到S3. 下载地址: https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-spark3.1-bundle_2.12/0.12.2/hudi-spark3.1-bundle_2.12-0.12.2.jar
* hudi-0.12的jar, 下载后上传到S3. 下载地址: https://dxs9dnjebzm6y.cloudfront.net/tmp/spark-sql-kafka-offset-committer-1.0.jar
# --extra-jars  eg. s3://panchao-data/jars/spark3.1-bundle_2.12-0.12.2.jar,s3://panchao-data/jars/spark-sql-kafka-offset-committer-1.0.jar
# --additional-python-modules jproperties
# --conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener
```

#### job properties
```shell
aws_region = us-east-1
# spark streaming job的checkpoint路径
checkpoint_location = s3://panchao-data/glue-spark/checkpoint/
# 多长时间处理一次数据，这个控制着写入的延迟，根据数据量大小 60 seconds ~ 180 seconds，下方设置的是10s,如果数据量大，写入性能会有损失，不建议设置太短，可以从60 seconds为基准开始
checkpoint_interval = 10 seconds
# kafka broker地址
kafka_broker = b-1.commonmskpanchao.wp46nn.c9.kafka.us-east-1.amazonaws.com:9092
# 需要同步的topic
topic = user_cdc_data,product_cdc_data
# 首次启动从kafka的最早记录消费
startingOffsets = earliest
# FLINK-CDC or DMS-CDC or MSK-DEBEZIUM-CDC
cdc_format = FLINK-CDC
# 并行写的线程数，比要同步的表的数据略大即可
thread_max_workers = 2
# 是否禁用日志
disable_msg = false
# 每个batch最大从kafka拉取的数据
max_offsets_per_trigger = 1000000
# 同步的表在Glue的哪个数据库，先在Glue手动创建这个数据库，名字自定义
hudi_db_name = cdc_hudi_db
# 写入的hudi的路径
hudi_s3_path = s3://panchao-data/glue-hudi-02/
# 要同步的mysql表，db_name指的是mysql的数据库名，支持正则，table_name指的是mysql表名，支持正则，primary_key指的是表的主键，可以是多个，逗号分隔。save_delete表示是否将detele数据单独写入到一张表。
sync_table_list = [\
  {"db_name": "test_db", "table_name": "product", "primary_key": "id"},\
  {"db_name": "test_db", "table_name": "user", "primary_key": "id"}\
]
```