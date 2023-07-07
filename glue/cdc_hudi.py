import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, lit, when, col
from pyspark.sql.types import StringType, BooleanType
from pyspark.sql.functions import udf

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pyspark.conf import SparkConf
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

import boto3
from jproperties import Properties
from urllib.parse import urlparse
from io import StringIO
import re


params = [
    'JOB_NAME',
    'TempDir',
    'aws_region',
    'config_s3_path',
]
args = getResolvedOptions(sys.argv, params)

conf = SparkConf()
conf.set('spark.sql.hive.convertMetastoreParquet', 'false')
conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
# conf.set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog')
conf.set('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
conf.set('spark.scheduler.mode', 'FAIR')
# spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session


def load_config(aws_region, config_s3_path):
    o = urlparse(config_s3_path, allow_fragments=False)
    client = boto3.client('s3', region_name=aws_region)
    data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
    contents = data['Body'].read().decode("utf-8")
    configs = Properties()
    configs.load(contents)
    return configs



params = load_config(args["aws_region"].strip(), args["config_s3_path"].strip())

s = StringIO()
params.list(out_stream=s)
logger.info("load config from s3 - my_log - params: {0}".format(s.getvalue()))
if not params:
    raise Exception("load config error  - my_log - s3_path: {0}".format(args["config_s3_path"]))

job_name = args['JOB_NAME']

aws_region = params["aws_region"].data
s3_endpoint = params["s3_endpoint"].data
checkpoint_location = params["checkpoint_location"].data
checkpoint_interval = params["checkpoint_interval"].data
kafka_broker = params["kafka_broker"].data
topic = params["topic"].data
startingOffsets = params["startingOffsets"].data
thread_max_workers = int(params["thread_max_workers"].data)
disable_msg = params["disable_msg"].data
cdc_format = params["cdc_format"].data
max_offsets_per_trigger = params["max_offsets_per_trigger"].data
consumer_group = params["consumer_group"].data
hudi_db_name = params["hudi_db_name"].data
hudi_s3_path = params["hudi_s3_path"].data

sync_table_list = json.loads(params["sync_table_list"].data)



reader = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("maxOffsetsPerTrigger", max_offsets_per_trigger) \
    .option("kafka.consumer.commit.groupid", consumer_group)
if startingOffsets == "earliest" or startingOffsets == "latest":
    reader.option("startingOffsets", startingOffsets)
else:
    reader.option("startingTimestamp", startingOffsets)
kafka_data = reader.load()

source_data = kafka_data.selectExpr("CAST(value AS STRING)")


def logger_msg(msg):
    if disable_msg == "false":
        logger.info(job_name + " - my_log - {0}".format(msg))
    else:
        pass


def getDFExampleString(df):
    if disable_msg == "false":
        data_str = df._jdf.showString(5, 20, False)
        # truncate false
        # data_str = df._jdf.showString(5, int(false), False)
        schema_str = df._jdf.schema().treeString()
        return schema_str + "\n" + data_str
    else:
        return "(disable show dataframe)"

def gen_filter_udf(db, table,cdc_format):
    def filter_table(str_json, ):
        reg_schema = ""
        reg_table = ""
        control_res = False
        if cdc_format == "DMS-CDC":
            reg_schema = '"schema-name":"{0}"'.format(db)
            reg_table = '"table-name":"{0}"'.format(table)
            record_type = '"record-type":"control"'
            control_pattern = re.compile(record_type)
            control_res = control_pattern.findall(str_json)
        elif cdc_format == "FLINK-CDC" or cdc_format == "MSK-DEBEZIUM-CDC":
            reg_schema = '"db":"{0}"'.format(db)
            reg_table = '"table":"{0}"'.format(table)
        schema_pattern = re.compile(reg_schema)
        schema_res = schema_pattern.findall(str_json)

        table_pattern = re.compile(reg_table)
        table_res = table_pattern.findall(str_json)

        if schema_res and table_res and (not control_res):
            return True
        else:
            return False
        # return '"schema-name":"{0}"'.format(db) in str_json and '"table-name":"{0}"'.format(table) in str_json
    return udf(filter_table, BooleanType())


def get_cdc_df_from_view(view_name, primary_key):
    # row_number order by metadata.timestamp get top 1, Merge the same primary key data in a batch, reduce copying to redshift data
    iud_df = ""
    cols_to_drop = ['operation_aws', 'seqnum_aws']
    if cdc_format == "DMS-CDC":
        partition_key = ",".join(["data." + pk for pk in primary_key.split(",")])
        # iud_op_sql = "select * from (select data.*, metadata.operation as operation, row_number() over (partition by {primary_key} order by metadata.timestamp desc) as seqnum  from {view_name} where (metadata.operation='load' or metadata.operation='delete' or metadata.operation='insert' or metadata.operation='update') and  metadata.`record-type`!='control' and metadata.`record-type`='data') t1 where seqnum=1".format(
        #     primary_key=partition_key, view_name="global_temp." + view_name)
        iud_op_sql = "select * from (select data.*, metadata.timestamp as mtime, metadata.operation as operation_aws, row_number() over (partition by {primary_key} order by metadata.timestamp desc) as seqnum_aws  from (select * from {view_name} where (metadata.operation='load' or metadata.operation='delete' or metadata.operation='insert' or metadata.operation='update') and  metadata.`record-type`!='control' and metadata.`record-type`='data') t1 )t2 where seqnum_aws=1".format(
            primary_key=partition_key, view_name="global_temp." + view_name)
        iud_df = spark.sql(iud_op_sql).withColumn('_hoodie_is_deleted',
                                     when(col('operation_aws') == "delete", True).otherwise(False)).drop(*cols_to_drop)

    elif cdc_format == "FLINK-CDC" or cdc_format == "MSK-DEBEZIUM-CDC":
        partition_key = ",".join(["after." + pk for pk in primary_key.split(",")])
        iud_op_sql = "select * from (select after.*, op as operation_aws, ts_ms as mtime, row_number() over (partition by {primary_key} order by ts_ms desc) as seqnum_aws  from {view_name} where (op='u' or op='d' or op='c' or op='r') ) t1 where seqnum_aws=1".format(
            primary_key=partition_key, view_name="global_temp." + view_name)
        iud_df = spark.sql(iud_op_sql).withColumn('_hoodie_is_deleted',
                                     when(col('operation_aws') == "d", True).otherwise(False)).drop(*cols_to_drop)

    return iud_df

def change_cdc_format_udf(cdc_format):
    def change_cdc_format(str_json, ):
        res_str_json = str_json
        if cdc_format == "FLINK-CDC" or cdc_format == "MSK-DEBEZIUM-CDC":
            match_op = re.search(r'"op":"(.*?)"', str_json)
            op_value = match_op.group(1)
            if op_value == "d":
                res_str_json = re.sub(r'"before":(.*?),"after":null',
                                      lambda
                                          match_data: f'"before":{match_data.group(1)},"after":{match_data.group(1)}',
                                      str_json)
        return res_str_json
    return udf(change_cdc_format, StringType())

def do_write(scf, hudi_db, table_name, primary_key):
    hudi_data_path = hudi_s3_path + hudi_db + "/" + table_name + "/"
    # use metadata.timestamp as precombine key
    precombine_key = "mtime"
    additional_options = {
        "hoodie.database.name": hudi_db,
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": primary_key,
        "hoodie.datasource.write.precombine.field": precombine_key,
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": hudi_db,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.write.markers.type": "DIRECT",
        "hoodie.cleaner.commits.retained": "2",
        "hoodie.keep.min.commits": "3",
        "hoodie.keep.max.commits": "4",
        "hoodie.metadata.enable": "false",
        "hoodie.delete.shuffle.parallelism": "10",
        "hoodie.upsert.shuffle.parallelism": "20",
        "hoodie.insert.shuffle.parallelism": "20",
        "hoodie.datasource.write.schema.allow.auto.evolution.column.drop": "true",
        "path": hudi_data_path
    }
    view_name = "kafka_source_" + table_name
    scf.createOrReplaceGlobalTempView(view_name)
    iud_df = get_cdc_df_from_view(view_name, primary_key)
    logger_msg("iud dataframe: " + getDFExampleString(iud_df))

    iud_df.write.format("hudi") \
        .options(**additional_options) \
        .mode("append") \
        .save()


def run_task(item, data_frame):
    task_status = {}
    try:
        logger_msg("sync table info:" + str(item))
        db_name = item["db_name"]
        table_name = item["table_name"]
        primary_key = item["primary_key"]
        task_status["table_name"] = table_name

        df = data_frame.filter(gen_filter_udf(db_name, table_name,cdc_format)(col('value')))
        fdf = df.select(change_cdc_format_udf(cdc_format)(col('value')).alias("value"))
        logger_msg("the table {0}: record number: {1}".format(table_name, str(fdf.count())))
        if fdf.count() > 0:
            logger_msg("the table {0}:  kafka source data: {1}".format(table_name, getDFExampleString(fdf)))
            # auto gen schema
            json_schema = spark.read.json(fdf.rdd.map(lambda p: str(p["value"]))).schema

            logger_msg("the table {0}: auto gen json schema: {1}".format(table_name, str(json_schema)))
            scf = fdf.select(from_json(col("value"), json_schema).alias("kdata")).select("kdata.*")

            logger_msg("the table {0}: kafka source data with auto gen schema: {1}".format(table_name,
                                                                                           getDFExampleString(scf)))

            do_write(scf, hudi_db_name, table_name, primary_key)
            logger_msg("sync the table complete: " + table_name)
            task_status["status"] = "finished"
            return task_status
        else:
            task_status["status"] = "this batch,table has no data"
    except Exception as e:
        task_status["status"] = "error"
        logger_msg(e)
        return task_status


def process_batch(data_frame, batchId):
    dfc = data_frame.cache()
    logger_msg("start process batch...")
    if data_frame:
        logger_msg("process batch id: " + str(batchId) + " record number: " + str(data_frame.count()))
        if data_frame.count() > 0:
            with ThreadPoolExecutor(max_workers=thread_max_workers) as pool:
                futures = [pool.submit(run_task, item, dfc) for
                           item in sync_table_list]
                task_list = []
                for future in as_completed(futures):
                    res = future.result()
                    logger_msg("future result: " + str(res))
                    if res:
                        task_list.append(res)
                        if res["status"] == "error":
                            logger_msg("task error, stop application" + str(task_list))
                            spark.stop()
                            raise Exception("task error, stop application" + str(task_list))
                logger_msg("task complete " + str(task_list))
                pool.shutdown(wait=False)
            dfc.unpersist()
            logger_msg("finish batch id: " + str(batchId))


save_to_hudi = source_data \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime=checkpoint_interval) \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

save_to_hudi.awaitTermination()
