import json, sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from glue_utils import utils
from awsglue.utils import getResolvedOptions
from pyspark.sql import Window
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql.functions import explode, col, row_number, lit, explode, col, row_number, from_json
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name_scripts', 's3_path_source', 's3_path_transform'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

config =utils.read_json_from_s3(args['bucket_name_scripts'], "scripts/stage/etl-config.json")
source_inclussions = ['0material_attr','0material_text','zt024_attr','zt439a_attr']
dyf_roots_zt024_attr = utils.transform(glueContext, source_config={'type': 's3','s3_path':"{}".format(args['s3_path_source']),'source': 'zt024_attr'} , config=config, source_inclussions=source_inclussions)
dyf_roots_zt439a_attr = utils.transform(glueContext, source_config={'type': 's3','s3_path':"{}".format(args['s3_path_source']),'source': 'zt439a_attr'} , config=config, source_inclussions=source_inclussions)
dyf_roots_material_attr = utils.transform(glueContext, source_config={'type': 's3','s3_path':"{}".format(args['s3_path_source']),'source': '0material_attr'} , config=config, source_inclussions=source_inclussions)
dyf_roots_material_text = utils.transform(glueContext, source_config={'type': 's3','s3_path':"{}".format(args['s3_path_source']),'source': '0material_text'} , config=config, source_inclussions=source_inclussions)

df_T024_stage=dyf_roots_zt024_attr['zt024_attr']['data'].toDF()
df_T439A_stage=dyf_roots_zt439a_attr['zt439a_attr']['data'].toDF()
df_MARA_stage=dyf_roots_material_attr['0material_attr']['data'].toDF()
df_MAKT_stage=dyf_roots_material_text['0material_text']['data'].toDF()

dyf_roots={}

dyf_T024_stage= DynamicFrame.fromDF(df_T024_stage, glueContext, "dyf_T024_stage")
dyf_roots['TO24'] = {'table': 'TO24', 'data': dyf_T024_stage}

dyf_T439A_stage= DynamicFrame.fromDF(df_T439A_stage, glueContext, "dyf_T439A_stage")
dyf_roots['T439A'] = {'table': 'T439A', 'data': dyf_T439A_stage}

dyf_MARA_stage= DynamicFrame.fromDF(df_MARA_stage, glueContext, "dyf_MARA_stage")
dyf_roots['MARA'] = {'table': 'MARA', 'data': dyf_MARA_stage}

dyf_MAKT_stage= DynamicFrame.fromDF(df_MAKT_stage, glueContext, "dyf_MAKT_stage")
dyf_roots['MAKT'] = {'table': 'MAKT', 'data': dyf_MAKT_stage}


utils.save_dyf(glueContext, args['s3_path_transform'], dyf_roots,'parquet')