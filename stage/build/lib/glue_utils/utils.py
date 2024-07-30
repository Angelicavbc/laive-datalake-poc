import boto3, json
from awsglue import DynamicFrame
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import col, row_number, col, row_number

def get_dyf_from_s3(glueContext, source_config, format='csv'):
    cnn_options = {
        "paths": [source_config['s3_path'] + source_config['source'] + "/"], 
        "recurse": True,
        "compression": "gzip"
    }
    if ('groupFiles' in source_config):
        cnn_options['groupFiles'] = source_config['groupFiles']
    if ('groupSize' in source_config):
        cnn_options['groupSize'] = source_config['groupSize']
    if ('useS3ListImplementation' in source_config):
        cnn_options['useS3ListImplementation'] = source_config['useS3ListImplementation']
    return glueContext.create_dynamic_frame.from_options( \
            connection_type="s3", \
            connection_options=cnn_options, \
            format=format, \
            format_options={"withHeader": True})

def read_json_from_s3(bucket_name, path):
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket_name, path)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


def transform(glueContext, source_config, config, source_inclussions = None):
    dyf_map = {}
    for el_name, el_config in config.items():
        if el_name in source_inclussions:
            source = el_config["source"]
            destination = el_config["destination"]
            mappings = el_config["mappings"]
#           autoincrement_column = el_config["autoincrement_column"]

            # Read CSV from the source path
            if source_config['type'] == 's3':
                dyf = get_dyf_from_s3(glueContext, source_config)

            # Apply column renaming and data type transformations
            dyf_schema = dyf.schema()
            dyf_t = dyf.apply_mapping(list(map(lambda t: (t[0], dyf_schema.getField(t[0]).dataType.typeName(), t[2], t[3]) if t[1] == "Dynamic" else tuple(t), mappings)))
#            if autoincrement_column:
#                autoincrement_order_by = el_config["autoincrement_order_by"]
#                window_spec_id = Window.orderBy(autoincrement_order_by)
#                df_t = dyf_t.toDF().withColumn(autoincrement_column, row_number().over(window_spec_id))\
#                       .withColumn(autoincrement_column, col(autoincrement_column))
#                
#               dyf_t = DynamicFrame.fromDF(df_t, glueContext, "dyf_t")            
            dyf_map[el_name] = {'table': destination, 'data': dyf_t}
    return dyf_map

def save_dyf(glueContext, s3_path, dyf_map, format = 'parquet', format_options = {}):
    for source, dyf_data in dyf_map.items():
        s3_resource_path = s3_path + dyf_data['table'] + '/'
        glueContext.purge_s3_path(s3_resource_path,  {"retentionPeriod": 0})
        glueContext.write_dynamic_frame.from_options(\
            frame = dyf_data['data'], \
            connection_options = {'path': s3_resource_path}, \
            connection_type = 's3', \
            format = format, \
            format_options = format_options)