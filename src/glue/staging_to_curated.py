import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import awswrangler as wr
import json

# Loaded variables from parameters
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "input_s3_bucket_name",
    "input_s3_bucket_prefix",
    "input_s3_format",
    "output_s3_bucket_name",
    "output_s3_bucket_prefix",
    "output_glue_database_name",
    "output_glue_table_name",
    "output_partitions_types",
    "output_s3_format",
    "output_s3_compression"
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(args)

job_id = args['JOB_ID']

input_s3_bucket_name = args['input_s3_bucket_name']
input_s3_bucket_prefix = args['input_s3_bucket_prefix']
input_s3_format = args['input_s3_format']

output_s3_bucket_name = args['output_s3_bucket_name']
output_s3_bucket_prefix = args['output_s3_bucket_prefix']
output_glue_database_name = args['output_glue_database_name']
output_glue_table_name = args['output_glue_table_name']
output_s3_compression = args["output_s3_compression"]
output_s3_format = args["output_s3_format"]
output_partitions_types = json.loads(args['output_partitions_types'])

if input_s3_bucket_prefix.startswith('/'):
    input_s3_bucket_prefix = input_s3_bucket_prefix[1:]

if output_s3_bucket_prefix.startswith('/'):
    output_s3_bucket_prefix = output_s3_bucket_prefix[1:]


def logger(message=''):
    print(json.dumps({
        "job_id": job_id,
        "message": message
    }))


def get_current_dynamicframe_column_types(dynamicframe):
    df_schema = dynamicframe.schema()
    df_json_schema = df_schema.jsonValue()

    columns_types = {}
    for field in df_json_schema['fields']:
        field_name = field["name"]
        field_type = field['container']['dataType']
        columns_types[f"{field_name}"] = field_type
    return columns_types


def remove_partitions_from_columns_types(dynamicframe_columns_types, partition_keys):
    logger(message='Removing partitions from columns types')
    for partition_key in partition_keys:
        logger(message=f'Removing partition {partition_key} from columns types')
        del dynamicframe_columns_types[partition_key]
    logger(message=dynamicframe_columns_types)
    return dynamicframe_columns_types


logger(message='Started processing')

partition_keys = list(output_partitions_types.keys())
data_source = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format=input_s3_format,
    connection_options={
        "paths": [f"s3://{input_s3_bucket_name}/{input_s3_bucket_prefix}"],
        'groupFiles': 'inPartition',
        'groupSize': '104857600',
        "recurse": True
    },
    transformation_ctx=f"{input_s3_bucket_name}_{input_s3_bucket_prefix}_{output_glue_database_name}_{output_glue_table_name}_rcontext1"
)

data_source_count = data_source.count()

logger(message=f'Count of records: {data_source_count}')

if data_source_count > 0:
    logger(message='Writing to Parquet.')

    incremental_sink = glueContext.getSink(
        format_options={
            "compression": output_s3_compression
        },
        connection_type="s3",
        path=f"s3://{output_s3_bucket_name}/{output_s3_bucket_prefix}",
        partitionKeys=partition_keys,
        transformation_ctx=f"{input_s3_bucket_name}_{input_s3_bucket_prefix}_{output_glue_database_name}_{output_glue_table_name}_wcontext1"
    )

    incremental_sink.setFormat(output_s3_format)
    incremental_sink.writeFrame(data_source)

    logger(message='Parsing current dynamicframe schema')

    dynamicframe_columns_types = get_current_dynamicframe_column_types(data_source)
    dynamicframe_columns_types = remove_partitions_from_columns_types(dynamicframe_columns_types, partition_keys)

    table_exists = wr.catalog.does_table_exist(
        database=output_glue_database_name, table=output_glue_table_name)
    if table_exists == False:
        logger(message=f"Creating table {output_glue_table_name}")
        wr.catalog.create_parquet_table(
            database=output_glue_database_name,
            table=output_glue_table_name,
            path=f"s3://{output_s3_bucket_name}/{output_s3_bucket_prefix}",
            columns_types=dynamicframe_columns_types,
            compression=output_s3_compression,
            description='Table managed through AWS Data Wrangler from a Glue Job',
            partitions_types=output_partitions_types
        )
    else:
        logger(message='Table already exists, skipping creation')
        logger(message='Updating table')

        existing_table_columns = wr.catalog.table(
            database=output_glue_database_name, table=output_glue_table_name)
        existing_table_columns_json = json.loads(
            existing_table_columns.to_json(orient='records')
        )

        existing_columns_types = {}

        # Build up base of columns
        for column in existing_table_columns_json:
            col_name = column['Column Name']
            existing_columns_types[col_name] = column['Type']

        for col_name in dynamicframe_columns_types:
            if existing_columns_types.get(col_name) == None:
                logger(
                    message=f"Found new column '{col_name}' with type '{dynamicframe_columns_types[col_name]}'. Adding to table.")

                col_type = dynamicframe_columns_types[col_name]
                if col_type not in ['int', 'bigint', 'integer', 'boolean', 'tinyint', 'smallint', 'double', 'float',
                                    'char', 'decimal', 'varchar', 'string', 'binary', 'date', 'timestamp']:
                    col_type = 'string'

                wr.catalog.add_column(
                    database=output_glue_database_name,
                    table=output_glue_table_name,
                    column_name=col_name,
                    column_type=col_type,
                    column_comment='Dynamically added through Glue Job'
                )

    wr.athena.start_query_execution(sql=f"MSCK REPAIR TABLE {output_glue_table_name};",
                                    database=output_glue_database_name)


else:
    logger(message="No rows in dataframe, skipping")

logger(message="Done")

job.commit()