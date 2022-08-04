import boto3
import pandas as pd
from datetime import datetime
import pytz
import os
import awswrangler as wr
import json
from cachetools import cached, TTLCache
import urllib.parse
import zlib
import uuid
from smart_open import open

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
glue = boto3.client('glue')
tz = pytz.timezone("US/Eastern")

ssm = boto3.client("ssm")

BUCKET_NAME = os.environ.get("BUCKET_NAME", "cbtsai-datalake-synnex-dev")
DATABASE = os.environ.get("DATABASE", "synnex_dev")
WORKGROUP = os.environ.get("WORKGROUP", "datalake-synnex")
SAVE_TO_DATALAKE = True
STAGE = os.environ.get("stage")
tz = pytz.timezone("US/Eastern")
df_format = "%Y-%m-%d %H:%M:%S"

def get_ssm_param(name, decrypt):
    param = ssm.get_parameter(
        Name=name,
        WithDecryption=decrypt
    )
    return param["Parameter"]["Value"]


def save_to_datalake(table, data, mode='append', partition_cols=None):
    print(f"saving to s3: {table}")
    tz = pytz.timezone("US/Eastern")
    df_format = "%Y-%m-%d %H:%M:%S"
    dt = datetime.now(tz).strftime(df_format)
    wrangler_response = {"paths": {}}

    if len(data) > 0:
        # save raw to s3
        df = pd.json_normalize(data)

        # set all dtypes to string
        cols = list(df.columns)
        athena_dtypes = {}

        if "processed_at" not in cols:
            df['processed_at'] = dt

        nacols = df.columns[df.isna().any()].tolist()

        # NA columns get to be understood as strings until told otherwise
        for col in nacols:
            del df[col]

        cols = list(df.columns)
        for col in cols:
            athena_dtypes[col] = "string"

        df = wr.catalog.sanitize_dataframe_columns_names(df)
        wr.catalog.drop_duplicated_columns(df)

        if SAVE_TO_DATALAKE:
            wrangler_response = wr.s3.to_parquet(
                df=df,
                path=f"s3://{BUCKET_NAME}/table={table}/etl_process=processed",
                index=False,
                dataset=True,
                mode=mode,
                catalog_versioning=True,
                database=DATABASE,
                table=table,
                partition_cols=partition_cols,
                dtype=athena_dtypes
            )
        else:
            print(f'skipping saving to the datalake: {table}')
        print(f"saving processed to s3: {table}")
    else:
        print('no data to save')
    return wrangler_response

@cached(cache=TTLCache(maxsize=4196,ttl=600))
def get_api_keys():
    API_KEYS = {
        "vco33-usvi1": get_ssm_param(f"/velocloud/vco33-usvi1/api_key", True)
    }
    return API_KEYS


def prepare_staging(event, context):
    logger.info(json.dumps(event))
    empty_staging_folder(event['table'])
    return event


def empty_staging_folder(table):
    '''Empty the S3 staging folder.'''
    logger.info(f'{table}: emptying staging folder')
    wr.s3.delete_objects(
        f's3://{BUCKET_NAME}/etl_process=staging/table={table}')
    wr.s3.delete_objects(
        f's3://{BUCKET_NAME}/etl_process=staging/table={table}_incremental')
    return table


def save_to_staging(table, data):
    '''Write a compressed JSON file to the staging S3 folder.'''
    logger.info('saving to staging')
    if len(data) == 0:
        logger.info(f'{table}: no data to save')
        return

    dt = datetime.now(tz).strftime(df_format)
    year = datetime.now(tz).strftime('%Y')
    month = datetime.now(tz).strftime('%m')

    df = pd.json_normalize(data)

    df['processed_at'] = dt
    df['processed_year'] = year
    df['processed_month'] = month

    # column names glue friendly
    df = prepare_dataframe_columns(df)
    df = convert_dataframe_to_string(df)

    data_prepped = df.to_json(orient='records', indent=2)

    with open(f's3://{BUCKET_NAME}/etl_process=staging/table={table}/{uuid.uuid4()}.json.gz', 'w') as f:
        f.write(data_prepped)

    return table


def save_data(event={}, context={}):
    '''Read the staging data, save it to parquet S3 datalake by default. '''
    logger.info(json.dumps(event))

    processor = event.get('processor', 'awswrangler')
    table = event['table']
    dlk_table = table + '_incremental'

    if processor == None or processor == 'awswrangler':
        data = wr.s3.read_json(
            path=f's3://{BUCKET_NAME}/etl_process=staging/table={dlk_table}',
            use_threads=12,
            dtype=str
        ).to_dict('records')

        logger.info(f"{dlk_table}: number of records {len(data)}")

        if len(data) == 0:
            logger.info(f'{dlk_table}: no data to save')

        save_to_curated(
            table=dlk_table,
            data=data,
            mode='append'
        )
        return event

    if processor == 'glue_job':
        job_name = f"cbtsai-datalake-velocloud-{STAGE}-staging_to_curated"
        job = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--input_s3_bucket_name": BUCKET_NAME,
                "--input_s3_bucket_prefix": f"etl_process=staging/table={dlk_table}/",
                "--output_s3_bucket_name": BUCKET_NAME,
                "--output_s3_bucket_prefix": f"etl_process=processed/table={dlk_table}/",
                "--output_glue_database_name": DATABASE,
                "--output_glue_table_name": dlk_table,
                "--output_partitions_types": event.get('output_partition_types', '{"processed_year": "string", "processed_month": "string"}')
            }
        )

        return {"job": job}


def convert_dataframe_to_string(df):
    df = df.fillna("")
    df = df.astype(str)

    df = df.apply(lambda x: x.apply(lambda y: y[:10000] if isinstance(y, str) else y))

    df = df.replace("nan", '')
    df = df.replace("None", '')

    return df


def prepare_dataframe_columns(df):
    df = wr.catalog.sanitize_dataframe_columns_names(df)
    wr.catalog.drop_duplicated_columns(df)
    return df


def save_to_raw(table, data):
    year = datetime.now(tz).strftime('%Y')
    month = datetime.now(tz).strftime('%m')

    if len(data) > 0:
        raw_path = f's3://{BUCKET_NAME}/etl_process=raw/table={table}/year={year}/month={month}/{uuid.uuid4()}.json.gz'
        logger.info(f'{table}: writing to: {raw_path}')
        with open(raw_path, 'w') as f:
            f.write(json.dumps(data, indent=2, default=str))

    return raw_path


def save_to_curated(table, data, mode='append', partition_cols=['processed_year', 'processed_month']):
    dt = datetime.now(tz).strftime(df_format)
    year = datetime.now(tz).strftime('%Y')
    month = datetime.now(tz).strftime('%m')

    if len(data) > 0:
    # save raw to s3
        df = pd.json_normalize(data)

        df['processed_at'] = dt
        df['processed_year'] = year
        df['processed_month'] = month

        # column names glue friendly
        df = prepare_dataframe_columns(df)
        df = convert_dataframe_to_string(df)

        logger.info(f'{table}: writing to: s3://{BUCKET_NAME}/etl_process=processed/table={table}')
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{BUCKET_NAME}/etl_process=processed/table={table}",
            index=False,
            dataset=True,
            mode=mode,
            catalog_versioning=True,
            schema_evolution=True,
            database=DATABASE,
            table=table,
            partition_cols=partition_cols
        )
    else:
        logger.info(f'{table}: no data to save')
    return