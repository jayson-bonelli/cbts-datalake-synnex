from utils import get_api_keys, DATABASE, save_to_datalake, WORKGROUP
import os
import json
import awswrangler as wr
import requests
from datetime import datetime
import pytz
import boto3

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

tz = pytz.timezone("US/Eastern")
client = boto3.client('ssm')


def get_po_number(event={}, context={}):
    logger.info(json.dumps(event))
    to_return = event
    po_number = event['po_number']
    query = f"select '{po_number}=po_number from v_synnex_po_numbers';"
    df = wr.athena.read_sql_query(query, database=DATABASE, ctas_approach=False,
                                  workgroup=WORKGROUP)
    records = json.loads(df.to_json(orient='records'))

    po_numbers = []

    for record in records:
        po_numbers.append({
            "po_number": record['po_number']
        })

    to_return['po_number'] = po_numbers

    return to_return


def get_po_status(event={}, context={}):
    logger.info(json.dumps(event))
    parameter = client.get_parameter(Name='/synnex/dev/credentials', WithDecryption=True)
    params = json.loads(parameter['Parameter']['Value'])
    user_id = params['User_Id']
    password = params['Password']
    customer_number = params['CustomerNumber']
    po_number = get_po_number()

    url = "https://testec.synnex.com/SynnexXML/POStatus"

    payload = f"""<?xml version="1.0" encoding="UTF-8" ?>
<SynnexB2B version="2.7">
    <Credential>
        <UserID>{user_id}</UserID>
        <Password>{password}</Password>
    </Credential>
    <OrderStatusRequest>
        <CustomerNumber>{customer_number}</CustomerNumber>
        <PONumber>'{po_number}'</PONumber>
    </OrderStatusRequest>
</SynnexB2B>"""

    headers = {
        'Authorization': 'Basic {API_KEY}',
        'Content-Type': 'application/xml',
        'Cookie': 'JSESSIONID=kMupFwI3MuNY3oShD6LvtU1X.ec_jb6_node2; cookiesession1=678A3E29NOPQRSTUVWYKLMNOPQRSF062'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    logger.info(response.status_code)

    if response.status_code !=200:
        raise requests.HTTPError(f'{response.status_code}: {response.text}')
    elif response.status_code == 200 and len(response.json()) > 0:

        data = response.json()

    if len(data) > 0:
        enriched_data = []
        processed_year = datetime.now(tz).strftime('%Y')
        processed_month = datetime.now(tz).strftime('%M')

        for e in data:
            e['processed_year'] = processed_year
            e['processed_month'] = processed_month
            enriched_data.append(e)
        data = enriched_data

    save_response = save_to_datalake(table='po_status', data=data, mode='overwrite', partition_cols=['processed_year', 'processed_month'])

    return save_response



