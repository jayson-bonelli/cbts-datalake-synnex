from utilities import get_api_keys, DATABASE, save_to_datalake, WORKGROUP
import os
import json
import awswrangler as wr
import requests
from datetime import datetime
import pytz


def get_po_number(event={}, context={}):
    logger.info(json.dumps(event))
    to_return = event
    query = f"select po_number from v_synnex_po_numbers';"
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


def get_po_data(event={}, context={}):
    logger.info(jsondumps(event))

    data_to_save = []


url = "https://ec.synnex.com/SynnexXML/POStatus"

payload = """<?xml version="'1.0'" encoding="'UTF-8'" ?>
<SynnexB2B version="2.7">
    <Credential>
        <UserID>cbts.businessintelligence@cinbell.com</UserID>
        <Password>!rKM0za6OOvAH</Password>
    </Credential>
    <OrderStatusRequest>
        <CustomerNumber>146357</CustomerNumber>
        <PONumber>SCM-PO-000402</PONumber>
    </OrderStatusRequest>
</SynnexB2B>"""

headers = {
    'Authorization': 'Basic Y2J0cy5idXNpbmVzc2ludGVsbGlnZW5jZUBjaW5iZWxsLmNvbTohcktNMHphNk9PdkFI',
    'Content-Type': 'application/xml',
    'Cookie': 'JSESSIONID=kMupFwI3MuNY3oShD6LvtU1X.ec_jb6_node2; cookiesession1=678A3E29NOPQRSTUVWYKLMNOPQRSF062'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
