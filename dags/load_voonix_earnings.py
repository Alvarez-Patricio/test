import requests
import json
import pandas as pd
import time
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from google.cloud import storage
from google.cloud import bigquery
from datetime import date, datetime, timedelta


default_args = {
    'owner': 'Federico',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1,0),
    #'schedule_interval': '0 1 * * *',
    'email': ['data@leadhousenetwork.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 1
}



@dag(default_args=default_args,
    schedule='0 */1 * * *',
    catchup=False)

def load_voonix_earnings():

    @task()
    def extract():
        VOONIX_API_KEY = 'c0c365639ef25890ec889e1eb9961accea78da84'
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ##end_date = datetime.now().strftime("%Y-%m-%d")
        ##start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        ##start_date = '2025-01-01'
        ##end_date = '2025-12-31'

        today = date.today()
        year, month = 2026, 1
        ranges = []

        while date(year, month, 1) <= today:
            start = date(year, month, 1)
            end = date(year + (month // 12), (month % 12) + 1, 1) - timedelta(days=1)
            if end > today:
                end = today
            ranges.append([start.isoformat(), end.isoformat()])
            month += 1
            if month > 12:
                month = 1
                year += 1

        blob_names =  []
        for range in ranges:
            try:

                start_date = range[0]
                end_date = range[1]


                url = f"https://leadhouse.voonix.net//api/?report=advertiserearnings&list&start={start_date}&end={end_date}&breakdown_period=daily&breakdown_level=campaign&key={VOONIX_API_KEY}"
                response = requests.request("GET", url)
                data = response.json()

                storage_client = storage.Client()
                bucket = storage_client.bucket('leadhouse-voonix')
                blob_name = f'voonix/raw/earnings/{start_time}{start_date}{end_date}/response.txt'
                blob = bucket.blob(blob_name)
                blob.upload_from_string(json.dumps(data))
                blob_names.append(blob_name)

            except:
                pass

        return blob_names

    @task()
    def transform_and_load(blob_names):

        for blob_name in blob_names:

            storage_client = storage.Client()
            bucket = storage_client.bucket('leadhouse-voonix')
            blob = bucket.blob(blob_name)
            extract_time = time.strftime("%Y-%m-%d %H:%M:%S")

            with blob.open("r") as f:
                file = f.read()
                data = json.loads(file)

                jason_earnings = data['data']
                advertiser_earnings = []
                for earning_key, values in jason_earnings.items():
                    for advertiser_id, value1 in values.items():
                        for login_id, value2 in value1.items():
                            for campaign_key, value3 in value2.items():
                                for earning_date, earning in value3.items():
                                    if isinstance(earning, dict):
                                        earning.update(
                                        {'advertiser_id': advertiser_id,
                                         'login_id': login_id,
                                         'campaign_key': campaign_key,
                                         'earning_date': earning_date})
                                        advertiser_earnings.append(earning)
                advertiser_earnings_df = pd.DataFrame.from_dict(advertiser_earnings)
                advertiser_earnings_df['timestamp'] = extract_time
                advertiser_earnings_df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
                advertiser_earnings_df.replace(',', '', regex=True, inplace=True)



            # advertiser_earnings_df = advertiser_earnings_df[['clicks', 'signups', 'active_players', 'deposits', 'FTD', 'qndc',
            #                                                  'ndc', 'CPA_count', 'unique_clicks', 'depositors',  'deposit_value',
            #                                                  'raw_deposit_value', 'REV_income', 'raw_REV_income', 'bonus',
            #                                                  'raw_bonus', 'netrevenue', 'raw_netrevenue', 'turnover', 'raw_turnover',
            #                                                  'gross_revenue', 'raw_gross_revenue', 'CPA_income', 'raw_CPA_income',
            #                                                  'CPL_income', 'raw_CPL_income', 'Extra_fee', 'raw_Extra_fee',
            #                                                  'advertiser_id', 'login_id', 'campaign_key', 'earning_date', 'timestamp']]

                advertiser_earnings_df['Extra_fee'] = advertiser_earnings_df['Extra_fee'].astype(float)
                advertiser_earnings_df['raw_Extra_fee'] = advertiser_earnings_df['raw_Extra_fee'].astype(float)
                advertiser_earnings_df['advertiser_id'] = advertiser_earnings_df['advertiser_id'].astype(int)
                advertiser_earnings_df['login_id'] = advertiser_earnings_df['login_id'].astype(int)
                advertiser_earnings_df['campaign_key'] = advertiser_earnings_df['campaign_key'].astype(str)
                advertiser_earnings_df['custom_column1'] = advertiser_earnings_df['custom_column1'].astype(str)
                advertiser_earnings_df['custom_column2'] = advertiser_earnings_df['custom_column2'].astype(str)
                advertiser_earnings_df['custom_column3'] = advertiser_earnings_df['custom_column3'].astype(str)
                advertiser_earnings_df['custom_column4'] = advertiser_earnings_df['custom_column4'].astype(str)
                advertiser_earnings_df['custom_column5'] = advertiser_earnings_df['custom_column5'].astype(str)
                advertiser_earnings_df['custom_column6'] = advertiser_earnings_df['custom_column6'].astype(str)
                advertiser_earnings_df['custom_column7'] = advertiser_earnings_df['custom_column7'].astype(str)
                advertiser_earnings_df['custom_column8'] = advertiser_earnings_df['custom_column8'].astype(str)
                advertiser_earnings_df['custom_column9'] = advertiser_earnings_df['custom_column9'].astype(str)
                advertiser_earnings_df['custom_column10'] = advertiser_earnings_df['custom_column10'].astype(str)
                advertiser_earnings_df['earning_date'] = advertiser_earnings_df['earning_date'].astype('datetime64[ns]')
                advertiser_earnings_df['timestamp'] = advertiser_earnings_df['timestamp'].astype('datetime64[ns]')
                advertiser_earnings_df = advertiser_earnings_df.add_prefix('voonix_')
                advertiser_earnings_df.columns = advertiser_earnings_df.columns.str.lower()

                client = bigquery.Client(project='data-421208')
                dataset_ref = client.dataset('voonix')
                table_ref = dataset_ref.table('load_voonix_earnings')
                client.load_table_from_dataframe(advertiser_earnings_df, table_ref).result()

    transform_and_load(extract())

#
load_voonix_earnings = load_voonix_earnings()