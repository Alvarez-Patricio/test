import requests
import json
import pandas as pd
import time
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, timedelta



default_args = {
    'owner': 'Federico',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'schedule': '@hourly',
    'from_email'= ['data@leadhousenetwork.com'],
    'email': ['leadhouse-notificatio-aaaatzzmvwebecucpvrxz3n4me@daxers.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    #'catchup': False,
    #'retry_delay': timedelta(minutes=1),
    #'retries': 1
}


@dag(default_args=default_args,
     schedule='0 */1 * * *',
     catchup=False)
def load_voonix_campaigns():

    @task()
    def extract():
        VOONIX_API_KEY = 'c0c365639ef25890ec889e1eb9961accea78da84'
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        url = f"https://leadhouse.voonix.net//api/?report=campaigns&list&key={VOONIX_API_KEY}"
        response = requests.request("GET", url)
        data = response.json()

        storage_client = storage.Client()
        bucket = storage_client.bucket('leadhouse-voonix')
        blob_name = f'voonix/raw/campaigns/{start_time}/response.txt'
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data))

        return blob_name

    @task()
    def transform_and_load(blob_name):

        storage_client = storage.Client()
        bucket = storage_client.bucket('leadhouse-voonix')
        blob = bucket.blob(blob_name)
        extract_time = time.strftime("%Y-%m-%d %H:%M:%S")

        with blob.open("r") as f:
            file = f.read()
            data = json.loads(file)

            json_data = data['logins']
            campaigns = []

            for login_id, login_values in json_data.items():
                advertiser_meta = login_values['advertiser_meta']
                advertiser_id = advertiser_meta['advertiser_id']
                advertiser_name = advertiser_meta['advertiser_name']
                login_meta = login_values['login_meta']
                login_id = login_meta['login_id']
                username = login_meta['username']

                for campaign_id, campaign_values in login_values['campaigns'].items():
                    site_meta = campaign_values['site_meta']
                    campaign_values['site_id'] = site_meta['site_id']
                    campaign_values['site_name'] = site_meta['site_name']

                    campaign_values['login_id'] = login_id
                    campaign_values['username'] = username
                    campaign_values['advertiser_id'] = advertiser_id
                    campaign_values['advertiser_name'] = advertiser_name
                    campaigns.append(campaign_values)

            campaigns_df = pd.DataFrame.from_dict(campaigns)
            campaigns_df.drop('site_meta', axis=1, inplace=True)
            # campaigns_df.drop('campaign_deals', axis=1, inplace=True)
            campaigns_df['timestamp'] = extract_time
            campaigns_df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
            campaigns_df.replace(',', '', regex=True, inplace=True)
            campaigns_df.rename(columns={'id': 'campaign_id'}, inplace=True)

            campaigns_df = campaigns_df[
                ['campaign_id', 'key', 'name', 'alias', 'group', 'note', 'site_id', 'site_name', 'login_id', 'username',
                 'advertiser_id', 'advertiser_name', 'timestamp']]

            campaigns_df = campaigns_df.add_prefix('voonix_')
            campaigns_df.columns = campaigns_df.columns.str.lower()

            client = bigquery.Client(project='data-421208')
            dataset_ref = client.dataset('voonix')
            table_ref = dataset_ref.table('load_voonix_campaigns')
            client.load_table_from_dataframe(campaigns_df, table_ref).result()

    transform_and_load(extract())

load_voonix_campaigns = load_voonix_campaigns()
