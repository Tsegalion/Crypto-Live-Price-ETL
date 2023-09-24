import requests
import pandas as pd
from datetime import datetime
from pandas_gbq import to_gbq
import logging

# Configure the logging settings
log_file = "script.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")

# CoinCap API Key
api_key = 'COIN_CAP_API'

# Project ID
project_id = 'crypto-pipeline-398422'

def fetch_data(api_key):
    try:
        baseurl = 'https://api.coincap.io/v2/'
        endpoint = 'assets'
        
        # Include API key in the request headers
        headers = {
            'Authorization': f'Bearer {api_key}'
        }
        
        price_data = requests.get(baseurl + endpoint, headers=headers)
        price_data.raise_for_status()  # Raise an exception for HTTP errors
        return price_data.json()['data']
    
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        raise e

def extract_data(asset):
    try:
        currency = asset['name']
        date_time = datetime.now()
        total_supply = asset['supply']
        volumeUsd24Hr = asset['volumeUsd24Hr']
        price = asset['priceUsd']
        changePercent24Hr = asset['changePercent24Hr']

        return {
            'currency' : currency,
            'date_time' : date_time,
            'total_supply' : total_supply,
            'volumeUsd24Hr' : volumeUsd24Hr,
            'price' : price,
            'changePercent24Hr' : changePercent24Hr
        }
    except Exception as e:
        logging.error(f"Error extracting data: {str(e)}")
        raise e

def call():
    try:
        data = fetch_data(api_key)
        coin = [extract_data(asset) for asset in data]
        df = pd.DataFrame(coin)
        return df
    except Exception as e:
        logging.error(f"Error in data processing: {str(e)}")
        raise e

def main():
    try:
        asset_update = call()

        # Changing datatypes
        columns_to_convert = ['total_supply', 'volumeUsd24Hr', 'price', 'changePercent24Hr']
        asset_update[columns_to_convert] = asset_update[columns_to_convert].astype(float)

        # Loading the BigQuery
        asset_update.to_gbq('crypto-pipeline-398422.asset_updates.assets', project_id, if_exists='append')
        
        logging.info("Data loaded to BigQuery successfully")
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()
