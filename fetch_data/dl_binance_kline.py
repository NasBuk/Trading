import requests
import pandas as pd
import pickle
import datetime
import logging
import os
from time import sleep, time
import argparse
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_URL = 'https://api.binance.com/api/v3/klines'
DEFAULT_PAIR = 'BTCUSDT'
DEFAULT_INTERVAL = '1m'
DEFAULT_START_DATE = '2017-07-14'
RATE_LIMIT_SLEEP = 0.2
TIME_BEFORE_SAVE = 300  # Time before saving in seconds

def fetch_klines(pair, interval, start_time, end_time):
    """Fetches kline (candlestick) data from the Binance API."""
    url = API_URL
    params = {
        'symbol': pair,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': 1000
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        klines = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching klines: {e}')
        return None
    return klines

def save_data(df, filename):
    """Saves DataFrame to a pickle file."""
    with open(filename, 'wb') as file:
        pickle.dump(df, file)
    logging.info(f'Data saved to {filename}')

def load_data(filename):
    """Loads DataFrame from a pickle file."""
    with open(filename, 'rb') as file:
        return pickle.load(file)

def get_initial_start_time(filename, start_date):
    """Gets the initial start time based on existing data or a default start date."""
    if os.path.exists(filename):
        logging.info('Data file exists, loading last row to get the latest timestamp...')
        with open(filename, 'rb') as file:
            last_row = pd.read_pickle(file).tail(1)
        last_timestamp = pd.Timestamp(last_row['Open time'].values[0]).timestamp() * 1000
        start_time = int(last_timestamp + 60000)
    else:
        logging.info('Data file does not exist, downloading all available historical data...')
        start_time = int(datetime.datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
    return start_time

def convert_data_types(df):
    """Convert data types of specific columns."""
    df['Open time'] = pd.to_datetime(df['Open time'], unit='ms')
    df['Close time'] = pd.to_datetime(df['Close time'], unit='ms')
    df['Open'] = df['Open'].astype(float)
    df['High'] = df['High'].astype(float)
    df['Low'] = df['Low'].astype(float)
    df['Close'] = df['Close'].astype(float)
    df['Volume'] = df['Volume'].astype(float)
    df['Quote asset volume'] = df['Quote asset volume'].astype(float)
    df['Taker buy base asset volume'] = df['Taker buy base asset volume'].astype(float)
    df['Taker buy quote asset volume'] = df['Taker buy quote asset volume'].astype(float)
    return df

def main(pair, interval, start_date):
    """Main function to fetch, process, and save kline data."""
    filename = f'{pair}_{interval}_Binance.pkl'
    start_time = get_initial_start_time(filename, start_date)
    end_time = int(datetime.datetime.now().timestamp() * 1000)

    # Print main parameters
    print(f"\n{'='*40}\nStarting Data Fetch\n{'='*40}")
    print(f"Currency Pair: {pair}")
    print(f"Interval: {interval}")
    print(f"Start Date: {datetime.datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Date: {datetime.datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output File: {filename}")
    print(f"Rate Limit Sleep: {RATE_LIMIT_SLEEP} seconds")
    print(f"Save Data Every: {TIME_BEFORE_SAVE} seconds\n{'='*40}\n")

    all_klines = []
    start_loop_time = time()

    # Estimate total number of requests
    total_requests = (end_time - start_time) // (1000 * 60 * 1000) + 1

    try:
        with tqdm(total=total_requests, desc="Fetching klines") as pbar:
            while start_time < end_time:
                klines = fetch_klines(pair, interval, start_time, end_time)
                if klines is None:
                    logging.error('Failed to fetch klines, exiting.')
                    break

                if len(klines) == 0:
                    break

                all_klines.extend(klines)
                start_time = klines[-1][0] + 60000
                pbar.update(1)
                
                sleep(RATE_LIMIT_SLEEP)  # adhere to Binance's API rate limit requirements

                # Check if the save interval has been exceeded
                if (time() - start_loop_time) >= TIME_BEFORE_SAVE:
                    new_df = pd.DataFrame(all_klines, columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
                                                               'Close time', 'Quote asset volume', 'Number of trades',
                                                               'Taker buy base asset volume', 'Taker buy quote asset volume',
                                                               'Ignore'])

                    # Convert data types of the new data
                    new_df = convert_data_types(new_df)

                    if os.path.exists(filename):
                        df = load_data(filename)
                        df = pd.concat([df, new_df], ignore_index=True)
                    else:
                        df = new_df

                    save_data(df, filename)
                    del df
                    all_klines = []  # reset buffer
                    start_loop_time = time()  # reset the timer

    except KeyboardInterrupt:
        logging.warning('KeyboardInterrupt received, saving progress...')
    except Exception as e:
        logging.error(f'An error occurred: {e}')

    # Save any remaining klines
    if len(all_klines) > 0:
        new_df = pd.DataFrame(all_klines, columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
                                                   'Close time', 'Quote asset volume', 'Number of trades',
                                                   'Taker buy base asset volume', 'Taker buy quote asset volume',
                                                   'Ignore'])

        # Convert data types of the new data
        new_df = convert_data_types(new_df)

        if os.path.exists(filename):
            df = load_data(filename)
            df = pd.concat([df, new_df], ignore_index=True)
        else:
            df = new_df

        save_data(df, filename)
        del df

    df = load_data(filename)
    print(df[['Open time', 'Open', 'High', 'Low', 'Close']].head())
    print(df[['Open time', 'Open', 'High', 'Low', 'Close']].tail())

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch kline (candlestick) data from Binance API.')
    parser.add_argument('--pair', type=str, default=DEFAULT_PAIR, help='Currency pair to fetch (e.g., BTCUSDT)')
    parser.add_argument('--interval', type=str, default=DEFAULT_INTERVAL, help='Interval for klines (e.g., 1m)')
    parser.add_argument('--start_date', type=str, default=DEFAULT_START_DATE, help='Start date for data (e.g., 2021-01-01)')
    
    args = parser.parse_args()
    
    main(args.pair, args.interval, args.start_date)
