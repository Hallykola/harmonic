import os
import requests
import json
import sys
from time import sleep
from datetime import datetime

from datetime import datetime
import pytz

import os
import pandas as pd
import json
import re


class PriceHelper:
    def __init__(self,pair):
        self.pair = pair

    def getPrice(self):
        pass

    def history_file_name(self, index, exchange, symbol):
        """
        Generates a filename for a history file based on index, exchange, and symbol.
        Args:
            index (int): The index of the history file.
            exchange (str): The exchange name (e.g., 'bybit', 'bitstamp').
            symbol (str): The trading symbol (e.g., 'BTCUSD', 'ETHUSD')

        Returns:
            str: The formatted filename.
        """
        return f'history/{exchange}/{symbol}_M1_{index}.json'

    def is_future_time(self, start_time, milli):
        """
        Checks if the given time is in the future compared to the current time (minus two latest minutes).

        Args:
            start_time: The time to be checked, in seconds or milliseconds.

        Returns:
            True if the time is in the future, False otherwise.
        """
        lagos_tz = pytz.timezone('Africa/Lagos')

        if milli:
            time_now = int(datetime.now().timestamp()*1000) - 120000
        else:
            time_now = int(datetime.now().timestamp()) - 120

        return start_time > time_now

    def load_existing_history(self, exchange, symbol):
        """
        Loads existing history data from files.
        Args:
            exchange (str): The exchange name.
            symbol (str): The trading symbol.

        Returns:
            list: Loaded history data or an empty list if no file found.
        """
        loadedData = []
        directory = f'history/{exchange}/'
        cnt = 0

        try:
            # Get a list of all files in the specified directory
            file_list = os.listdir(directory)

            # Define a custom sorting function
            def numericalSort(value):
                parts = re.split(r'(\d+)', value)
                parts[1::2] = map(int, parts[1::2])
                return parts

            # Sort the filenames numerically
            sorted_files = sorted(file_list, key=numericalSort)

            # cnt = 0
            for filename in sorted_files:
                if filename.startswith(f'{symbol}_M1_') and filename.endswith('.json'):
                    # Load data from each valid history file
                    with open(os.path.join(directory, filename), 'r') as file:
                        file_data = json.load(file)
                        if len(file_data)>0:
                            loadedData += file_data
                            cnt +=1

            if not loadedData:
                print(f"No history files found for {exchange} - {symbol}. Starting fresh.")

        except FileNotFoundError:
            print(f"Directory {directory} not found.")

        return [loadedData, cnt]

    def download_history(self, exchange, symbol,limit,milli,urls,batch_size,jump,start):
        acc_data, nmb_files = self.load_existing_history(exchange, symbol)

        while True:
            # define start and end time of the next dataset which will be pulled
            if len(acc_data) > 0 and not jump:
                if exchange in ['bybit','bybit-linear','okx','bitfinex']:
                    latest_timestamp = int(acc_data[-1][0])
                    start = latest_timestamp
                    print(datetime.fromtimestamp(latest_timestamp/1000))
                elif exchange == "bitstamp":
                    latest_timestamp = int(acc_data[-1]["timestamp"])
                    start = latest_timestamp
                    print(datetime.fromtimestamp(latest_timestamp))
                elif exchange in ['kucoin-spot', 'kucoin-futures']:
                    latest_timestamp = int(acc_data[-1][0])
                    start = latest_timestamp
                    print(datetime.fromtimestamp(latest_timestamp))
                else:
                    print("Could not update the latest timestamp.")
                    break
                # check if we reached present time
            if self.is_future_time(start, milli):
                print("All price data pulled. Good luck!")
                break
            end = start + limit * 60 * 1000 if milli else start + limit * 60
            if jump:
                print("Seems like there is no data available for this period. Trying to jump.")
                start = end
                end = start + limit * 60 * 1000 if milli else start + limit * 60
                jump = False
            # construct final url
            if exchange in ['kucoin-spot', 'kucoin-futures']:
                url = urls[exchange] + "&startAt=" + str(start) + "&endAt=" + str(end)
            elif exchange == "okx":
                url = urls[exchange] + "&after=" + str(end)
            else:
                url = urls[exchange] + "&start=" + str(start) + "&end=" + str(end)
            print(url + " __ " + str(len(acc_data)))

            # send request
            r = requests.get(url=url)
            if r.status_code != 200:
                if r.status_code == 429:
                    print("Too many requests. Stopping for now.")
                else:
                    print("Something went wrong. I am done.")
                break

            # extract data from request in json format
            if exchange in["bybit", "bybit-linear"]:
                data = r.json()["result"]['list']
                data.reverse()
                package_complete = len(data) >= limit-1
                if len(acc_data)>0 and len(data) > 0 and data[0][0] == acc_data[-1][0]:
                    print("removed duplicate timestamp: " + str(data[0][0]))
                    data = data[1:]
            elif exchange == "bitstamp":
                data = r.json()["data"]["ohlc"]
                package_complete = len(data) >= limit
                if len(acc_data)>0 and len(data) > 0 and data[0]['timestamp'] == acc_data[-1]['timestamp']:
                    print("removed duplicate timestamp: " + str(data[0]['timestamp']))
                    data = data[1:]
            elif exchange in ['kucoin-spot', 'kucoin-futures', 'okx']:
                data = r.json()['data']
                data.reverse()
                package_complete = len(data) >= limit - 1
                if len(acc_data) > 0 and len(data) > 0 and data[0][0] == acc_data[-1][0]:
                    print("removed duplicate timestamp: " + str(data[0][0]))
                    data = data[1:]
            elif exchange in ['bitfinex']:
                data = r.json()
                package_complete = len(data) >= limit - 1
                if len(acc_data) > 0 and len(data) > 0 and data[0][0] == acc_data[-1][0]:
                    print("removed duplicate timestamp: " + str(data[0][0]))
                    data = data[1:]
            else:
                break
            # merge data
            if len(data) == 0:
                jump = True
            else:
                acc_data += data

            # write to file
            next_file = (len(acc_data) > batch_size and nmb_files == 0) or (len(acc_data) > batch_size * nmb_files and nmb_files > 0)
            if next_file or not package_complete:
                idx = nmb_files - 1
                while idx < nmb_files + 1:
                    if idx >= 0:
                        file_path = self.history_file_name(idx, exchange, symbol)
                        with open(file_path, 'w') as file:
                            content = acc_data[idx * batch_size:(idx + 1) * batch_size]
                            json.dump(content, file)
                            print("wrote to file " + str(idx))
                    idx += 1

            if next_file:
                nmb_files += 1
            if not package_complete:
                print('Received less data than expected: ' + str(len(data)) + ' entries')
                print("Short break. Will continue shortly after.")
                sleep(1)
    
    def df_from_existing_history(self, exchange, symbol):

        # Directory containing JSON files
        directory = f'history/{exchange}/'

        # List to hold dataframes
        dfs = []

        # Get a list of all files in the specified directory
        file_list = os.listdir(directory)

        # Define a custom sorting function
        def numericalSort(value):
            parts = re.split(r'(\d+)', value)
            parts[1::2] = map(int, parts[1::2])
            return parts

        # Sort the filenames numerically
        sorted_files = sorted(file_list, key=numericalSort)

        for filename in sorted_files:
            if filename.startswith(f'{symbol}_M1_') and filename.endswith('.json'):
                # Load data from each valid history file
                with open(os.path.join(directory, filename), 'r') as file:
                    df = pd.read_json(os.path.join(directory, filename))
                    dfs.append(df)


        # Concatenate all dataframes into one
        final_df = pd.concat(dfs, ignore_index=True)
        # Rename columns for clarity
        final_df.columns = ['startTime', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        final_df = final_df.dropna()

        final_df

        my_df= final_df.copy()
        my_df['startTime'] = pd.to_datetime(my_df['startTime'])

        # Ensure the DataFrame is sorted
        my_df = my_df.sort_values(by='startTime')

        # Set the startTime column as the index
        my_df = my_df.set_index('startTime')

        # Create a full range of startTimes with hourly frequency
        full_range = pd.date_range(start=my_df.index.min(), end=my_df.index.max(), freq='min')

        # Reindex the DataFrame to this full range
        my_df_reindexed = my_df.reindex(full_range)

        # Identify missing startTimes
        missing_startTimes = my_df_reindexed[my_df_reindexed.isna().any(axis=1)].index

        print("Missing Timestamps:")
        print(missing_startTimes)


        # Convert 'startTime' column to datetime and set it as index
        final_df['startTime'] = pd.to_datetime(final_df['startTime'], unit='ms') # Assuming 'startTime' is in milliseconds
        final_df.set_index('startTime', inplace=True)

        ohlcv_15min = final_df.resample('15min').agg({
            'open': 'first',     # First value in the 30-minute interval
            'high': 'max',       # Maximum value in the 30-minute interval
            'low': 'min',        # Minimum value in the 30-minute interval
            'close': 'last',     # Last value in the 30-minute interval
            'volume': 'sum'      # Sum of the volumes in the 30-minute interval
        })
        ohlcv_5min = final_df.resample('5min').agg({
            'open': 'first',     # First value in the 30-minute interval
            'high': 'max',       # Maximum value in the 30-minute interval
            'low': 'min',        # Minimum value in the 30-minute interval
            'close': 'last',     # Last value in the 30-minute interval
            'volume': 'sum'      # Sum of the volumes in the 30-minute interval
        })
        ohlcv_10min = final_df.resample('10min').agg({
            'open': 'first',     # First value in the 30-minute interval
            'high': 'max',       # Maximum value in the 30-minute interval
            'low': 'min',        # Minimum value in the 30-minute interval
            'close': 'last',     # Last value in the 30-minute interval
            'volume': 'sum'      # Sum of the volumes in the 30-minute interval
        })
        ohlcv_30min = final_df.resample('30min').agg({
            'open': 'first',     # First value in the 30-minute interval
            'high': 'max',       # Maximum value in the 30-minute interval
            'low': 'min',        # Minimum value in the 30-minute interval
            'close': 'last',     # Last value in the 30-minute interval
            'volume': 'sum'      # Sum of the volumes in the 30-minute interval
        })

        ohlcv_1hr = final_df.resample('1h').agg({
            'open': 'first',     # First value in the 30-minute interval
            'high': 'max',       # Maximum value in the 30-minute interval
            'low': 'min',        # Minimum value in the 30-minute interval
            'close': 'last',     # Last value in the 30-minute interval
            'volume': 'sum'      # Sum of the volumes in the 30-minute interval
        })
        return  ohlcv_5min, ohlcv_10min, ohlcv_15min,  ohlcv_30min,  ohlcv_1hr
    
    def updateHistory(self, asymbol):
        exclude_current_candle = True
        exchange = 'bybit'
        symbol = asymbol
        batch_size = 50000
        acc_data = []
        milli = True if exchange in ['bybit','bybit-linear', 'okx', 'bitfinex'] else False
        jump = False
        interval=1 #done for bybit only


        try:
            os.makedirs('history/'+exchange)
        except Exception:
            pass

        limits = {
            "bybit": 1000,
            "bybit-linear": 1000,
            "bitstamp": 1000,
            "kucoin-spot": 1500,
            "kucoin-futures": 1500,
            "okx": 100,
            "bitfinex": 10000
        }

        limit = limits[exchange]
        x_timestamp = datetime(2024, 12, 2) #datetime(2025, 1, 22) #
        epoch = datetime(1970, 1, 1)
        x_timestamp_millis = int((x_timestamp - epoch).total_seconds() * 1000)
        x_timestamp_millis

        if exchange == 'bybit':
            if symbol == 'ETHUSD':
                start = 1548633600000
            elif symbol == 'BTCUSD':
                start = 1542502800000
            elif symbol == symbol:
                start = x_timestamp_millis
            else:
                sys.exit("symbol not found")
        elif exchange == "bitstamp":
            if symbol == "btcusd":
                start = 1322312400
            else:
                sys.exit("symbol not found")
        elif exchange == "kucoin-spot":
            if symbol == "BTC-USDT":
                start = 1508720400
            else:
                sys.exit("symbol not found")
        elif exchange == "okx":
            if symbol == "BTC-USDT":
                start = 1534294800000
            else:
                sys.exit("symbol not found")
        elif exchange == "bitfinex":
            if symbol == "BTCUSD":
                start = 1372467600000
            else:
                sys.exit("symbol not found")
        else:
            sys.exit("exchange not found")

        urls = {
            "bybit": f"https://api.varsityreghub.com/prices.php?category=inverse&symbol={symbol}&interval={interval}&limit={limit}",
            "bybit-linear": f"https://api.bybit.com/v5/market/mark-price-kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}",
            "bitstamp": f"https://www.bitstamp.net/api/v2/ohlc/{symbol}/?step=60&limit={limit}&exclude_current_candle={exclude_current_candle}",
            "kucoin-spot": f"https://api.kucoin.com/api/v1/market/candles?type=1min&symbol={symbol}",
            "kucoin-futures": f"https://api-futures.kucoin.com/api/v1/market/candles?type=1min&symbol={symbol}",
            "okx": f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&limit={limit}",
            "bitfinex": f"https://api-pub.bitfinex.com/v2/candles/trade:1m:t{symbol}/hist?sort=1&limit={limit}"
        }
        #https://api.bybit.com/v5/market/kline

        self.download_history(exchange, symbol,limit,milli,urls,batch_size,jump,start)