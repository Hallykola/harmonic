import threading
from queue import Queue
from helpers.pricehelper import PriceHelper
import time

class PriceGetter(threading.Thread):
    def __init__(self,pair,analysis_work_queue5m,analysis_work_queue10m,analysis_work_queue15m,analysis_work_queue30m,analysis_work_queue1h):
        super().__init__()
        self.pair = pair
        self.analysis_work_queue5m = analysis_work_queue5m
        self.analysis_work_queue10m = analysis_work_queue10m
        self.analysis_work_queue15m = analysis_work_queue15m
        self.analysis_work_queue30m = analysis_work_queue30m
        self.analysis_work_queue1h = analysis_work_queue1h

    def run(self):
        pricehelper = PriceHelper(self.pair)
        
        while True:
            
            pricehelper.updateHistory(self.pair)
            
            ohlcv_5min, ohlcv_10min, ohlcv_15min,  ohlcv_30min,  ohlcv_1hr = pricehelper.df_from_existing_history("bybit",self.pair)
            df5 = ohlcv_5min.iloc[-500:].copy()
            df15 = ohlcv_15min.iloc[-500:].copy()
            df30 = ohlcv_30min.iloc[-500:].copy()
            df10 = ohlcv_10min.iloc[-500:].copy()
            df1 = ohlcv_1hr.iloc[-500:].copy()
            
            df5["pair"] = self.pair            
            df15["pair"] = self.pair
            df30["pair"] = self.pair
            df10["pair"] = self.pair
            df1["pair"] = self.pair  
                     
            self.analysis_work_queue5m.put(df5)
            self.analysis_work_queue10m.put(df10)
            self.analysis_work_queue15m.put(df15)
            self.analysis_work_queue30m.put(df30)

            self.analysis_work_queue1h.put(df1)
            time.sleep(120)
