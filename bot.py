from helpers.pricegetter_worker  import PriceGetter
from helpers.analysis_worker import AnalysisWorker
import json
from tradesettings import TradeSettings
import time
from queue import Queue


class Bot:
    def __init__(self):
        pass


    def loadSettings(self):
            with open("./settings.json","r") as f:
                self.settings = json.loads(f.read())
                self.tradeSettings  = {k:TradeSettings(k,v) for k,v in self.settings['trading_pairs'].items()}
                self.trade_risk  = self.settings['trade_risk']
                self.granularity  = self.settings['granularity']

    def runStreamer(self):
        self.loadSettings()
        threads = []
        analysis_work_queue5m = Queue()
        analysis_work_queue10m = Queue()
        analysis_work_queue15m = Queue()
        analysis_work_queue30m = Queue()
        analysis_work_queue1h = Queue()
        analysis_work_queues = [analysis_work_queue5m,analysis_work_queue10m,analysis_work_queue15m,analysis_work_queue30m,analysis_work_queue1h]
        timeframestags = ["5 mins","10 mins","15mins","30mins","1 hour"]
        for pair in self.tradeSettings.keys():
            price_processor_thread =  PriceGetter(pair,analysis_work_queue5m,analysis_work_queue10m,analysis_work_queue15m,analysis_work_queue30m,analysis_work_queue1h)
            price_processor_thread.daemon=True
            threads.append(price_processor_thread)
            price_processor_thread.start()

        for i,queue in enumerate(analysis_work_queues):
            analysis_worker_thread = AnalysisWorker(queue,timeframestags[i])
            analysis_worker_thread.daemon=True
            threads.append(price_processor_thread)
            analysis_worker_thread.start()

        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("KeyboardInterrupt")

        print("ALL DONE")
