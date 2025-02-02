import threading
from queue import Queue
from helpers.patternfinder import PatternFinder
import time
import pandas as pd
# from playsound import playsound
from preferredsoundplayer import *

class AnalysisWorker(threading.Thread):
    def __init__(self,analysis_work_queue,timeframe):
        super().__init__()
        # self.pair = pair
        self.analysis_work_queue = analysis_work_queue
        self.tag = timeframe
    
    def analyse_df(self,df):
        pf = PatternFinder()
        tag = self.tag
        found_recent = pf.show_recent_patterns(df,tag)
        
        if found_recent:
            # playsound('BEEP_AND_KICK_k10.wav')
            soundplay("BEEP_AND_KICK_k10.wav")



    def run(self):
       
        while True:
            df:pd.DataFrame  = self.analysis_work_queue.get()
            # print("Na df  b this o!")
            # print(df)
            self.analyse_df(df)
            # print("TradeWorker : ", trade_decision)