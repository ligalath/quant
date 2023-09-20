import datetime
import logging
# import Wind API
from WindPy import w
import pandas as pd
from Pandas2DataFeeds import STVPdData

class WindDataFetch:
    def __init__(self) -> None:
        w.start()
    
    def __del__(self) -> None:
        w.stop()
    
    def fetch_data(self, codes_set:set, from_date:datetime, to_date:datetime, fields:list) -> pd.DataFrame:
        if not w.isconnected():
            logging.error('wind not inited')
            return None
        # 任取一只国债010107.SH六月份以来的净值历史行情数据
        history_data=w.wsd("010107.SH",
                   "sec_name,ytm_b,volume,duration,convexity,open,high,low,close,vwap", 
                   "2018-06-01", "2018-06-11", "returnType=1;PriceAdj=CP", usedf=True) 
        # returnType表示到期收益率计算方法，PriceAdj表示债券价格类型‘
        print(history_data[1].head())
        for code in codes_set:
            data_frame = w.wsd(codes = code,fields=fields,beginTime=from_date, endTime=to_date,usedfdt =True)[1]
            print('dkjaskdjf')
            
        print(data_frame[1].head())
        return data_frame
    def fetch_codes(self,options):
        if not w.isconnected():
            logging.error('wind not inited')
            return None
        codes_set = list(pd.Series(w.wset("sectorconstituent",options).Data[1]))
        #TODO... 去除ST 
        return codes_set
        
        

    def read_csv(file_path):
        df = pd.read_csv(file_path)
        print(df)

    