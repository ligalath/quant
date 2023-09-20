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
        data_frame = w.wsd(codes = codes_set,fields=fields,beginTime=from_date, endTime=to_date,usedfdt = True )
        return data_frame
    def fetch_codes(options):
        if not w.isconnected():
            logging.error('wind not inited')
            return None
        codes_set = list(pd.Series(w.wset("sectorconstituent",options).Data[1]))
        #TODO... 去除ST 
        return codes_set
        
        

    def read_csv(file_path):
        df = pd.read_csv(file_path)
        print(df)

    