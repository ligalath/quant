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
    
    '''
    @return 
    {
        "SH_00213":DataFrame,
        "SH_00213":DataFrame,
    }
    DataFrame format:
                 OPEN  CLOSE   HIGH    LOW   PCT_CHG      TURN    datetime
    2023-02-23  14.00  14.05  14.32  13.98  0.213980  0.424874  2023-02-23
    2023-02-24  14.02  13.86  14.03  13.80 -1.352313  0.376175  2023-02-24
    2023-02-27  13.75  13.69  13.88  13.68 -1.226551  0.320250  2023-02-27
    2023-03-01  13.80  14.17  14.19  13.74  2.830189  0.630465  2023-03-01
    '''
    def fetch_data(self, codes_set:set, from_date:datetime, to_date:datetime, fields:list) -> pd.DataFrame:
        if not w.isconnected():
            logging.error('wind not inited')
            return None
        histroy_data = {}
        for code in codes_set:
            data_frame = w.wsd(codes = code,fields=fields,beginTime=from_date, endTime=to_date,usedfdt =True)[1]
            data_frame['datetime'] = data_frame.index
            histroy_data[code] = data_frame
            print(data_frame)
            
        return histroy_data 
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

    