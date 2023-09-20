import backtrader as bt

import pandas

class STVPdData(bt.feeds.PandasData):
    #除open high low close volume openinterest外的列名在此声明
    lines = ('turnover', )
    #
    params = (
        ('datetime', -1),
        ('open', -1),
        ('high', -1),
        ('low', -1),
        ('close', -1),
        ('volume', -1),
        ('openinterest', -1),
        ('turnover', -1)
    )