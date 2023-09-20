'''
凸显性因子策略
'''
import backtrader as bt
import pandas as pd
from datetime import *
'''
month_data: code|datetime  | open |close | high | low  | volume/K | openinterest/% |turnover/%
            0011|2022-01-01| 12.3 | 11.3 | 15.2 | 9.3  | 123.1    |  0.09          | 0.12
stock_data:
'''
class Sailence:
    def __init__(self, month_data:pd.DataFrame):
        self.month_data = month_data
        self.month_data["sailence"] = 0
        self.month_data["sailence_weight"] = 0
        self.sailence_weight = dict()
        #sailence: {"code1" : sailence, "code2":sailence2...}
        self.sailence_factor = None
        self.sailence_twist = 0.7
        self.period = 30
    #data preprocess before calculate sailence
    def preprocess(self):
        #insert column of daily_yield
        self.month_data.insert(self.month_data.shape[1], "daily_yield", 0)
        #calculate daily yield
        for i in len(self.month_data):
            data_entry = self.month_data.iloc[i]
            open = data_entry["open"]
            close = data_entry["close"]
            data_entry["daily_yield"] = (close - open)/open
    def sailence_func(self, from_date:datetime, to_date:datetime):
        #sort dateframe by datetime in descending
        self.month_data.sort_values(by = ["datetime"], ascending=False, inplace=True)
        #monthly process 
        entry_offset = 0
        while(True):
            tmp = datetime.strptime(self.month_data.iloc[entry_pos]["datetime"], "%y-%m-%d")
            if(tmp == to_date):
                break
            entry_offset += 1
        entry_pos = entry_offset + 0
        to_date = datetime.strptime(self.month_data.iloc[entry_pos]["datetime"], "%y-%m-%d")
        from_date = to_date - timedelta(days=self.period)
        process_date = to_date
        while(self.month_data.shape[0] > entry_pos):
            data_entry = self.month_data.iloc[entry_pos]
            process_date = datetime.strptime(data_entry["datetime"], "%y-%m-%d")
            if(process_date < from_date):
                break
            entry_pos += 1
            sailence = 0
            rate = (data_entry["close"] - data_entry["open"])/data_entry["open"]
            if rate  > 0.098:
                sailence = rate* 100 * self.period
            else:
                sailence = data_entry["turnover"]
            data_entry["sailence"] = sailence
    def sailence_weight(self, from_date:datetime, to_date:datetime):
        period_df = self.month_data.query("datetime <= \"{to_date}\" and datetime >= \"{from_date}\"".format(to_date=to_date, from_date=from_date))
        stock_codes_set = list(self.month_data["codes"].unique())
        for code in stock_codes_set:
            stock_monthly_cross = period_df.query("code == {code}".format(code = code))
            #sort sailence by value:最凸显---》最不凸显
            stock_monthly_cross.sort_values(by = ["sailence"], ascending=False, inplace=True)
            period = len(stock_monthly_cross)
            sum = 0
            for i in len(stock_monthly_cross):
                sum += self.sailence_twist^i
            for i in len(stock_monthly_cross):
                stock_monthly_cross.iloc[i]["sailence_weight"] = self.sailence_twist^i/sum * period
            self.code_2_sailence_weight[code] = stock_monthly_cross
            
    def sailence_factor(self):
         for code in self.code_2_sailence_weight.keys():
            stock_monthly_cross = self.code_2_sailence_weight[code]
            self.code_2_sailence_factor[code] = stock_monthly_cross["sailence_weight"].cov(stock_monthly_cross["daily_yield"])


        
        
        
        
            

# Create a Stratey
class TestStrategy(bt.Strategy):
    params = (
        ('maperiod', 15),
    )

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        #TODO ... get history data
        ####
        # Keep a reference to the "close" line in the data[0] dataseries
        self.data_close = self.datas[0].close
        self.data_open = self.datas[0].open
        self.data_high = self.datas[0].high
        self.data_low = self.datas[0].low
        self.data_turnover= self.datas[0].turnover

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # Add a MovingAverageSimple indicator
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.data_close[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            # Not yet ... we MIGHT BUY if ...
            if self.data_close[0] > self.sma[0]:

                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.data_close[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()

        else:

            if self.data_close[0] < self.sma[0]:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.data_close[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()