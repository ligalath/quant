# Import the backtrader platform
import backtrader as bt

import datetime  # For datetime objects
from dateutil.relativedelta import relativedelta
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])

#local modules
from DataFetch import WindDataFetch
from Pandas2DataFeeds import STVPdData
from STR_strategy import Sailence
from STR_strategy import STRStrategy



if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(STRStrategy)

    # Fetch data
    wind_data_fetcher = WindDataFetch()
    #2 year data
    to_date = datetime.datetime.now()
    # from_date = to_date - relativedelta(year=2)
    from_date = to_date - relativedelta(month=2)
    fields = ['open','close','high','low','pct_chg','turn']
    today = datetime.datetime.today()
    today_str = today.strftime('%Y-%m-%d')
    #A股全部数据
    options = "date={today};sectorid=a001010100000000".format(today=today_str)
    codes_set = wind_data_fetcher.fetch_codes(options)
    code_2_df = wind_data_fetcher.fetch_data(codes_set, from_date, to_date, fields)
    #process dataframe with Sailence Strategy
    sailence_processor = Sailence(df, from_date, to_date)
    code_2_data = sailence_processor.process(from_date, to_date)
    # Create a Data Feed
    for code in code_2_data.keys():
        one_stock_data = code_2_data[code]
        one_stock_data.sort_values(by=['datetime'], ascending=True, inplace=True)
        data = STVPdData(
            dataname = code,
            fromdate = from_date,
            todate = to_date,
            reverse=False
        )
        # Add the Data Feed to Cerebro
        cerebro.adddata(data)

    # Set our desired cash start
    cerebro.broker.setcash(100000.0)

    # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)

    # Set the commission
    cerebro.broker.setcommission(commission=0.0003)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())