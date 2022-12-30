import argparse
import base64
import ctypes
from datetime import datetime
from io import BytesIO
# import logging
import mplfinance as mpf
import os
import pandas as pd
import sys

class ChartStruct(ctypes.Structure):
    _fields_ = [
        ('date', ctypes.c_char * 20),
        ('open', ctypes.c_float),
        ('high', ctypes.c_float),
        ('low', ctypes.c_float),
        ('close', ctypes.c_float),
        ('volume', ctypes.c_int),
        ('sma_50', ctypes.c_float),
        ('sma_200', ctypes.c_float)
    ]

# TODO:
# 1. Remove the -1 values for SMA50 and SMA200
# 2. Revert to integers for open, high, low, close, sma
class StxPlotBin:
    def __init__(self, stk, mkt_name, start_dt, end_dt, intraday):
        bin_file_path = os.path.join(os.getenv('HOME'), 'stx', 'mkt', mkt_name,
                                     'intraday' if intraday else 'eod', f'{stk}.dat')
        dates, data = [], []
        with open(bin_file_path, 'rb') as file:
            x = ChartStruct()
            while file.readinto(x) == ctypes.sizeof(x):
                dates.append(datetime.fromisoformat(x.date.decode('utf-8')))
                data.append((x.open, x.high, x.low, x.close, x.volume, x.sma_50, x.sma_200))
        if not dates or not data:
            print(f'Could not read binary data for stock {stk} in market {mkt_name}')
        idf = pd.DataFrame(
            data=data,
            index=dates,
            columns=['Open', 'High', 'Low', 'Close', 'Volume', 'SMA50', 'SMA200']) #,
            # dtype=[str, float, float, float, float, int, float, float])

        idf.index.name = 'Date'

        self.plot_df = idf.loc[start_dt:end_dt,:]
        self.intraday = intraday
        self.stk = stk

        # resample_map ={'Open' :'first',
        #                'High' :'max'  ,
        #                'Low'  :'min'  ,
        #                'Close':'last',
        #                'Volume':'sum'}
        # if period > 5:
        #     resample_period = f'{period}T'
        #     rs = idf.resample(resample_period).agg(resample_map).dropna()
        #     self.plot_df = rs
        # else:
        #     self.plot_df = idf
        # self.s = 'yahoo'

    def drawchart(self):
        fig = mpf.figure(figsize=(10, 6), style='yahoo')
        ax1 = fig.add_subplot(3, 1, (1, 2))
        ax2 = fig.add_subplot(3, 1, 3, sharex=ax1)
        apd = None
        if ('SMA50' in self.plot_df.columns and
            'SMA200' in self.plot_df.columns):
            apd = mpf.make_addplot(self.plot_df[['SMA50', 'SMA200']], ax=ax1)
        fig.subplots_adjust(hspace=0)
        xticks, xticklabels = [], []
        if self.intraday:
            day = -1
            for i, dt in enumerate(self.plot_df.index):
                if dt.day != day:
                    xticks.append(i)
                    xticklabels.append(datetime.strftime(dt, '%b %d'))
                    day = dt.day
        else:
            mth = -1
            for i, dt in enumerate(self.plot_df.index):
                if dt.dayofweek == 0:
                    xticks.append(i)
                    if dt.month != mth:
                        mth = dt.month
                        xticklabels.append(datetime.strftime(dt, '%b %d'))
                    else:
                        xticklabels.append(datetime.strftime(dt, '%d'))
        # print(f'xticklabels = {xticklabels}')
        ax1.set_xticks(xticks)
        ax1.set_xticklabels(xticklabels)
        ax2.set_xticklabels(xticklabels)
        # if not self.trend_lines:
        if not apd:
            mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
                     axtitle=self.stk)
        else:
            mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
                     axtitle=self.stk, addplot=apd)
        # else:
        #     if not apd:
        #         mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
        #                  axtitle=self.title, alines=self.trend_lines)
        #     else:
        #         mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
        #                  axtitle=self.title, alines=self.trend_lines,
        #                  addplot=apd)
        return fig

    def plotchart(self, savefig=True):
        fig = self.drawchart()
        if savefig:
            fig.savefig(f"/tmp/{self.stk}{'_ID' if self.intraday else ''}.png")

    def b64_png(self):
        fig = self.drawchart()
        figfile = BytesIO()
        fig.savefig(figfile, format='png')
        figfile.seek(0)
        figdata_png = figfile.getvalue()
        figdata_png = base64.b64encode(figdata_png).decode("utf-8")
        return figdata_png


if __name__ == '__main__':
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
    #     '%(message)s',
    #     datefmt='%Y-%m-%d %H:%M:%S',
    #     level=logging.INFO
    # )
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--stk', type=str, default='NFLX',
                        help='Stock to chart')
    parser.add_argument('-m', '--mkt', type=str, default='test',
                        help='Market for the stock to chart')
    parser.add_argument('-p', '--period', type=int, default=15,
                        help='Time interval covered by one candle, in minutes')
    parser.add_argument('-a', '--sorp', type=str, default='p',
                        help='Save or plot the chart (s)ave, (p)lot')
    parser.add_argument('-s', '--startdate', type=str, required=True,
                        help='Start date for chart')
    parser.add_argument('-e', '--enddate', type=str, required=True,
                        help='End date for chart')
    parser.add_argument('-i', '--intraday', action='store_true',
                        help="Run Intraday analysis")    

    args = parser.parse_args()
    sp = StxPlotBin(args.stk, args.mkt, args.startdate, args.enddate, args.intraday)
    savefig = args.sorp.startswith('s')
    sp.plotchart(savefig=savefig)
    if not savefig:
        mpf.show()
