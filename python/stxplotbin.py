import argparse
import base64
import ctypes
from datetime import datetime
from io import BytesIO
import mplfinance as mpf
import numpy as np
import os
import pandas as pd
from psycopg2 import sql
import stxcal
import stxdb
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

class StxPlotBin:
    def __init__(self, stk, start_dt, end_dt, intraday):
        bin_file_path = os.path.join(os.getenv('HOME'), 'stx', 'mkt', 'test',
                                     'intraday' if intraday else 'eod', f'{stk}.dat')
        with open(bin_file_path, 'rb') as file:
            result = []
            x = ChartStruct()
            while file.readinto(x) == ctypes.sizeof(x):
                result.append((x.date, x.open, x.high, x.low, x.close, x.volume, x.sma_50, x.sma_200))
            print(result)
        chart_struct_dtype =  np.dtype([
            ('Date', np.byte, (20,)),
            ('Open', np.float,),
            ('High', np.float,),
            ('Low', np.float,),
            ('Close', np.float,),
            ('Volume', np.int,),
            ('SMA50', np.float,),
            ('SMA200', np.float,),
        ])
        self.chart_data = np.fromfile(bin_file_path, dtype=chart_struct_dtype, count=-1)
        # print(f'chart_data[0]: {self.chart_data[0].Date}')
        #     idf = pd.read_sql(q, stxdb.db_get_cnx(), index_col='dt',
        #                       parse_dates=['dt'])
        #     idf.index.name='Date'
        #     idf.drop('oi', inplace=True, axis=1)
        #     idf['o'] /= 100
        #     idf['hi'] /= 100
        #     idf['lo'] /= 100
        #     idf['c'] /= 100
        #     idf.rename(columns={'o': 'Open',
        #                         'hi': 'High',
        #                         'lo': 'Low',
        #                         'c': 'Close',
        #                         'v': 'Volume'},
        #                inplace=True)
        # else:
        #     idf = idf.loc[start_dt:end_dt,:]
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
        # self.stk = stk
        # if len(self.plot_df) > 51:
        #     self.plot_df['SMA50'] = self.plot_df['Close'].rolling(50).mean()
        # if len(self.plot_df) > 201:
        #     self.plot_df['SMA200'] = self.plot_df['Close'].rolling(200).mean()

    def drawchart(self):
        # fig = mpf.figure(figsize=(10, 6), style='yahoo')
        # ax1 = fig.add_subplot(3, 1, (1, 2))
        # ax2 = fig.add_subplot(3, 1, 3, sharex=ax1)
        # apd = None
        # if ('SMA50' in self.plot_df.columns and
        #     'SMA200' in self.plot_df.columns):
        #     apd = mpf.make_addplot(self.plot_df[['SMA50', 'SMA200']], ax=ax1)
        # fig.subplots_adjust(hspace=0)
        # xticks, xticklabels = [], []
        # day = -1
        # for i, dt in enumerate(self.plot_df.index):
        #     if dt.day != day:
        #         xticks.append(i)
        #         xticklabels.append(datetime.strftime(dt, '%b %d'))
        #         day = dt.day
        # print(f'xticklabels = {xticklabels}')
        # ax1.set_xticks(xticks)
        # ax1.set_xticklabels(xticklabels)
        # ax2.set_xticklabels(xticklabels)
        # # if not self.trend_lines:
        # if not apd:
        #     mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
        #              axtitle=self.stk)
        # else:
        #     mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
        #              axtitle=self.stk, addplot=apd)
        # # else:
        # #     if not apd:
        # #         mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
        # #                  axtitle=self.title, alines=self.trend_lines)
        # #     else:
        # #         mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
        # #                  axtitle=self.title, alines=self.trend_lines,
        # #                  addplot=apd)
        # return fig
        pass

    def plotchart(self, savefig=True):
        pass
        # fig = self.drawchart()
        # if savefig:
        #     fig.savefig(f'/tmp/{self.ts.stk}_ID.png')

    def b64_png(self):
        pass
        # fig = self.drawchart()
        # figfile = BytesIO()
        # fig.savefig(figfile, format='png')
        # figfile.seek(0)
        # figdata_png = figfile.getvalue()
        # figdata_png = base64.b64encode(figdata_png).decode("utf-8")
        # return figdata_png


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--stk', type=str, default='NFLX',
                        help='Stock to chart')
    parser.add_argument('-p', '--period', type=int, default=15,
                        help='Time interval covered by one candle, in minutes')
    parser.add_argument('-a', '--sorp', type=str, default='p',
                        help='Save or plot the chart (s)ave, (p)lot')
    parser.add_argument('-s', '--startdate', type=str,
                        default=stxcal.move_busdays(
                            stxcal.current_busdate(hr=9), -10),
                        help='Start date for chart')
    parser.add_argument('-e', '--enddate', type=str,
                        default=stxcal.current_busdate(hr=9),
                        help='End date for chart')
    parser.add_argument('-i', '--intraday', action='store_true',
                        help="Run Intraday analysis")    

    args = parser.parse_args()
    sp = StxPlotBin(args.stk, args.startdate, args.enddate, args.intraday)
    savefig = args.sorp.startswith('s')
    sp.plotchart(savefig=savefig)
    if not savefig:
        mpf.show()
