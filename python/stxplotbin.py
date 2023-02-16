import argparse
import base64
import ctypes
from datetime import datetime
from io import BytesIO
import logging
import mplfinance as mpf
import os
import pandas as pd
import sys

class ChartStruct(ctypes.Structure):
    _fields_ = [
        ('o', ctypes.c_int),
        ('hi', ctypes.c_int),
        ('lo', ctypes.c_int),
        ('c', ctypes.c_int),
        ('v', ctypes.c_int),
        ('dt', ctypes.c_char * 20)#,
        # ('sma_50', ctypes.c_int),
        # ('sma_200', ctypes.c_int)
    ]

class StxPlotBin:

    def __init__(self, _lib, stk, num_days, end_dt, intraday, period=5):
        logging.info('start')
        num_recs = ctypes.c_int(0)
        self._lib = _lib
        self._lib.stx_get_ohlcv.argtypes = (
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_bool,
            ctypes.c_bool,
            ctypes.POINTER(ctypes.c_int),
        )
        self._lib.stx_get_ohlcv.restype = ctypes.POINTER(ChartStruct)
        realtime = False
        logging.info('getting stock data')
        res = self._lib.stx_get_ohlcv(
            ctypes.c_char_p(stk.encode('UTF-8')),
            ctypes.c_char_p(end_dt.encode('UTF-8')),
            ctypes.c_int(num_days),
            ctypes.c_bool(intraday),
            ctypes.c_bool(realtime),
            ctypes.byref(num_recs)
        )
        logging.info(f'got stock data, num_recs = {num_recs}')

        _ohlc_list = res[:num_recs.value]
        ohlc_list = [(x.o, x.hi, x.lo, x.c, x.v,
                     datetime.fromisoformat(x.dt.decode('utf-8')))
                     for x in _ohlc_list]
        self._lib.stx_free_ohlcv.argtypes = (
            ctypes.POINTER(ctypes.POINTER(ChartStruct)),
        )
        self._lib.stx_free_ohlcv.restype = None
        logging.info('freeing stock data')
        self._lib.stx_free_ohlcv(
            ctypes.byref(res)
        )
        idf = pd.DataFrame(
            data=ohlc_list,
            columns=['Open','High','Low','Close','Volume', 'Date'])
        idf.set_index('Date', inplace=True)
        logging.info('built data frame')
        
        self.plot_df = idf
        self.intraday = intraday
        self.stk = stk
        self.s = 'yahoo'
        if intraday and period > 5:
            resample_period = f'{period}T'
            resample_map = {
                'Open'  : 'first',
                'High'  : 'max'  ,
                'Low'   : 'min'  ,
                'Close' : 'last',
                'Volume': 'sum'
            }
            idf = idf.resample(resample_period).agg(resample_map).dropna()
        self.plot_df = idf
        logging.info('done')

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
                     axtitle=self.stk, warn_too_much_data=10000)
        else:
            mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
                     axtitle=self.stk, addplot=apd, warn_too_much_data=10000)
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
    so_file = os.path.join(os.sep, 'usr', 'local', 'bin', 'stx_lib.so')
    _lib = ctypes.CDLL(so_file)
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--stk', type=str, default='NFLX',
                        help='Stock to chart')
    parser.add_argument('-p', '--period', type=int, default=5,
                        help='Time interval covered by one candle, in minutes')
    parser.add_argument('-a', '--sorp', type=str, default='p',
                        help='Save or plot the chart (s)ave, (p)lot')
    parser.add_argument('-d', '--days', type=int, required=True,
                        help='Number of days shown in chart')
    parser.add_argument('-e', '--enddate', type=str, required=True,
                        help='End date for chart')
    parser.add_argument('-i', '--intraday', action='store_true',
                        help="Run Intraday analysis")    

    args = parser.parse_args()
    sp = StxPlotBin(_lib, args.stk, args.days, args.enddate, args.intraday,
                    args.period)
    savefig = args.sorp.startswith('s')
    sp.plotchart(savefig=savefig)
    if not savefig:
        mpf.show()
