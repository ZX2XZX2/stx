import argparse
from datetime import datetime
import mplfinance as mpf
import pandas as pd
from psycopg2 import sql
import stxcal
import stxdb
import sys

class StxPlotID:
    def __init__(self, stk, start_date, end_date, period=5):
        q = sql.Composed([
            sql.SQL("SELECT * FROM intraday WHERE stk="),
            sql.Literal(stk),
            sql.SQL(" AND dt BETWEEN "),
            sql.Literal(f'{start_date} 09:30:00'),
            sql.SQL(" AND "),
            sql.Literal(f'{end_date} 16:00:00'),
            sql.SQL(" ORDER BY dt")
        ])
        idf = pd.read_sql(q, stxdb.db_get_cnx(), index_col='dt',
                          parse_dates=['dt'])
        idf.index.name='Date'
        idf.drop('oi', inplace=True, axis=1)
        idf['o'] /= 100
        idf['hi'] /= 100
        idf['lo'] /= 100
        idf['c'] /= 100
        idf.rename(columns={'o': 'Open',
                            'hi': 'High',
                            'lo': 'Low',
                            'c': 'Close',
                            'v': 'Volume'},
                   inplace=True)
        # self.plot_df = idf
        resample_map ={'Open' :'first',
                       'High' :'max'  ,
                       'Low'  :'min'  ,
                       'Close':'last',
                       'Volume':'sum'}
        if period > 5:
            resample_period = f'{period}T'
            rs = idf.resample(resample_period).agg(resample_map).dropna()
            self.plot_df = rs
        else:
            self.plot_df = idf
        self.s = 'yahoo'
        self.stk = stk
        if len(self.plot_df) > 51:
            self.plot_df['SMA50'] = self.plot_df['Close'].rolling(50).mean()
        if len(self.plot_df) > 201:
            self.plot_df['SMA200'] = self.plot_df['Close'].rolling(200).mean()
        
    def plotchart(self, savefig=True):
        fig = mpf.figure(figsize=(10, 6), style='yahoo')
        ax1 = fig.add_subplot(3, 1, (1, 2))
        ax2 = fig.add_subplot(3, 1, 3, sharex=ax1)
        apd = None
        if ('SMA50' in self.plot_df.columns and
            'SMA200' in self.plot_df.columns):
            apd = mpf.make_addplot(self.plot_df[['SMA50', 'SMA200']], ax=ax1)
        fig.subplots_adjust(hspace=0)
        xticks, xticklabels = [], []
        day = -1
        for i, dt in enumerate(self.plot_df.index):
            if dt.day != day:
                xticks.append(i)
                xticklabels.append(datetime.strftime(dt, '%b %d'))
                day = dt.day
        ax1.set_xticks(xticks)
        ax1.set_xticklabels(xticklabels)
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
        if savefig:
            fig.savefig(f'/tmp/{self.ts.stk}_ID.png')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--stk', type=str, default='SPY',
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
    args = parser.parse_args()
    sp = StxPlotID(args.stk, args.startdate, args.enddate, args.period)
    savefig = args.sorp.startswith('s')
    sp.plotchart(savefig=savefig)
    if not savefig:
        mpf.show()
