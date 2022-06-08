from datetime import datetime
import mplfinance as mpf
import pandas as pd
from psycopg2 import sql
import stxdb
import sys

class StxPlotID:
    def __init__(self, stk, start_date, end_date, period='5T'):
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
        self.plot_df = idf
        # resample_map ={'Open' :'first',
        #                'High' :'max'  ,
        #                'Low'  :'min'  ,
        #                'Close':'last' }
        # resample_period = '15T'
        # rs = idf.resample(resample_period).agg(resample_map).dropna()
        self.s = 'yahoo'
        self.stk = stk
        
    def plotchart(self, savefig=True):
        fig = mpf.figure(figsize=(10, 6), style='yahoo')
        ax1 = fig.add_subplot(3, 1, (1, 2))
        ax2 = fig.add_subplot(3, 1, 3, sharex=ax1)
        apd = None
        # if ('SMA50' in self.plot_df.columns and
        #     'SMA200' in self.plot_df.columns):
        #     apd = mpf.make_addplot(self.plot_df[['SMA50', 'SMA200']],
        #                            ax=ax1)
        fig.subplots_adjust(hspace=0)
        # xticks, xticklabels = [], []
        # mth = -1
        # for i, dt in enumerate(self.plot_df.index):
        #     if dt.dayofweek == 0:
        #         xticks.append(i)
        #         if dt.month != mth:
        #             mth = dt.month
        #             xticklabels.append(datetime.strftime(dt, '%b %d'))
        #         else:
        #             xticklabels.append(datetime.strftime(dt, '%d'))
        # ax1.set_xticks(xticks)
        # ax1.set_xticklabels(xticklabels)
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
    # TODO: use argparser and all that stuff
    stk = sys.argv[1]
    sd = sys.argv[2]
    ed = sys.argv[3]
    sorp = sys.argv[4]

    sp = StxPlotID(stk, sd, ed)
    savefig = sorp.startswith('s')
    sp.plotchart(savefig=savefig)
    if not savefig:
        mpf.show()
