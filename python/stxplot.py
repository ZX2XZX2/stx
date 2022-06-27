import base64
from datetime import datetime
from io import BytesIO
import mplfinance as mpf
from stxts import StxTS
import sys

class StxPlot:
    def __init__(self, ts, title, start_date, end_date, trend_lines=None,
                 stk=None):
        if ts is not None:
            self.ts = ts
        else:
            self.ts = self.loadts(stk, start_date, end_date)
        self.title = title
        # Create my own `marketcolors` to use with the `nightclouds` style:
        # mc = mpf.make_marketcolors(up='#00ff00',down='#ff0000',inherit=True)
        # Create new style based on `nightclouds` but w/ my own `marketcolors`:
        # self.s  = mpf.make_mpf_style(base_mpf_style='nightclouds',
        #                              marketcolors=mc)
        self.s = 'yahoo'
        self.trend_lines = trend_lines
        self.plot_df = self.ts.df.loc[start_date:end_date,:]

    def loadts(self, stk, start_date, end_date):
        if stk is None:
            return None
        ts = StxTS(stk, start_date, end_date)
        day_ix = ts.set_day(end_date)
        if day_ix == -1:
            return None
        ts.df.index.name='Date'
        ts.df.drop('oi', inplace=True, axis=1)
        ts.df['o'] /= 100
        ts.df['hi'] /= 100
        ts.df['lo'] /= 100
        ts.df['c'] /= 100
        ts.df['v'] *= 1000
        ts.df.rename(columns={'o': 'Open',
                              'hi': 'High',
                              'lo': 'Low',
                              'c': 'Close',
                              'v': 'Volume'},
                     inplace=True)
        return ts
        
    def drawchart(self):
        fig = mpf.figure(figsize=(10, 6), style='yahoo')
        ax1 = fig.add_subplot(3, 1, (1, 2))
        ax2 = fig.add_subplot(3, 1, 3, sharex=ax1)
        apd = None
        if ('SMA50' in self.plot_df.columns and
            'SMA200' in self.plot_df.columns):
            apd = mpf.make_addplot(self.plot_df[['SMA50', 'SMA200']],
                                   ax=ax1)
        fig.subplots_adjust(hspace=0)
        xticks, xticklabels = [], []
        mth = -1
        for i, dt in enumerate(self.plot_df.index):
            if dt.dayofweek == 0:
                xticks.append(i)
                if dt.month != mth:
                    mth = dt.month
                    xticklabels.append(datetime.strftime(dt, '%b %d'))
                else:
                    xticklabels.append(datetime.strftime(dt, '%d'))
        ax1.set_xticks(xticks)
        ax1.set_xticklabels(xticklabels)
        if not self.trend_lines:
            if not apd:
                mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
                         axtitle=self.title)
            else:
                mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
                         axtitle=self.title, addplot=apd)
        else:
            if not apd:
                mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
                         axtitle=self.title, alines=self.trend_lines)
            else:
                mpf.plot(self.plot_df, type='candle', ax=ax1, volume=ax2,
                         axtitle=self.title, alines=self.trend_lines,
                         addplot=apd)
        return fig

    def plotchart(self, savefig=True):
        fig = self.drawchart()
        if savefig:
            fig.savefig(f'/tmp/{self.ts.stk}.png')

    def webchartstream(self):
        fig = self.drawchart()
        figfile = BytesIO()
        fig.savefig(figfile, format='png')
        figfile.seek(0)
        figdata_png = figfile.getvalue()
        figdata_png = base64.b64encode(figdata_png).decode("utf-8")
        return f'<img src="data:image/png;base64,{figdata_png}" width="1500">'


    def b64_png(self):
        fig = self.drawchart()
        figfile = BytesIO()
        fig.savefig(figfile, format='png')
        figfile.seek(0)
        figdata_png = figfile.getvalue()
        figdata_png = base64.b64encode(figdata_png).decode("utf-8")
        return figdata_png


if __name__ == '__main__':
    # TODO: use argparser and all that stuff
    stk = sys.argv[1]
    sd = sys.argv[2]
    ed = sys.argv[3]
    sorporw = sys.argv[4]

    sp = StxPlot(None, stk, sd, ed, stk=stk)
    if sorporw == 'w':
        chartstream = sp.webchartstream()
        print(f'{chartstream}')
    else:
        savefig = sorporw.startswith('s')
        sp.plotchart(savefig=savefig)
        if not savefig:
            mpf.show()
