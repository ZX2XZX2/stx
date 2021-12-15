import mplfinance as mpf
from stxts import StxTS
import sys

class StxPlot:
    def __init__(self, stock, start_date, end_date):
        ts = StxTS(stock, start_date, end_date)
        ts.df.index.name='Date'
        ts.df.drop('oi', inplace=True, axis=1)
        ts.df['o'] /= 100
        ts.df['hi'] /= 100
        ts.df['lo'] /= 100
        ts.df['c'] /= 100
        ts.df.rename(columns={'o': 'Open', 
                              'hi': 'High', 
                              'lo': 'Low', 
                              'c': 'Close', 
                              'v': 'Volume'}, 
                     inplace=True)
        self.ts = ts
        # Create my own `marketcolors` to use with the `nightclouds` style:
        mc = mpf.make_marketcolors(up='#00ff00',down='#ff0000',inherit=True)
        # Create new style based on `nightclouds` but w/ my own `marketcolors`:
        self.s  = mpf.make_mpf_style(base_mpf_style='nightclouds',
                                     marketcolors=mc)

    def plot_to_file(self):
        mpf.plot(self.ts.df, type='candle', style=self.s, title=self.ts.stk, 
                 volume=True, savefig='/tmp/{0:s}.png'.format(self.ts.stk))

    def plot(self):
        mpf.plot(self.ts.df, type='candle', style=self.s, title=self.ts.stk, 
                 volume=True)

if __name__ == '__main__':
    # TODO: use argparser and all that stuff
    stk = sys.argv[1]
    sd = sys.argv[2]
    ed = sys.argv[3]
    sorp = sys.argv[4]
    sp = StxPlot(stk, sd, ed)
    if sorp.startswith('s'):
        sp.plot_to_file()
    elif sorp.startswith('p'):
        sp.plot()
    else:
        print('Dont know what to do with {}'.format(sorp))
