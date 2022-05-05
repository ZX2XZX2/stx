import mplfinance as mpf
from stxts import StxTS
import sys

class StxPlot:
    def __init__(self, ts, title, start_date, end_date, trend_lines=None):
        self.ts = ts
        self.title = title
        # Create my own `marketcolors` to use with the `nightclouds` style:
        mc = mpf.make_marketcolors(up='#00ff00',down='#ff0000',inherit=True)
        # Create new style based on `nightclouds` but w/ my own `marketcolors`:
        self.s  = mpf.make_mpf_style(base_mpf_style='nightclouds',
                                     marketcolors=mc)
        self.trend_lines = trend_lines

    def plot_to_file(self):
        if not self.trend_lines:
            mpf.plot(self.ts.df, type='candle', style=self.s, volume=True,
                     title=self.title, figsize=[12, 5],
                     savefig=f'/tmp/{self.ts.stk}.png')
        else:
            mpf.plot(self.ts.df, type='candle', alines=self.trend_lines,
                     style=self.s, volume=True, title=self.title,
                     figsize=[12, 5], savefig=f'/tmp/{self.ts.stk}.png')

    def plot(self):
        if not self.trend_lines:
            mpf.plot(self.ts.df, type='candle', style=self.s, volume=True)
        else:
            mpf.plot(self.ts.df, type='candle', alines=self.trend_lines,
                     style=self.s, volume=True)


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
