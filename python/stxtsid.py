import pandas as pd
from psycopg2 import sql
import stxcal
import stxdb
from stxplot import StxPlot
from stxplotid import StxPlotID
from stxts import StxTS

class StxTSID(StxTS):
    def __init__(self, stk, sd, ed, et, id_tbl='intraday', eod_tbl='eods',
                 split_tbl='dividends'):
        super().__init__(stk, sd, ed, eod_tbl, split_tbl)
        self.id_sd = stxcal.move_busdays(ed, -10)
        self.id_sdt = f'{self.id_sd} 09:35'
        self.id_edt = f'{ed} {et}'
        self.crt_date = ed
        self.id_tbl = id_tbl
        self.idf = self.load_intraday()

    def load_intraday(self):
        q = sql.Composed([
            sql.SQL("SELECT * FROM "),
            sql.Identifier(self.id_tbl),
            sql.SQL(" WHERE stk="),
            sql.Literal(self.stk),
            sql.SQL(" AND dt BETWEEN "),
            sql.Literal(self.id_sdt),
            sql.SQL(" AND "),
            sql.Literal(self.id_edt),
            sql.SQL(" ORDER BY dt")
        ])
        idf = pd.read_sql(q, stxdb.db_get_cnx(), index_col='dt',
                          parse_dates=['dt'])
        return idf

    def mpf_id(self, crt_date):
        super().mpf_eod(crt_date)
        self.idf.index.name='Date'
        self.idf.drop(['stk', 'oi'], inplace=True, axis=1)
        self.idf['o'] /= 100
        self.idf['hi'] /= 100
        self.idf['lo'] /= 100
        self.idf['c'] /= 100
        self.idf.rename(columns={'o': 'Open',
                                 'hi': 'High',
                                 'lo': 'Low',
                                 'c': 'Close',
                                 'v': 'Volume'},
                        inplace=True)
        self.adjust_last_day()

    def adjust_intraday_splits(self):
        # TODO: adjust intraday data for splits
        pass

    def adjust_last_day(self):
        lastday_tf = self.idf.loc[self.crt_date]
        resample_map = {
            'Open' :'first',
            'High' :'max'  ,
            'Low'  :'min'  ,
            'Close':'last',
            'Volume':'sum'
        }
        crt_dt = self.crt_date
        last_daily = lastday_tf.resample('1440T').agg(resample_map).dropna()
        self.df.loc[self.ed, 'Open'] = last_daily.loc[self.ed, 'Open']
        self.df.loc[self.ed, 'High'] = last_daily.loc[self.ed, 'High']
        self.df.loc[self.ed, 'Low'] = last_daily.loc[self.ed, 'Low']
        self.df.loc[self.ed, 'Close'] = last_daily.loc[self.ed, 'Close']
        self.df.loc[self.ed, 'Volume'] = last_daily.loc[self.ed, 'Volume']

    def getchartstreams(self, end_dt, eod_days=90, id_days1=10,
                        id_mins1=30, id_days2=5, id_mins2=10):
        chartdict = {}
        end_date, end_time = end_dt.split(' ')
        start_date = stxcal.move_busdays(end_date, -eod_days)
        sp = StxPlot(None, self.stk, start_date, end_date, stk=self.stk)
        chartdict['eod_png'] = sp.b64_png()
        start_date = stxcal.move_busdays(end_date, -id_days1)
        start_dt = f'{start_date} 09:35'
        spid1 = StxPlotID(self.idf, start_dt, end_dt, self.stk, id_mins1)
        chartdict[ 'id1_png'] = spid1.b64_png()
        start_date = stxcal.move_busdays(end_date, -id_days2)
        start_dt = f'{start_date} 09:35'
        spid2 = StxPlotID(self.idf, start_dt, end_dt, self.stk, id_mins2)
        chartdict[ 'id2_png'] = spid2.b64_png()
        return chartdict
    #     if period > 5:
    #         resample_period = f'{period}T'
    #         rs = idf.resample(resample_period).agg(resample_map).dropna()
    #         self.plot_df = rs
    #     else:
    #         self.plot_df = idf
    #     self.s = 'yahoo'
    #     self.stk = stk



    #     self.idf = pd.read_sql(q, stxdb.db_get_cnx(), index_col='dt'
