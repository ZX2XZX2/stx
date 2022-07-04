import pandas as pd
from psycopg2 import sql
import stxcal
import stxdb
from stxts import StxTS

class StxTSID(StxTS):
    def __init__(self, stk, sd, ed, et, id_tbl='intraday', eod_tbl='eods',
                 split_tbl='dividends'):
        super().__init__(stk, sd, ed, eod_tbl, split_tbl)
        self.id_sd = stxcal.move_busdays(ed, -10)
        self.id_sdt = f'{self.id_sd} 09:35'
        self.id_edt = f'{ed} {et}'
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
        idf.index.name='Date'
        idf.drop(['stk', 'oi'], inplace=True, axis=1)
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
        return idf

    def adjust_intraday_splits(self):
        # TODO: adjust intraday data for splits
        pass

    # def adjust_last_day(self):
    #     # self.plot_df = idf
    #     resample_map ={'Open' :'first',
    #                    'High' :'max'  ,
    #                    'Low'  :'min'  ,
    #                    'Close':'last',
    #                    'Volume':'sum'}
    #     if period > 5:
    #         resample_period = f'{period}T'
    #         rs = idf.resample(resample_period).agg(resample_map).dropna()
    #         self.plot_df = rs
    #     else:
    #         self.plot_df = idf
    #     self.s = 'yahoo'
    #     self.stk = stk



    #     self.idf = pd.read_sql(q, stxdb.db_get_cnx(), index_col='dt'
