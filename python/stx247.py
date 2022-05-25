import argparse
from datetime import datetime
import glob
import json
import logging
import numpy as np
import os
import pandas as pd
from psycopg2 import sql
import re
import smtplib
import stxcal
import stxdb
import stxgrps
from stxjl import StxJL
from stxts import StxTS
from stxplot import StxPlot
import sys
import time
import traceback as tb
import warnings
from weasyprint import HTML
import zipfile

warnings.filterwarnings("ignore")

"""
SELECT time_setups.dt, time_setups.stk, ind_groups.industry, ind_groups.sector, time_setups.direction, time_setups.setup, time_setups.tm, indicators_1.name, indicators_1.value, indicators_1.bucket_rank FROM time_setups, indicators_1, ind_groups WHERE time_setups.dt='2022-03-02' AND setup LIKE 'JC_%' AND triggered='t' AND indicators_1.dt='2022-03-01' AND time_setups.stk=indicators_1.ticker AND indicators_1.name='CS_45' AND ind_groups.dt = '2022-02-18' AND ind_groups.stk=time_setups.stk ORDER BY time_setups.direction, indicators_1.value;

SELECT time_setups.dt, time_setups.stk, time_setups.direction, time_setups.setup, indicators_1.name, indicators_1.value, indicators_1.bucket_rank FROM time_setups, indicators_1 WHERE time_setups.dt='2022-03-02' AND setup LIKE 'JC_%' AND triggered='t' AND indicators_1.dt='2022-03-01' AND time_setups.stk=indicators_1.ticker AND indicators_1.name='CS_20' ORDER BY time_setups.direction, indicators_1.value;

"""

class StxAnalyzer:
    def __init__(self, indicator_names, indicator_tenors):
        self.report_dir = os.path.join(os.getenv('HOME'), 'market')
        logging.info('PDF reports are stored locally in {0:s}'.
                     format(self.report_dir))
        self.indicator_names = indicator_names
        self.indicator_tenors = indicator_tenors
        self.trend_dict = {}
        self.report_style = '''
<style>
body {
  font-family: sans-serif;
  background-color: black;
  color: white;
}
table {
  border-collapse: collapse;
  border: 1px solid black;
  width: 100%;
  word-wrap: normal;
  table-layout: auto;
}
img {
  display: block;
  margin-left: auto;
  margin-right: auto;
  width: 99%;
}
</style>
'''

    def get_triggered_setups(self, dt, eod, triggered=False):
        setups_date = dt if eod else stxcal.prev_busday(dt)
        q = sql.Composed([
            sql.SQL("SELECT time_setups.dt, time_setups.stk, "
                    "ind_groups.industry, ind_groups.sector, "
                    "time_setups.direction, time_setups.setup, "
                    "time_setups.tm, indicators_1.name, "
                    "indicators_1.value, indicators_1.bucket_rank "
                    "FROM time_setups, indicators_1, ind_groups "
                    "WHERE time_setups.dt="),
            sql.Literal(dt),
            sql.SQL(' AND setup IN ('),
            sql.SQL(', ').join([
                sql.Literal('JC_5DAYS'),
                sql.Literal('JC_1234')
            ]),
            sql.SQL(") AND triggered="),
            sql.Literal('t' if triggered else 'f'),
            sql.SQL(" AND indicators_1.dt="),
            sql.Literal(setups_date),
            sql.SQL(" AND time_setups.stk=indicators_1.ticker "
                    "AND indicators_1.name="),
            sql.Literal('CS_45'),
            sql.SQL(" AND ind_groups.dt = "),
            sql.Literal(stxcal.prev_expiry(dt)),
            sql.SQL( " AND ind_groups.stk=time_setups.stk "
                     "ORDER BY time_setups.direction, indicators_1.value")
        ])
        logging.info(f'triggered_indicators SQL = '
                     f'{q.as_string(stxdb.db_get_cnx())}')
        df = pd.read_sql(q, stxdb.db_get_cnx())
        # Concatenate the setup strings
        groupby_cols = [x for x in df.columns if x != 'setup']
        df['setup'] = df.groupby(groupby_cols)['setup'].transform(
            lambda x: ', '.join(x))
        # drop duplicate data and reset the setup datafrane index
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_opt_spreads(self, crt_date, eod):
        exp_date = stxcal.next_expiry(crt_date, min_days=(1 if eod else 0))
        q = sql.Composed([sql.SQL('select stk, opt_spread from leaders '
                                  'where expiry='), sql.Literal(exp_date)])
        cnx = stxdb.db_get_cnx()
        with cnx.cursor() as crs:
            crs.execute(q.as_string(cnx))
            spread_dict = {x[0]: x[1] for x in crs}
        return spread_dict

    def filter_spreads(self, df, spreads, max_spread):
        df['spread'] = df.apply(lambda r: spreads.get(r['stk']), axis=1)
        df.drop_duplicates(['stk', 'direction'], inplace=True)
        df = df[df.spread < max_spread]
        return df

    def add_indicators(self, df, dt, indicators, eod):
        """Indicators are only available for the previous day, if this is an
        intraday run

        """
        indicators_date = dt if eod else stxcal.prev_busday(dt)
        stks = df.stk.unique().tolist()
        q = sql.Composed([
            sql.SQL("SELECT ticker, dt, name, value, rank, bucket_rank "
                    "FROM indicators_1 WHERE dt="),
            sql.Literal(indicators_date),
            sql.SQL(" AND ticker IN ("),
            sql.SQL(', ').join([sql.Literal(x) for x in stks]),
            sql.SQL(") AND name IN ("),
            sql.SQL(', ').join([sql.Literal(x) for x in indicators]),
            sql.SQL(')')
        ])
        logging.info(f'get indicators: ')        
        logging.info(f'q = {q.as_string(stxdb.db_get_cnx())}')
        indicators_df = pd.read_sql(q, stxdb.db_get_cnx())
        indicators_df = indicators_df.rename(columns = {'ticker': 'stk'})
        for indicator in indicators:
            indicator_df = indicators_df.query("name==@indicator").copy()
            indicator_df = indicator_df.rename(columns = {
                'value': f'{indicator}--value',
                'rank': f'{indicator}--rank',
                'bucket_rank': f'{indicator}--bucket'
            })
            indicator_df.drop(columns=['name', 'dt'], inplace=True)
            df = df.merge(indicator_df, on='stk')
        return df

    def build_indicators_table(self, row):
        ind_tbl_dict = {}
        table_header = ['<tr><th></th>']
        for ten in self.indicator_tenors:
            table_header.append(f'<th>{ten}</th>')
        table_header.append('</tr>')
        indicator_table = ['<table border="1">', ''.join(table_header)]
        for ind_name in self.indicator_names:
            t_row = [f'<tr><th>{ind_name}</th>']
            for ten in self.indicator_tenors:
                ind_value = f'{ind_name}_{ten}--value'
                ind_bucket = f'{ind_name}_{ten}--bucket'
                if ind_value in row and ind_bucket in row:
                    t_row.append(f"<td>{row[ind_bucket]}/{row[ind_value]}</td>")
                else:
                    t_row.append(f"<td></td>")
            t_row.append('</tr>')
            indicator_table.append(''.join(t_row))
        indicator_table.append('</table>')
        return indicator_table


    def get_jl_trend_lines(self, ts, jl_setup_df, crt_date):
        trend_lines = {}
        for _, row in jl_setup_df.iterrows():
            setup = row.setup
            setup_color = 'g' if row.direction == 'U' else 'r'
            if setup in ['JL_P', 'JL_B']:
                piv1 = row.info.get('channel', {}).get('p1', {})
                piv2 = row.info.get('channel', {}).get('p2', {})
                if not piv1 or not piv2:
                    continue
            elif setup == 'JL_SR':
                sr_pivots = row.info.get('sr_pivots', [])
                if not sr_pivots or len(sr_pivots) < 2:
                    continue
                piv1, piv2 = sr_pivots[0], sr_pivots[-1]
            d1, d2 = str(piv1['date']), str(piv2['date'])
            s1, s2 = piv1['state'], piv2['state']
            logging.debug(f'd1={d1}, d2={d2}, s1={s1}, s2={s2}')
            y1 = ts.get(d1, 'High' if s1 in ['UT', 'NRa'] else 'Low')
            y2 = ts.get(d2, 'High' if s2 in ['UT', 'NRa'] else 'Low')
            x1, x2 = 0, stxcal.num_busdays(d1, d2)
            logging.debug(f'x1={x1}, x2={x2}, y1={y1}, y2={y2}')
            slope = (y2 - y1) / float(x2 - x1)
            y3 = y2 + slope * stxcal.num_busdays(d2, crt_date)
            alines = trend_lines.get('alines', [])
            colors = trend_lines.get('colors', [])
            alines.append([(d1, y1), (crt_date, y3)])
            colors.append(setup_color)
            trend_lines['alines'] = alines
            trend_lines['colors'] = colors
        return trend_lines

    def get_trend_lines(self, row, crt_dt):
        setup = row.setup
        setup_color = 'g' if row.direction == 'U' else 'r'
        """
        dt  | stk | setup | factor | direction | triggered | tm | info
        2022-01-26 | CSX | JL_P  |    100 | U         | t         | 20:00:00 | {"pivot": {"obv": 3, "date": "2022-01-25", "price": 3315, "state": "DT"}, "length": 26, "channel": {"p1": {"obv": -21, "date": "2021-12-17", "price": 3494, "state": "NRe"}, "p2": {"obv": 3, "date": "2022-01-24", "price": 3315, "state": "DT"}, "bound": "lower"}}
        """
        if setup == 'JL_P':
            y1 = row.info.get('channel', {}).get('p1', {}).get('price')
            y2 = row.info.get('channel', {}).get('p2', {}).get('price')
            d1 = row.info.get('channel', {}).get('p1', {}).get('date')
            d2 = row.info.get('channel', {}).get('p2', {}).get('date')
            if (not y1) or (not y2) or (not d1) or (not d2):
                logging.warn(f'Setup info in wrong format, no pivot prices '
                             f'and/or dates')
                return None
            d1 = str(d1)
            d2 = str(d2)
            x1 = 0
            x2 = stxcal.num_busdays(d1, d2)
            slope = (y2 - y1) / float(x2 - x1)
            y3 = y2 + slope * stxcal.num_busdays(d2, crt_dt)
            return dict(alines=[(d1, y1 / 100.0), (crt_dt, y3 / 100.0)],
                        colors=[setup_color])
        """
        dt  | stk | setup | factor | direction | triggered | tm | info
        yyyy-mm-dd | AEP  | JL_B  |     50 | U         | t         | 09:49:00 | {"vr": 10, "ipx": 9953, "slope": -66.333335876464844, "length": 6, "channel": {"p1": {"obv": -16, "date": "2022-nn-ee", "price": 10351, "state": "NRa"}, "p2": {"obv": -3, "date": "2022-mm-dd", "price": 10152, "state": "NRa"}, "bound": "lower"}}
        """
        if setup == 'JL_B':
            d1 = str(row['dt'])
            p1 = row.info['ipx'] / 100.0
            d2 = row.info.get('channel', {}).get('p1', {}).get('date')
            d2 = row.info['channel']['p1']['date']
            p2 = row.info['channel']['p1']['price'] / 100.0
            return dict(alines=[(d1, p1), (d2, p2)], colors=[setup_color])
        """
        dt | stk | setup | factor | direction | triggered | tm | info
        2022-01-26 | BJ  | JL_SR |    100 | D         | t         | 20:00:00 | {"sr": 6366, "vr": 155, "length": 49, "num_sr": 2, "sr_pivots": [{"obv": -20, "date": "2021-11-12", "price": 6366, "state": "UT"}, {"obv": -15, "date": "2021-12-01", "price": 6326, "state": "NRe"}, {"obv": -12, "date": "2022-01-24", "price": 6338, "state": "NRa"}]}
        """
        if setup == 'JL_SR':
            sr_pivots = row.info['sr_pivots']
            d1 = sr_pivots[0]['date']
            y1 = sr_pivots[0]['price'] / 100.0
            d2 = sr_pivots[-1]['date']
            y2 = sr_pivots[-1]['price'] / 100.0
            x1 = 0
            x2 = stxcal.num_busdays(d1, d2)
            slope = (y2 - y1) / float(x2 - x1)
            y3 = y2 + slope * stxcal.num_busdays(d2, crt_dt)
            return dict(alines=[(d1, y1), (crt_dt, y3)], colors=[setup_color])
        return None

    def get_ts_and_title(self, ticker, s_date, crt_date, direction=None,
                         tm=None, setups=None):
        ts, title = None, None
        ts = StxTS(ticker, s_date, crt_date)
        day_ix = ts.set_day(crt_date)
        if day_ix == -1:
            return None, None
        avg_volume = np.average(ts.df['v'].values[-20:])
        rgs = [max(h, c_1) - min(l, c_1)
               for h, l, c_1 in zip(ts.df['hi'].values[-20:],
                                    ts.df['lo'].values[-20:],
                                    ts.df['c'].values[-21:-1])]
        avg_rg = np.average(rgs)
        title = ''.join([
            f"{ticker}  "
            f"D: {direction if direction else ''}  ",
            f"V: {int(1000 * avg_volume):,d}  ",
            f"R: {avg_rg / 100:.2f}  ",
            f"T: {tm if tm else ''}  ",
            f"S: {setups if setups else ''}"
        ])
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

        return ts, title

    def get_jl_setups_for_analysis(self, stk, crt_date, num_jl_days):
        s_date = stxcal.move_busdays(crt_date, -num_jl_days)
        q = sql.Composed([
            sql.SQL("SELECT * FROM time_setups WHERE dt BETWEEN "),
            sql.Literal(s_date),
            sql.SQL(" AND "),
            sql.Literal(crt_date),
            sql.SQL(" AND stk="),
            sql.Literal(stk),
            sql.SQL(" AND ((setup="), sql.Literal("JL_SR"),
            sql.SQL(" AND (info->>'num_sr')::int > "), sql.Literal(1),
            sql.SQL(")"),
            sql.SQL(" OR (setup IN ("),
            sql.SQL(', ').join([
                sql.Literal('JL_P'),
                sql.Literal('JL_B')
            ]),
            sql.SQL(") AND (info->>'length')::int >= "), sql.Literal(20),
            sql.SQL('))')
        ])
        logging.debug(f'jl stps sql = {q.as_string(stxdb.db_get_cnx())}')
        jl_setup_df = pd.read_sql(q, stxdb.db_get_cnx())
        logging.debug(f'{stk} has {len(jl_setup_df)} JL setups')
        return jl_setup_df

    def find_start_date(self, jl_setup_df, crt_date):
        start_date = crt_date
        for _, jl_setup_row in jl_setup_df.iterrows():
            crt_setup = jl_setup_row['setup']
            crt_info = jl_setup_row['info']
            if crt_setup == 'JL_B' or crt_setup == 'JL_P':
                dt0 = jl_setup_row.info.get('channel', {}).get(
                    'p1', {}).get('date')
            else: # if crt_setup == 'JL_SR'
                sr_pivots = jl_setup_row.info.get('sr_pivots', [])
                if not sr_pivots:
                    dt0 = None
                else:
                    dt0 = sr_pivots[0].get('date')
            if dt0 is None:
                continue
            if start_date > str(dt0):
                start_date = str(dt0)
        start_date = stxcal.move_busdays(start_date, -5)
        num_days = stxcal.num_busdays(start_date, crt_date)
        if num_days < 220:
            start_date = stxcal.move_busdays(crt_date, -220)
        return start_date

    def get_stk_ts(self, stk, start_date, crt_date):
        ts = StxTS(stk, start_date, crt_date)
        day_ix = ts.set_day(crt_date)
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
        if len(ts.df) > 51:
            ts.df['SMA50'] = ts.df['Close'].rolling(50).mean()
        if len(ts.df) > 201:
            ts.df['SMA200'] = ts.df['Close'].rolling(200).mean()
        return ts

    def get_avg_stats(self, ts):
        avg_volume = np.average(ts.df['Volume'].values[-20:])
        rgs = [max(h, c_1) - min(l, c_1)
               for h, l, c_1 in zip(ts.df['High'].values[-20:],
                                    ts.df['Low'].values[-20:],
                                    ts.df['Close'].values[-21:-1])]
        avg_rg = np.average(rgs)
        return avg_volume, avg_rg

    def get_title(self, ticker, avg_volume, avg_rg, direction=None,
                  tm=None, setups=None):
        title = ''.join([
            f"{ticker}  "
            f"D: {direction if direction else ''}  ",
            f"V: {int(avg_volume):,d}  ",
            f"R: {avg_rg:.2f}  ",
            f"T: {tm if tm else ''}  ",
            f"S: {setups if setups else ''}"
        ])
        return title

    """
    1. Get the JL setups in the last 20 business days
    2. Find the furthest first date in those setups. Move 5 BDs further (d_0).
    3. Load the data frame between d_0 and crt_date
    4. Add 50 and 200 MA to df
    5. Add the trend lines for all JL setups (adjust JL setups pivot prices)
    6. for each trendline, calc the intersection with current date
    """
    def setup_report1(self, row, crt_date, num_jl_days=20):
        res = []
        stk = row['stk']
        jl_setup_df = self.get_jl_setups_for_analysis(stk, crt_date,
                                                      num_jl_days)
        start_date = self.find_start_date(jl_setup_df, crt_date)
        logging.debug(f'First date for {stk} is {start_date}')
        ts = self.get_stk_ts(stk, start_date, crt_date)
        if ts is None:
            return []
        trend_lines = self.get_jl_trend_lines(ts, jl_setup_df, crt_date)
        avg_volume, avg_rg = self.get_avg_stats(ts)
        title = self.get_title(stk, avg_volume, avg_rg, row['direction'],
                               row['tm'], row['setup'])
        stk_plot = StxPlot(ts, title, start_date, crt_date, trend_lines)
        stk_plot.plot_to_file()
        res.append(f"<h4>{stk}  {row['bucket_rank']} [{row['industry']}, "
                   "{row['sector']}]</h4>")
        res.append(f'<img src="/tmp/{stk}.png" alt="{stk}">')
        res.extend(self.build_indicators_table(row))
        return res

    def setup_report(self, row, s_date, ana_s_date, crt_date, isd):
        res = []
        try:
            stk = row['stk']
            trend_lines = self.get_trend_lines(row, crt_date)
            if trend_lines:
                trend_start_date = trend_lines.get('alines')[0][0]
                if trend_start_date < s_date:
                    s_date = trend_start_date
                stk_sr = self.trend_dict.get(stk, {})
                stk_lines = stk_sr.get('alines', []) + trend_lines['alines']
                stk_colors = stk_sr.get('colors', []) + trend_lines['colors']
                self.trend_dict[stk] = dict(alines=stk_lines, colors=stk_colors)
            ts, title = self.get_ts_and_title(stk, s_date, crt_date,
                                              row['direction'], row['tm'],
                                              row['setup'])
            stk_plot = StxPlot(ts, title, s_date, crt_date,
                               self.trend_dict.get(stk))
            stk_plot.plot_to_file()
            res.append(f"<h4>{stk} {isd.get(stk, ['N/A', 'N/A'])}</h4>")
            res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.
                       format(stk, stk))
            res.extend(self.build_indicators_table(row))
        except:
            logging.error('Failed analysis for {0:s}'.format(stk))
            tb.print_exc()
            return []
        try:
            ana_res = self.ana_report(stk, ana_s_date, crt_date)
            res.append(ana_res)
        except:
            logging.error('Failed to analyze {0:s}'.format(stk))
            tb.print_exc()
        return res

    """ Display the triggered setups in chronological order """
    def add_timeline_report(self, setup_df):
        res = []
        time_setups = setup_df.sort_values(
            by=['tm'],
            ascending=False,
            inplace=False
        )
        res.append("<h3> Time Setups</h3>")
        res.append('<table>')
        res.append(
            "<tr>"
            "<th>Stock</th>"
            # "<th>Industry</th>"
            # "<th>Sector</th>"
            "<th>Direction</th>"
            "<th>Setup</th>"
            "<th>CS_45</td>"
            # "<th>Bucket_Rank</th>"
            "<th>Time</th>"
            "</tr>"
        )
        for _, row in time_setups.iterrows():
            res.append(
                f"<tr>"
                f"<td>{row['stk']}</td>"
                # f"<td>{row['industry']}</td>"
                # f"<td>{row['sector']}</td>"
                f"<td>{row['direction']}</td>"
                f"<td>{row['setup']}</td>"
                f"<td>{row['value']} / {row['bucket_rank']}</td>"
                # f"<td>{row['bucket_rank']}</td>"
                f"<td>{row['tm']}</td>"
                f"</tr>"
            )
        res.append('</table>')
        return res


    def trigger_setup_report(self, row, s_date, crt_date):
        res = []
        try:
            stk = row['stk']
            trend_lines = None # self.get_trend_lines(row)
            ts, title = self.get_ts_and_title(stk, s_date, crt_date,
                                              row['direction'], row['tm'],
                                              row['setup'])
            stk_plot = StxPlot(ts, title, s_date, crt_date, trend_lines)
            stk_plot.plot_to_file()
            res.append(f"<h4>{stk} {row['bucket_rank']} [{row['industry']}, "
                       f"{row['sector']}]</h4>")
            # res.append(f"<h4>{stk} "
            #     f"[{', '.join(sorted(stxetfs.stock_labels(stk)))}]</h4>")
            res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.
                       format(stk, stk))
            res.extend(self.build_indicators_table(row))
        except:
            logging.error('Failed analysis for {0:s}'.format(stk))
            tb.print_exc()
            return []
        # try:
        #     jl_res = StxJL.jl_report(stk, jl_s_date, crt_date, 1.5)
        #     res.append(jl_res)
        # except:
        #     logging.error('{0:s} JL(1.5) calc failed'.format(stk))
        #     tb.print_exc()
        # try:
        #     ana_res = self.ana_report(stk, ana_s_date, crt_date)
        #     res.append(ana_res)
        # except:
        #     logging.error('Failed to analyze {0:s}'.format(stk))
        #     tb.print_exc()
        return res


    """ Assume here that setup_df has the following columns: industry
    group, sector, cs_45, tm, and setup_df is already sorted following
    direction and cs_45. If triggered is True, then this is a
    real-time intraday report, and we need to also provide a table
    sorted by tm.  Otherwise, only provide setup charts sorted by
    CS_45.  TODO: retrieve JL setups for last 20 days and plot the
    channels for those setups on chart """
    def get_triggered_report(self, crt_date, setup_df, triggered=False):
        start_date = stxcal.move_busdays(crt_date, -90)
        logging.info(f'setup_df has {len(setup_df)} rows')
        res = []
        if triggered:
            res.extend(self.add_timeline_report(setup_df))
        up_setup_df = setup_df.query("direction=='U'").copy()
        up_setup_df.sort_values(by=['value'], ascending=False, inplace=True)
        down_setup_df = setup_df.query("direction=='D'").copy()
        res.append('<h3>{0:d} UP Setups</h3>'.format(len(up_setup_df)))
        for _, row in up_setup_df.iterrows():
            # res.extend(self.trigger_setup_report(row, start_date,
            #                                      crt_date))
            res.extend(self.setup_report1(row, crt_date))
        res.append('<h3>{0:d} DOWN Setups</h3>'.format(len(down_setup_df)))
        for _, row in down_setup_df.iterrows():
            res.extend(self.trigger_setup_report(row, start_date,
                                                 crt_date))
        return res

    def index_report(self, crt_date):
        s_date = stxcal.move_busdays(crt_date, -90)
        jl_s_date = stxcal.move_busdays(crt_date, -350)
        res = []
        res.append('<h3>Index report</h3>')
        for index in ['^GSPC', '^IXIC', '^DJI']:
            ts, title = self.get_ts_and_title(index, s_date, crt_date)
            stk_plot = StxPlot(ts, title, s_date, crt_date)
            stk_plot.plot_to_file()
            res.append(f'<h4>{index}</h4>')
            res.append(f'<img src="/tmp/{index}.png" alt="{index}">')
            try:
                jl_res = StxJL.jl_report(index, jl_s_date, crt_date, 1.0)
                res.append(jl_res)
            except:
                logging.error(f'{index} JL(1.0) calc failed')
                tb.print_exc()
                try:
                    jl_res = StxJL.jl_report(index, jl_s_date, crt_date, 2.0)
                    res.append(jl_res)
                except:
                    logging.error(f'{index} JL(2.0) calc failed')
                    tb.print_exc()
        return res

    def get_report(self, crt_date, setup_df, isd):
        s_date = stxcal.move_busdays(crt_date, -50)
        ana_s_date = stxcal.move_busdays(crt_date, -20)
        res = []
        logging.info(f'setup_df has {len(setup_df)} rows')
        up_setup_df = setup_df.query("direction=='U'").copy()
        up_setup_df.sort_values(by=['value'], ascending=False, inplace=True)
        down_setup_df = setup_df.query("direction=='D'").copy()
        res.append('<h3>{0:d} UP Setups</h3>'.format(len(up_setup_df)))
        for _, row in up_setup_df.iterrows():
            setup_len = row.info.get('length', 0)
            start_date = stxcal.move_busdays(crt_date, -setup_len - 3)
            res.extend(self.setup_report(row, start_date, ana_s_date,
                                         crt_date, isd))
        res.append('<h3>{0:d} DOWN Setups</h3>'.format(len(down_setup_df)))
        for _, row in down_setup_df.iterrows():
            setup_len = row.info.get('length', 0)
            start_date = stxcal.move_busdays(crt_date, -setup_len - 3)
            res.extend(self.setup_report(row, start_date, ana_s_date,
                                         crt_date, isd))
        return res

    def get_jl_setups(self, dt, eod):
        setups_indicators_date = dt if eod else stxcal.prev_busday(dt)
        q = sql.Composed([
            sql.SQL("SELECT time_setups.dt, time_setups.stk, "
                    "ind_groups.industry, ind_groups.sector, "
                    "time_setups.direction, time_setups.setup, "
                    "time_setups.tm, time_setups.info, indicators_1.name, "
                    "indicators_1.value, indicators_1.bucket_rank "
                    "FROM time_setups, indicators_1, ind_groups "
                    "WHERE time_setups.dt="),
            sql.Literal(dt),
            sql.SQL(" AND (time_setups.info->>'length')::int >= "),
            sql.Literal(20),
            sql.SQL(" AND ((setup="), sql.Literal("JL_SR"),
            sql.SQL(" AND (time_setups.info->>'num_sr')::int > "),
            sql.Literal(1),
            sql.SQL(") OR (setup IN ("),
            sql.SQL(', ').join([
                sql.Literal('JL_P'),
                sql.Literal('JL_B')
            ]),
            sql.SQL(')))'),
            sql.SQL(" AND indicators_1.dt="),
            sql.Literal(setups_indicators_date),
            sql.SQL(" AND time_setups.stk=indicators_1.ticker "
                    "AND indicators_1.name="),
            sql.Literal('CS_45'),
            sql.SQL(" AND ind_groups.dt = "),
            sql.Literal(stxcal.prev_expiry(dt)),
            sql.SQL( " AND ind_groups.stk=time_setups.stk "
                     "ORDER BY time_setups.direction, indicators_1.value")
        ])
        logging.info(f'jl setups sql = {q.as_string(stxdb.db_get_cnx())}')
        df = pd.read_sql(q, stxdb.db_get_cnx())
        return df

    def do_analysis(self, crt_date, max_spread, indicators, eod):
        if eod:
            logging.info(f'indicators = {indicators}')
            for indicator in indicators:
                logging.info(f'indicator = {indicator}, crt_date = {crt_date}')
                stxgrps.calc_group_indicator(indicator, crt_date)
        isd = self.get_industries_sectors(crt_date)
        spreads = self.get_opt_spreads(crt_date, eod)
        df_trigger_today = self.get_triggered_setups(crt_date, eod, triggered=True)
        df_jl = self.get_jl_setups(crt_date, eod)
        logging.info(f'Found {len(df_trigger_today)} triggered setups and '
                     f'{len(df_jl)} JL setups for {crt_date}')
        if df_trigger_today.empty and df_jl.empty:
            logging.error(f'No triggered/JL setups for {crt_date}.  '
                          'Exiting...')
            return None
        # df_1 = self.filter_spreads(df_1, spreads, max_spread)
        # df_3 = self.filter_spreads(df_3, spreads, max_spread)
        df_trigger_today = self.add_indicators(df_trigger_today, crt_date,
                                               indicators, eod)
        df_jl = self.add_indicators(df_jl, crt_date, indicators, eod)
        res = ['<html>', self.report_style, '<body>']
        res.append(f"Report created on "
                   f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}")
        res.extend(self.index_report(crt_date))
        res.append(f'<h2>TODAY - {crt_date}</h2>')
        res.extend(self.get_triggered_report(crt_date, df_trigger_today,
                                             triggered=True))
        if eod:
            df_trigger_tomorrow = self.get_triggered_setups(
                crt_date, eod, triggered=False)
            df_trigger_tomorrow = self.add_indicators(
                df_trigger_tomorrow, crt_date, indicators, eod)
            next_date = stxcal.next_busday(crt_date)
            res.append(f'<h2>TOMMORROW - {next_date}</h2>')
            res.extend(self.get_triggered_report(
                crt_date, df_trigger_tomorrow))
        res.append(f'<h2>JL - {crt_date}</h2>')
        res.extend(self.get_report(crt_date, df_jl, isd))
        res.append('</body>')
        res.append('</html>')
        with open('/tmp/x.html', 'w') as html_file:
            html_file.write('\n'.join(res))
        logging.info('Generated HTML report')
        '''If this run every 10 minutes, overwrite intraday report'''
        suffix = 'EOD' if eod else 'ID'
        pdf_fname = f'{crt_date}_{suffix}.pdf'
        logging.info(f'PDF report file name: {pdf_fname}')
        pdf_filename = os.path.join(self.report_dir, pdf_fname)
        HTML(filename='/tmp/x.html').write_pdf(pdf_filename)
        logging.info(f'Saved report locally in {pdf_filename}')
        return pdf_filename

    def ana_report(self, stk, start_date, end_date):
        res = '<table><tr>'
        jl_start_date = stxcal.move_busdays(end_date, -8)
        # add the A/D setups table
        res += '<td><table>'
        qad = sql.Composed(
            [sql.SQL('select * from time_setups where dt between '),
             sql.Literal(start_date),
             sql.SQL(' and '),
             sql.Literal(end_date),
             sql.SQL(' and setup in ('),
             sql.SQL(',').join([sql.Literal('Gap'),
                                sql.Literal('SC'),
                                sql.Literal('RDay')]),
             sql.SQL(') and stk='),
             sql.Literal(stk),
             sql.SQL(' order by dt, direction, setup')])
        df_ad = pd.read_sql(qad, stxdb.db_get_cnx())
        for _, row in df_ad.iterrows():
            res += '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td>'\
                '</tr>'.format(row['dt'].strftime('%b %d'), row['setup'],
                               row['direction'], row['tm'])
        res += '</td></table>'
        # add the JL setups table
        res += '<td><table>'
        qjl = sql.Composed([
            sql.SQL('SELECT * FROM time_setups WHERE dt BETWEEN '),
            sql.Literal(jl_start_date),
            sql.SQL(' AND '),
            sql.Literal(end_date),
            sql.SQL(" AND (info->>'length')::int >= "), sql.Literal(25),
            sql.SQL(" AND ((setup="), sql.Literal("JL_SR"),
            sql.SQL(" AND (info->>'num_sr')::int > "), sql.Literal(1),
            sql.SQL(") OR (setup IN ("),
            sql.SQL(', ').join([
                sql.Literal('JL_P'),
                sql.Literal('JL_B')
            ]),
            sql.SQL('))) AND stk='),
            sql.Literal(stk),
            sql.SQL(' ORDER BY dt, direction, setup, factor')
        ])
        df_jl = pd.read_sql(qjl, stxdb.db_get_cnx())
        for _, row in df_jl.iterrows():
            res += '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td>'\
                '<td>{}</td></tr>'.format(row['dt'].strftime('%b %d'),
                                          row['setup'], row['direction'],
                                          row['factor'], row['tm'])
        res += '</table></td>'
        # add the candlesticks setups table
        res += '<td><table>'
        qcs = sql.Composed(
            [sql.SQL('select * from time_setups where dt between '),
             sql.Literal(start_date),
             sql.SQL(' and '),
             sql.Literal(end_date),
             sql.SQL(' and setup in ('),
             sql.SQL(',').join([sql.Literal('EngHarami'),
                                sql.Literal('Cbs'),
                                sql.Literal('3out'),
                                sql.Literal('3'),
                                sql.Literal('Kicking'),
                                sql.Literal('Piercing'),
                                sql.Literal('Engulfing'),
                                sql.Literal('Star')]),
             sql.SQL(') and stk='),
             sql.Literal(stk),
             sql.SQL(' order by dt, direction, setup')])
        df_cs = pd.read_sql(qcs, stxdb.db_get_cnx())
        for _, row in df_cs.iterrows():
            res += '<tr><td>{}</td><td>{}</td><td>{}</td></tr>'.format(
                row['dt'].strftime('%b %d'), row['setup'], row['direction'])
        res += '</td></table>'
        res += '</tr></table>'
        return res

    def update_local_directory(self, crt_date):
        today_date = stxcal.today_date()
        start_of_current_month = f'{today_date[:8]}01'
        prev_month_date = stxcal.prev_busday(start_of_current_month)
        start_of_prev_month = f'{prev_month_date[:8]}01'
        zipfile_name = os.path.join(
            self.report_dir,
            f'{stxcal.prev_busday(start_of_prev_month)}.zip'
        )
        logging.info(f'Archive all reports prior to {start_of_prev_month} '
                     f'in {zipfile_name}')
        pdf_file_list = glob.glob(os.path.join(self.report_dir, '*.pdf'))
        zipfile_open_mode = 'a' if os.path.isfile(zipfile_name) else 'w'
        num_archived_pdfs = 0
        z = zipfile.ZipFile(zipfile_name, zipfile_open_mode)
        for pdf_file in pdf_file_list:
            short_filename = pdf_file.split(os.path.sep)[-1]
            if short_filename < start_of_prev_month:
                z.write(pdf_file)
                num_archived_pdfs += 1
                os.remove(pdf_file)
        z.close()
        logging.info(f'Archived {num_archived_pdfs} PDF reports '\
                     f'in {zipfile_name}')

    def get_industries_sectors(self, dt):
        ind_sector_dct = {}
        db_date = stxcal.prev_expiry(dt)
        q = sql.Composed([
            sql.SQL("SELECT stk, industry, sector FROM ind_groups "),
            sql.SQL("WHERE source = "),
            sql.Literal('yf'),
            sql.SQL(' AND dt = '),
            sql.Literal(db_date)
        ])
        res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
        if not res:
            q = sql.Composed([
                sql.SQL("SELECT dt FROM ind_groups WHERE dt >= "),
                sql.Literal(dt),
                sql.SQL(" ORDER BY dt LIMIT 1")
            ])
            upper_date = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
            q = sql.Composed([
                sql.SQL("SELECT dt FROM ind_groups WHERE dt < "),
                sql.Literal(dt),
                sql.SQL(" ORDER BY dt LIMIT 1")
            ])
            lower_date = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
            if not upper_date and not lower_date:
                return ind_sector_dct
            if not upper_date:
                db_date = lower_date[0]
            else:
                if not lower_date:
                    db_date = upper_date[0]
                else:
                    lower_num = stxcal.num_busdays(lower_date[0], dt)
                    upper_num = stxcal.num_busdays(dt, upper_date[0])
                    db_date = lower_date[0] if lower_num <= upper_num else \
                        upper_date[0]
            q = sql.Composed([
                sql.SQL("SELECT stk, industry, sector FROM ind_groups "),
                sql.SQL("WHERE source = "),
                sql.Literal('yf'),
                sql.SQL(' AND dt = '),
                sql.Literal(db_date)
            ])
            res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
        ind_sector_dct = {x[0]: [x[1], x[2]] for x in res}
        return ind_sector_dct


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--max_spread', type=int, default=33,
                        help='Maximum spread for leaders')
    parser.add_argument('-d', '--date', type=str, 
                        default=stxcal.current_busdate(hr=9),
                        help='Date to retrieve setups')
    parser.add_argument('-n', '--indicators', type=str, 
                        default='CS_10,CS_20,CS_45,OBV_10,OBV_20,OBV_45,'
                        'RS_10,RS_252,RS_4,RS_45',
                        help='Indicators to calculate')
    parser.add_argument('-e', '--eod', action='store_true',
                        help="Run EOD analysis")
    parser.add_argument('-i', '--intraday', action='store_true',
                        help="Run Intraday analysis")    
    parser.add_argument('-c', '--cron', action='store_true',
                        help="Flag invocation from cron job")
    parser.add_argument('-a', '--startdate', type=str,
                        help='Start date for batch runs')
    parser.add_argument('-z', '--enddate', type=str,
                        help='End date for batch runs')
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    analysis_type = 'Analysis'
    eod = False
    if args.cron:
        today_date = stxcal.today_date()
        if not stxcal.is_busday(today_date):
            logging.warn("stx_247 dont run on holidays ({0:s})".
                         format(today_date))
            sys.exit(0)
    if args.eod:
        analysis_type = 'EOD'
        eod = True
    if args.intraday:
        analysis_type = 'Intraday'
    if args.date:
        crt_date = args.date
    indicator_list = args.indicators.split(',')
    indicator_tenor_list = [x.split('_') for x in indicator_list]
    indicators_df = pd.DataFrame(indicator_tenor_list,
                                 columns=['indicator', 'tenor'])
    indicators_df['tenor'] = indicators_df['tenor'].astype('int')
    indicators_df.sort_values(by=['indicator', 'tenor'], inplace=True)
    indicator_names = sorted(indicators_df['indicator'].unique())
    indicator_tenors = sorted(indicators_df['tenor'].unique())
    stx_ana = StxAnalyzer(indicator_names, indicator_tenors)
    if args.startdate and args.enddate:
        logging.info(f'Run analysis from {args.startdate} to {args.enddate}')
        crs_date = args.startdate
        num = 0
        while crs_date <=args.enddate:
            logging.info(f'Run analysis for {crs_date}')
            pdf_report = stx_ana.do_analysis(crs_date, args.max_spread,
                                             indicator_list, True)
            if pdf_report is None:
                logging.error(f'No report generated for {crs_date}')
            stx_ana.update_local_directory(crs_date)
            crs_date = stxcal.next_busday(crs_date)
            num += 1
        logging.info(f'Ran EOD analysis for {num} days between '
                     f'{args.startdate} and {args.enddate}')
    else:
        logging.info(f'Run analysis for {crt_date}')
        pdf_report = stx_ana.do_analysis(crt_date, args.max_spread,
                                         indicator_list, eod)
        if pdf_report is None:
            logging.error(f'No report was generated for {crt_date}')
        stx_ana.update_local_directory(crt_date)
