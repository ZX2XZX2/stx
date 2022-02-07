import argparse
import datetime
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

class StxAnalyzer:
    def __init__(self):
        self.report_dir = os.path.join(os.getenv('HOME'), 'market')
        logging.info('PDF reports are stored locally in {0:s}'.
                     format(self.report_dir))
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
    def get_rs_stx(self, dt):
        q = sql.Composed([
                sql.SQL("select stk, indicators->>'rs' as rs, "
                        "indicators->>'rs_rank' as rs_rank from indicators"),
                sql.SQL(' where dt='),
                sql.Literal(dt),
                sql.SQL(' and stk not in (select * from excludes)')])
        rsdf = pd.read_sql(q, stxdb.db_get_cnx()) 
        rsdf[["rs", "rs_rank"]] = rsdf[["rs", "rs_rank"]].apply(pd.to_numeric)
        rsdf.sort_values(by=['rs'], ascending=False, inplace=True)
        return rsdf

    def get_triggered_setups(self, dt):
        q = sql.Composed([
                sql.SQL('select * from setups where dt='), sql.Literal(dt), 
                sql.SQL(' and setup in ('), sql.SQL(', ').join(
                    [sql.Literal('JC_5DAYS'), sql.Literal('JC_1234')]), 
                sql.SQL(') and triggered='), sql.Literal(True)])
        df = pd.read_sql(q, stxdb.db_get_cnx())
        return df

    def get_setups_for_tomorrow(self, dt):
        next_dt = stxcal.next_busday(dt)
        q = sql.Composed([
                sql.SQL('select * from setups where dt='), 
                sql.Literal(next_dt), sql.SQL(' and setup in ('), 
                sql.SQL(', ').join(
                    [sql.Literal('JC_5DAYS'), sql.Literal('JC_1234')]), 
                sql.SQL(')')])
        df = pd.read_sql(q, stxdb.db_get_cnx())
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
            sql.SQL("SELECT * FROM indicators_1 WHERE dt="),
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

    def rs_report(self, i, row, s_date, jl_s_date, ana_s_date, crt_date, isd):
        res = []
        stk = row['stk']
        stk_plot = StxPlot(stk, s_date, crt_date)
        stk_plot.plot_to_file()
        res.append(f"<h4>{i + 1}. {stk} {isd.get(stk, ['N/A', 'N/A'])}, "
                   f"RS={row['rs']}</h4>")
        res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.format(stk, stk))
        try:
            jl_res = StxJL.jl_report(stk, jl_s_date, crt_date, 1.5)
            res.append(jl_res)
        except:
            logging.error('JL(1.5) calc failed for {0:s}'.format(stk))
            tb.print_exc()
        try:
            ana_res = self.ana_report(stk, ana_s_date, crt_date)
            res.append(ana_res)
        except:
            logging.error('Failed to analyze {0:s}'.format(stk))
            tb.print_exc()
        return res

    def build_indicators_table(self, row):
        ind_tbl_dict = {}
        for k, v in row.items():
            if '--' in k:
                indicator_name, val_rank_bucket = k.split('--')
                ind_col_dict = ind_tbl_dict.get(indicator_name, {})
                ind_col_dict[val_rank_bucket] = v
                ind_tbl_dict[indicator_name] = ind_col_dict
        table_header = ['<tr>']
        table_value_row = ['<tr>']
        table_rank_row = ['<tr>']
        table_bucket_row = ['<tr>']
        for indicator_name, indicator_data in ind_tbl_dict.items():
            table_header.append(f'<th>{indicator_name}</th>')
            table_value_row.append(f"<td>{indicator_data.get('value')}</td>")
            table_rank_row.append(f"<td>{indicator_data.get('rank')}</td>")
            table_bucket_row.append(f"<td>{indicator_data.get('bucket')}</td>")
        table_header.append('</tr>')
        table_value_row.append('</tr>')
        table_rank_row.append('</tr>')
        table_bucket_row.append('</tr>')
        indicator_table = [
            '<table border="1">',
            ''.join(table_header),
            ''.join(table_value_row),
            ''.join(table_rank_row),
            ''.join(table_bucket_row),
            '</table>'
        ]
        return indicator_table

    def get_trend_lines(self, row):
        setup = row.setup
        """
        dt  | stk | setup | factor | direction | triggered | tm | info
        2022-01-26 | CSX | JL_P  |    100 | U         | t         | 20:00:00 | {"pivot": {"obv": 3, "date": "2022-01-25", "price": 3315, "state": "DT"}, "length": 26, "channel": {"p1": {"obv": -21, "date": "2021-12-17", "price": 3494, "state": "NRe"}, "p2": {"obv": 3, "date": "2022-01-24", "price": 3315, "state": "DT"}, "bound": "lower"}}
        """
        if setup == 'JL_P':
            d1 = row.info['channel']['p1']['date']
            p1 = row.info['channel']['p1']['price'] / 100.0
            d2 = row.info['pivot']['date']
            p2 = row.info['pivot']['price'] / 100.0
            return [(d1, p1), (d2, p2)]
        """
        dt | stk | setup | factor | direction | triggered | tm | info
        2022-01-26 | BJ  | JL_SR |    100 | D         | t         | 20:00:00 | {"sr": 6366, "vr": 155, "length": 49, "num_sr": 2, "sr_pivots": [{"obv": -20, "date": "2021-11-12", "price": 6366, "state": "UT"}, {"obv": -15, "date": "2021-12-01", "price": 6326, "state": "NRe"}, {"obv": -12, "date": "2022-01-24", "price": 6338, "state": "NRa"}]}
        """
        if setup == 'JL_SR':
            sr_pivots = row.info['sr_pivots']
            d1 = sr_pivots[0]['date']
            p1 = sr_pivots[0]['price'] / 100.0
            d2 = sr_pivots[-1]['date']
            p2 = sr_pivots[-1]['price'] / 100.0
            return [(d1, p1), (d2, p2)]
        """
        dt | stk | setup | factor | direction | triggered | tm | info
        2022-02-02 | AMD | JL_B  |    100 | U         | t         | 09:48:00 | {"vr": 26, "ipx": 12320, "len": 26, "obv1": -42, "obv2": -25, "slope": -128.91667175292969}
        """
        return None

    def setup_report(self, row, s_date, jl_s_date, ana_s_date, crt_date, isd):
        res = []
        try:
            stk = row['stk']
            trend_lines = self.get_trend_lines(row)
            stk_plot = StxPlot(stk, s_date, crt_date, trend_lines)
            stk_plot.plot_to_file()
            res.append(f"<h4>{stk} {isd.get(stk, ['N/A', 'N/A'])}</h4>")
            # res.append(f"<h4>{stk} "
            #     f"[{', '.join(sorted(stxetfs.stock_labels(stk)))}]</h4>")
            res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.
                       format(stk, stk))
            ts = StxTS(stk, s_date, crt_date)
            day_ix = ts.set_day(crt_date)
            if day_ix == -1:
                return []
            avg_volume = np.average(ts.df['v'].values[-20:])
            rgs = [max(h, c_1) - min(l, c_1)
                   for h, l, c_1 in zip(ts.df['hi'].values[-20:],
                                        ts.df['lo'].values[-20:],
                                        ts.df['c'].values[-21:-1])]
            avg_rg = np.average(rgs)
            res.append('<table border="1">')
            res.append('<tr><th>name</th><th>dir</th><th>spread'
                       '</th><th>avg_volume</th><th>avg_rg</th></tr>')
            res.append('<tr><td>{0:s}</td><td>{1:s}</td><td>{2:d}</td><td>'
                       '{3:,d}</td><td>{4:.2f}</td></tr>'.
                       format(stk, row['direction'], int(row['spread']),
                              int(1000 * avg_volume), avg_rg / 100))
            res.append('</table>')
            res.extend(self.build_indicators_table(row))
            # res.append('<table border="1">')
            # res.append('<tr><th>name</th><th>dir</th><th>spread'
            #            '</th><th>avg_volume</th><th>avg_rg</th><th>hi_act'
            #            '</th><th>rs</th><th>rs_rank</th></tr>')
            # res.append('<tr><td>{0:s}</td><td>{1:s}</td><td>{2:d}</td><td>'
            #            '{3:,d}</td><td>{4:.2f}</td><td>{5:d}</td>'
            #            '<td>{6:d}</td><td>{7:d}</td></tr>'.
            #            format(stk, row['direction'], int(row['spread']),
            #                   int(1000 * avg_volume), avg_rg / 100,
            #                   row['hi_act'], row['rs'], row['rs_rank']))
            # res.append('</table>')
        except:
            logging.error('Failed analysis for {0:s}'.format(stk))
            tb.print_exc()
            return []
        try:
            jl_res = StxJL.jl_report(stk, jl_s_date, crt_date, 1.5)
            res.append(jl_res)
        except:
            logging.error('{0:s} JL(1.5) calc failed'.format(stk))
            tb.print_exc()
        try:
            ana_res = self.ana_report(stk, ana_s_date, crt_date)
            res.append(ana_res)
        except:
            logging.error('Failed to analyze {0:s}'.format(stk))
            tb.print_exc()
        return res

    def get_report(self, crt_date, setup_df, isd, do_analyze):
        s_date = stxcal.move_busdays(crt_date, -50)
        jl_s_date = stxcal.move_busdays(crt_date, -350)
        ana_s_date = stxcal.move_busdays(crt_date, -20)
        res = []
        # rsdf = self.get_rs_stx(crt_date)
        if do_analyze:
            indexes = ['^GSPC', '^IXIC', '^DJI']
            for index in indexes:
                stk_plot = StxPlot(index, s_date, crt_date)
                stk_plot.plot_to_file()
                res.append('<h4>{0:s}</h4>'.format(index))
                res.append('<img src="/tmp/{0:s}.png" alt="{1:s}">'.
                           format(index, index))
                try:
                    jl_res = StxJL.jl_report(index, jl_s_date, crt_date, 1.0)
                    res.append(jl_res)
                except:
                    logging.error('{0:s} JL(1.0) calc failed'.format(index))
                    tb.print_exc()
                try:
                    jl_res = StxJL.jl_report(index, jl_s_date, crt_date, 2.0)
                    res.append(jl_res)
                except:
                    logging.error('{0:s} JL(2.0) calc failed'.format(index))
                    tb.print_exc()
                try:
                    ana_res = self.ana_report(index, ana_s_date, crt_date)
                    res.append(ana_res)
                except:
                    logging.error('Failed to analyze {0:s}'.format(index))
                    tb.print_exc()
        # setup_df = df.merge(rsdf)
        # setup_df = df
        logging.info(f'setup_df has {len(setup_df)} rows')
        up_setup_df = setup_df.query("direction=='U'").copy()
        up_setup_df.sort_values(by=['tm'], ascending=False, inplace=True)
        down_setup_df = setup_df.query("direction=='D'").copy()
        down_setup_df.sort_values(by=['tm'], ascending=False, inplace=True)
        res.append('<h3>{0:d} UP Setups</h3>'.format(len(up_setup_df)))
        for _, row in up_setup_df.iterrows():
            setup_len = row.info['length']
            start_date = stxcal.move_busdays(crt_date, -setup_len - 3)
            res.extend(self.setup_report(row, start_date, jl_s_date, ana_s_date,
                                         crt_date, isd))
        res.append('<h3>{0:d} DOWN Setups</h3>'.format(len(down_setup_df)))
        for _, row in down_setup_df.iterrows():
            res.extend(self.setup_report(row, s_date, jl_s_date, ana_s_date,
                                         crt_date, isd))
        # if do_analyze:
        if False:
            rsbest = rsdf.query('rs_rank==99').copy()
            rsworst = rsdf.query('rs_rank==0').copy()
            rsworst.sort_values(by=['rs'], ascending=True, inplace=True)
            res.append('<h3>RS Leaders</h3>')
            for i, (_, row) in enumerate(rsbest.iterrows()):
                res.extend(self.rs_report(i, row, s_date, jl_s_date,
                                          ana_s_date, crt_date, isd))
            res.append('<h3>RS Laggards</h3>')
            for i, (_, row) in enumerate(rsworst.iterrows()):
                res.extend(self.rs_report(i, row, s_date, jl_s_date,
                                          ana_s_date, crt_date, isd))

        return res

    def get_jl_setups(self, dt):
        q = sql.Composed([
            sql.SQL("SELECT * FROM time_setups WHERE dt="),
            sql.Literal(dt),
            sql.SQL(" AND (info->>'length')::int >= "), sql.Literal(25),
            sql.SQL(" AND ((setup="), sql.Literal("JL_SR"),
            sql.SQL(" AND (info->>'num_sr')::int > "), sql.Literal(1),
            sql.SQL(") OR (setup IN ("),
            sql.SQL(', ').join([
                sql.Literal('JL_P'),
                sql.Literal('JL_B')
            ]),
            sql.SQL(')))')
        ])
        logging.info(f'q = {q.as_string(stxdb.db_get_cnx())}')
        df = pd.read_sql(q, stxdb.db_get_cnx())
        return df

    def do_analysis(self, crt_date, max_spread, indicators, eod):
        isd = self.get_industries_sectors(crt_date)
        spreads = self.get_opt_spreads(crt_date, eod)
        # df_1 = self.get_triggered_setups(crt_date)
        df_3 = self.get_jl_setups(crt_date)
        if df_3.empty:
            logging.error(f'No JL setups for {crt_date}.  Exiting...')
            return None
        logging.info(f'Found {len(df_3)} JL setups')
        # if df_1.empty and df_3.empty:
        #   logging.error(f'No triggered/JL setups for {crt_date}.  Exiting...')
        #     return None
        # self.get_high_activity(crt_date, df_1)
        # self.get_high_activity(crt_date, df_3)
        # df_1 = self.filter_spreads(df_1, spreads, max_spread)
        df_3 = self.filter_spreads(df_3, spreads, max_spread)
        df_3 = self.add_indicators(df_3, crt_date, indicators, eod)
        res = ['<html>', self.report_style, '<body>']
        res.append('<h3>TODAY - {0:s}</h3>'.format(crt_date))
        # res.extend(self.get_report(crt_date, df_1, isd, True))
        # if eod:
        #     df_2 = self.get_setups_for_tomorrow(crt_date)
        #     next_date = stxcal.next_busday(crt_date)
        #     self.get_high_activity(crt_date, df_2)
        #     df_2 = self.filter_spreads(df_2, spreads, max_spread)
        #     res.append('<h3>TOMMORROW - {0:s}</h3>'.format(next_date))
        #     res.extend(self.get_report(crt_date, df_2, isd, False))
        res.append('<h3>JL - {0:s}</h3>'.format(crt_date))
        res.extend(self.get_report(crt_date, df_3, isd, True))
        res.append('</body>')
        res.append('</html>')
        with open('/tmp/x.html', 'w') as html_file:
            html_file.write('\n'.join(res))
        logging.info('Generated HTML report')
        time_now = datetime.datetime.now()
        time_now_date = '{0:d}-{1:02d}-{2:02d}'.format(time_now.year, 
                                                       time_now.month, 
                                                       time_now.day)
        '''If this run every 10 minutes, overwrite intraday report'''
        suffix = 'EOD' if eod else 'ID'
        pdf_fname = '{0:s}_{1:s}.pdf'.format(crt_date, suffix)
        logging.info('PDF report file name: {0:s}'.format(pdf_fname))
        pdf_filename = os.path.join(self.report_dir, pdf_fname)
        HTML(filename='/tmp/x.html').write_pdf(pdf_filename)
        logging.info('Saved report locally in {0:s}'.format(pdf_filename))
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
                        help='Date to retrieve setups')
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
    stx_ana = StxAnalyzer()
    if args.startdate and args.enddate:
        logging.info('Running analysis from {args.startdate} to {args.enddate}')
        crs_date = args.startdate
        num = 0
        while crs_date <=args.enddate:
            logging.info(f'Running analysis for {crs_date}')
            pdf_report = stx_ana.do_analysis(crs_date, args.max_spread,
                                             indicator_list, True)
            if pdf_report is None:
                logging.error(f'No report was generated for {crs_date}')
            stx_ana.update_local_directory(crs_date)
            crs_date = stxcal.next_busday(crs_date)
            num += 1
        logging.info(f'Ran EOD analysis for {num} days between '
                     f'{args.startdate} and {args.enddate}')
    else:
        logging.info(f'Running analysis for {crt_date}')
        pdf_report = stx_ana.do_analysis(crt_date, args.max_spread,
                                         indicator_list, eod)
        if pdf_report is None:
            logging.error(f'No report was generated for {crt_date}')
        stx_ana.update_local_directory(crt_date)
