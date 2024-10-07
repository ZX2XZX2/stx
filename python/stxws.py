import ast
import ctypes
import datetime
import json
import logging
import os
import pandas as pd
import polars as pl
from psycopg2 import sql
import stxcal
import stxdb
from stxindicators import indicator_filter
import traceback as tb

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)s '
    '[%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
from flask import Flask, render_template, request, url_for, flash
import matplotlib
from stxplotbin import StxPlotBin
matplotlib.use('Agg')
from stxplot import StxPlot
from stxplotid import StxPlotID

ixxx = 0
refresh = 1 # refrsh time in minutes. for realtime, it is 5 minutes

app = Flask(__name__)

market_date = '2023-05-01'
market_time = '15:45'

frequencydict = {
    '5min': '5min',
    '10min': '10min',
    '15min': '15min',
    '30min': '30min',
    '60min': '60min'
}

_lib_name = os.path.join(os.sep, 'usr', 'local', 'bin', 'stx_lib.so')
_lib = ctypes.CDLL(_lib_name)


@app.route('/')
def index():
    charts = []
    date_dict = {}
    end_date, end_time = stxcal.current_intraday_busdatetime()
    start_date = stxcal.move_busdays(end_date, -90)
    for stxindex in ['^GSPC', '^IXIC', '^DJI']:
        sp = StxPlot(None, stxindex, start_date, end_date, stk=stxindex)
        date_dict[stxindex] = str(sp.ts.df.index[sp.ts.l - 1].date())
        chartdict = { 'figdata_png': sp.b64_png() }
        charts.append(chartdict)
    return render_template('indexes.html', charts=charts, date_dict=date_dict)


@app.route('/indexes')
def show_indexes():
    charts = []
    date_dict = {}
    end_date, end_time = stxcal.current_intraday_busdatetime()
    start_date = stxcal.move_busdays(end_date, -90)
    for stxindex in ['^GSPC', '^IXIC', '^DJI']:
        sp = StxPlot(None, stxindex, start_date, end_date, stk=stxindex)
        date_dict[stxindex] = str(sp.ts.df.index[sp.ts.l - 1].date())
        chartdict = { 'figdata_png': sp.b64_png() }
        charts.append(chartdict)
    return render_template('indexes.html', charts=charts, date_dict=date_dict)


@app.route('/charts', methods=('GET', 'POST'))
def charts():
    charts = []
    stks = ''
    dt, end_time = stxcal.current_intraday_busdatetime()
    num_days = 90
    if request.method == 'POST':
        stks = request.form['stocks']
        dt = request.form['datetime']
        num_days = int(request.form['num_days'])
        if not stks:
            flash('Stocks are required!')
        elif not dt:
            flash('Date is required!')
        else:
            stk_list = stks.split(' ')
            if request.form['action'] == 'Next':
                dt = stxcal.next_busday(dt)
            end_date = dt
            start_date = stxcal.move_busdays(end_date, -num_days)
            for stk in stk_list:
                sp = StxPlot(None, stk, start_date, end_date, stk=stk)
                chartdict = { 'figdata_png': sp.b64_png() }
                charts.append(chartdict)
    return render_template('charts.html', charts=charts, stx=stks, dt=dt,
                           num_days=num_days)


@app.route('/idcharts', methods=('GET', 'POST'))
def idcharts():
    charts = []
    stks = ''
    end_date, end_time = stxcal.current_intraday_busdatetime()
    num_days = 5
    freq = '5min'
    if request.method == 'POST':
        stks = request.form['stocks']
        end_date = request.form['dt_date']
        end_time = request.form['dt_time']
        end_dt = f'{end_date} {end_time}'
        num_days = int(request.form['num_days'])
        freq = request.form['frequency']
        if not stks:
            flash('Stocks are required!')
        elif not end_dt:
            flash('Date is required!')
        else:
            if request.form['action'] == 'Next':
                end_date, end_time = stxcal.next_intraday(end_dt)
                end_dt = f'{end_date} {end_time}'
            stk_list = stks.split(' ')
            start_date = stxcal.move_busdays(end_date, -num_days + 1)
            start_dt = f'{start_date} 09:30'
            frequency = int(freq[:-3])
            for stk in stk_list:
                sp = StxPlotID(None, start_dt, end_dt, stk, frequency)
                chartdict = { 'figdata_png': sp.b64_png() }
                charts.append(chartdict)
    return render_template(
        'idcharts.html', charts=charts, stx=stks,
        dt_date=end_date, dt_time=end_time, num_days=num_days,
        frequencydict=frequencydict, freq=freq)


@app.route('/analysis', methods=('GET', 'POST'))
def analysis():
    logging.info('Start analysis')
    charts = []
    stks = ''
    dt_date, dt_time = stxcal.current_intraday_busdatetime()
    eod_days = 90
    id_days = 5
    freq = '5min'
    if request.method == 'POST':
        stks = request.form['stocks']
        dt_date = request.form['dt_date']
        dt_time = request.form['dt_time']
        try:
            eod_days = int(request.form['eod_days'])
        except:
            logging.warn(f'No EOD days input, using default value {eod_days}')
        try:
            id_days = int(request.form['id_days'])
        except:
            logging.warn(f'No ID days input, using default value {id_days}')

        end_dt = f'{dt_date} {dt_time}:00'
        end_date = dt_date
        freq = request.form['frequency']
        if not stks:
            flash('Stocks are required!')
        else:
            stk_list = stks.split(' ')
            if request.form['action'] == 'Next':
                end_date, end_time = stxcal.next_intraday(end_dt)
                end_dt = f'{end_date} {end_time}'
                dt_date = end_date
                dt_time = end_time
            charts = generate_charts(None, stk_list, end_dt, eod_days, id_days,
                                     freq)
    return render_template('analysis.html', charts=charts, stx=stks,
                           dt_date=dt_date, dt_time=dt_time,
                           eod_days=eod_days, id_days=id_days, freq=freq,
                           frequencydict=frequencydict)


def get_current_price(stk, mkt_dt):
    sql_cmd = sql.Composed([
        sql.SQL("SELECT "), sql.Identifier('c'), sql.SQL(" FROM "),
        sql.Identifier('intraday'), sql.SQL(" WHERE "),
        sql.Identifier('stk'), sql.SQL('='), sql.Literal(stk),
        sql.SQL(" AND "),
        sql.Identifier('dt'), sql.SQL("="), sql.Literal(mkt_dt)
    ])
    logging.info(sql_cmd.as_string(stxdb.db_get_cnx()))
    c_res = stxdb.db_read_cmd(sql_cmd.as_string(stxdb.db_get_cnx()))
    current_price = c_res[0][0]
    return current_price


def get_stop_loss_target(stk, risk_dt, risk_date, direction):
    sql_cmd = sql.Composed([
        sql.SQL("SELECT * FROM "), sql.Identifier('stx_risk'),
        sql.SQL(" WHERE "),
        sql.Identifier('stk'), sql.SQL("="), sql.Literal(stk),
        sql.SQL(" AND DATE("), sql.Identifier('dt'), sql.SQL(")="),
        sql.Literal(risk_date), sql.SQL(" AND "), sql.Identifier('dt'),
        sql.SQL("<="), sql.Literal(risk_dt), sql.SQL( "AND "),
        sql.Identifier('direction'), sql.SQL("="), sql.Literal(direction),
        sql.SQL(" ORDER BY "), sql.Identifier('dt'),
        sql.SQL(" DESC LIMIT 1")
    ])
    risk_res = stxdb.db_read_cmd(sql_cmd.as_string(stxdb.db_get_cnx()))
    if len(risk_res) == 0:
        return 0, 0
    stop_loss = risk_res[0][4]
    target = risk_res[0][5]
    return stop_loss, target


def get_trades(mkt_name, dt_date, dt, stk_filter, in_out):
    act_dir = -1 if in_out == 'in' else 1
    sql_cmd = f"SELECT stk, direction, SUM(quantity) AS {in_out}_shares, "\
        f"SUM(price * quantity) / SUM(quantity) AS {in_out}_price FROM trades "\
        f"WHERE market = '{mkt_name}' AND DATE(dt) = '{dt_date}' "\
        f"AND dt <= '{dt}' {stk_filter} AND action * direction = {act_dir} "\
        f"GROUP BY stk, direction"
    df = pd.read_sql(sql_cmd, stxdb.db_get_cnx())
    return df


def get_portfolio(mkt_name, stx, mkt_dt):
    dt_date, _ = mkt_dt.split()
    stk_filter = "" if stx == '*' else f" AND stk='{stx}' "
    # get initiation trades
    df_in = get_trades(mkt_name, dt_date, mkt_dt, stk_filter, 'in')
    if len(df_in) == 0:
        return []
    # get closure trades
    df_out = get_trades(mkt_name, dt_date, mkt_dt, stk_filter, 'out')
    # get active trades (initiated trades not closed yet)
    df_pf = df_in.merge(df_out, on='stk', how='outer')
    df_pf = df_pf.fillna(0)
    df_pf['open_shares'] = df_pf['in_shares'] - df_pf['out_shares']
    df_pf['direction'] = df_pf.apply(lambda r:
        'Long' if r['direction_x'] == 1 else 'Short', axis=1)
    open_df = df_pf.query('open_shares>0').copy()
    # generate portfolio list
    pf_list = open_df[['stk', 'direction', 'open_shares', 'in_price',
                       'direction_x']].values.tolist()
    # process/enhance portfolio list
    for x in pf_list:
        print(f"start: x = {x}")
        stk = x[0]
        num_shares = x[2]
        in_price = x[3]
        direction = x.pop()
        current_price = get_current_price(stk, mkt_dt)
        x.append(current_price)
        pnl = int(direction * num_shares * (current_price - in_price))
        x.append(pnl)
        stop_loss, target = get_stop_loss_target(stk, mkt_dt, dt_date,
                                                 direction)
        x.append(target)
        x.append(stop_loss)
        if current_price != stop_loss:
            reward_risk_ratio = int(
                100 * (target - current_price) / (current_price - stop_loss)
            ) / 100.0
        else:
            reward_risk_ratio = -1
        x.append(reward_risk_ratio)
        print(f"end: x = {x}")
    return pf_list

def get_watchlist(mkt_name):
    q = sql.Composed([
        sql.SQL("SELECT "), sql.Identifier("stk"), sql.SQL(" FROM "),
        sql.Identifier("market_watch"), sql.SQL(" WHERE "),
        sql.Identifier("mkt"), sql.SQL("="), sql.Literal(mkt_name)
    ])
    res_db = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    watchlist = [x[0] for x in res_db]
    logging.info(f"The watchlist for market {mkt_name} is {watchlist}")
    return watchlist

def get_indicators(mkt_cache, mkt_date):
    min_activity = mkt_cache.get('min_activity', 10000)
    up_limit = mkt_cache.get('up_limit', 10)
    down_limit = mkt_cache.get('down_limit', 10)
    _lib.stx_eod_analysis.restype = ctypes.c_void_p
    ind_names = 'CS_45'
    res = _lib.stx_eod_analysis(
        ctypes.c_char_p(mkt_date.encode('UTF-8')),
        ctypes.c_char_p(ind_names.encode('UTF-8')),
        ctypes.c_int(min_activity),
        ctypes.c_int(up_limit),
        ctypes.c_int(down_limit),
    )
    res_json = json.loads(ctypes.cast(res, ctypes.c_char_p).value)
    _lib.stx_free_text.argtypes = (ctypes.c_void_p,)
    _lib.stx_free_text.restype = None
    _lib.stx_free_text(ctypes.c_void_p(res))
    indicators = res_json.get('indicators')
    return indicators

def set_indicator_charts(indicator_charts, indicator_list, mkt_name, mkt_date):
    df1 = indicator_filter(
        mkt_date,
        filter_criteria=ast.literal_eval(
            "[('RS_252', 90), ('RS_45', 90), ('CS_45', 70)]"
        ),
        min_activity=1000,
        min_close=1000,
        min_range=30,
        min_pct_rg=200,
    )
    stx_up = df1["stk"].unique().to_list()
    stx_down = df1["stk"].unique().to_list()

    up_charts = generate_charts(mkt_name, stx_up,
                                f'{mkt_date} 15:55:00', 45, 10,
                                '30min')
    down_charts = generate_charts(mkt_name, stx_down,
                                    f'{mkt_date} 15:55:00', 180, 2,
                                    '5min')
    indicator_charts["RCS"] = {
        "up": up_charts,
        "down": down_charts
    }
    with pl.Config(tbl_rows= -1, tbl_cols=-1, fmt_str_lengths=10000):
        print(df1)

def get_market(mkt_name, mkt_date, mkt_dt, mkt_cache, mkt_realtime):
    if isinstance(mkt_dt, datetime.datetime):
        mkt_dt = mkt_dt.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(mkt_date, datetime.date):
        mkt_date = mkt_date.strftime("%Y-%m-%d")
    eod_market = mkt_dt.endswith('16:00:00')
    mktdt = mkt_dt.replace('16:00:00', '15:55:00')
    portfolio = get_portfolio(mkt_name, '*', mktdt)
    # pf_list contains stock, in_price, stop_loss and target
    pf_list = [[x[0], x[3], x[7], x[6]] for x in portfolio]
    pf_charts = generate_charts(mkt_name, pf_list, mktdt, 0, 2, '5min') \
        if pf_list else []
    id_days1 = 10
    id_days2 = 2
    freq1 = '30min'
    freq2 = '5min'
    idx_list = ['SPY']
    idx_charts = generate_charts(mkt_name, idx_list, mktdt, 90, id_days1,
                                 freq1, id_days2, freq2)
    watchlist = get_watchlist(mkt_name)
    wl_charts = generate_charts(mkt_name, watchlist, mktdt, 90, id_days1,
                                freq1, id_days2, freq2) if watchlist else []        
    indicator_charts = {}
    if eod_market:
        _ = get_indicators(mkt_cache, mkt_date)
        i_list = ["RS_252", "RS_45", "CS_45"]
        set_indicator_charts(indicator_charts, i_list, mkt_name, mkt_date)
        refresh = ''
    else: # if before EOD, give enough time to perform indicator analysis
        refresh = 10 * 60000 if mkt_dt.endswith('15:55:00') else 60000

    return render_template(
        'eod.html',
        market_name=mkt_name,
        market_date=mkt_date,
        market_dt=mkt_dt,
        pf_charts=pf_charts,
        idx_charts=idx_charts,
        wl_charts=wl_charts,
        watchlist=watchlist,
        indicator_charts=indicator_charts,
        portfolio=portfolio,
        eod_market=eod_market,
        refresh=refresh
    )


def create_market(request):
    mkt_name = request.form.get('mkt_name')
    mkt_date = request.form.get('dt_date')
    mkt_dt = f"{mkt_date} 16:00:00"
    mkt_realtime = "TRUE" if request.form.get('realtime') else "FALSE"  
    q = sql.Composed([
        sql.SQL("SELECT "), sql.Identifier("mkt_name"), sql.SQL(" FROM "),
        sql.Identifier("market_caches"), sql.SQL(" WHERE "),
        sql.Identifier("mkt_name"), sql.SQL("="), sql.Literal(mkt_name)
    ])
    res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    if res:
        return f'A market named {mkt_name} already exists'
    mkt_cache = {"portfolio": [], "watchlist": [], "setups": {}}
    q = sql.Composed([
        sql.SQL("INSERT INTO "), sql.Identifier("market_caches"),
        sql.SQL(" VALUES ("), sql.SQL(',').join(
            [
                sql.Literal(mkt_name),
                sql.Literal(mkt_date),
                sql.Literal(mkt_dt),
                sql.Literal(json.dumps(mkt_cache)),
                sql.Literal(mkt_realtime)
            ]
        ),
        sql.SQL(")")
    ])
    try:
        stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
    except:
        return f'Market {mkt_name} create failed:<br>{tb.print_exc()}'
    return get_market(mkt_name, mkt_date, mkt_dt, mkt_cache, mkt_realtime)


def load_market(mkt_name):
    active_market_filename = os.path.join(os.sep, 'tmp', 'active_market.txt')
    with open(active_market_filename, 'w') as f:
        f.write(mkt_name)
    q = sql.Composed([
        sql.SQL("SELECT * FROM "), sql.Identifier("market_caches"),
        sql.SQL(" WHERE "),
        sql.Identifier("mkt_name"), sql.SQL("="), sql.Literal(mkt_name)
    ])
    res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    if not res:
        logging.error(f'Could not load market {mkt_name}, it does not exist')
        return f'Could not load market {mkt_name}, it does not exist'
    mkt_date = res[0][1]
    mkt_dt = res[0][2]
    mkt_cache = res[0][3]
    mkt_realtime = res[0][4]
    return get_market(mkt_name, mkt_date, mkt_dt, mkt_cache, mkt_realtime)


def advance_market(mkt_name):
    q = sql.Composed([
        sql.SQL("SELECT * FROM "), sql.Identifier("market_caches"),
        sql.SQL(" WHERE "),
        sql.Identifier("mkt_name"), sql.SQL("="), sql.Literal(mkt_name)
    ])
    res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    if not res:
        logging.error(f'Could not load market {mkt_name}, it does not exist')
        raise Exception(f'Could not load market {mkt_name}, it does not exist')
    mkt_date = res[0][1]
    mkt_dt = res[0][2]
    mkt_cache = res[0][3]
    mkt_realtime = res[0][4]
    if isinstance(mkt_dt, datetime.datetime):
        mkt_dt = mkt_dt.strftime("%Y-%m-%d %H:%M:%S")
    intraday_market = not mkt_dt.endswith('16:00:00')
    if intraday_market:
        try:
            mkt_date, mkt_dt = update_market_datetime(mkt_name, mkt_dt)
        except:
            raise
    return mkt_date, mkt_dt, mkt_cache, mkt_realtime


def update_market_datetime(mkt_name, mkt_dt):
    mdate, mdt = stxcal.next_market_datetime(mkt_dt)
    q = sql.Composed([
        sql.SQL("UPDATE "), sql.Identifier('market_caches'),
        sql.SQL(" SET "),
        sql.Identifier('mkt_date'), sql.SQL('='), sql.Literal(mdate),
        sql.SQL(","),
        sql.Identifier('mkt_update_dt'),sql.SQL("="),sql.Literal(mdt),

        sql.SQL(" WHERE "),
        sql.Identifier('mkt_name'),sql.SQL('='),sql.Literal(mkt_name)
    ])
    logging.info(q.as_string(stxdb.db_get_cnx()))
    try:
        stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
    except:
        raise
    return mdate, mdt


def jump_market(request):
    mkt_name = request.form.get('market_name')
    mkt_dt = request.form.get('market_dt')
    mkt_date = request.form.get('market_date')
    jump_time = request.form.get('mkt_time')
    # validate the time we want to jump to.  It must:
    # 1. be between (including) 09:30 and 15:55
    # 2. be later than the current time
    _, market_time = mkt_dt.split(' ')
    mt_tokens = market_time.split(':')
    market_hrs = int(mt_tokens[0])
    market_mins = int(mt_tokens[1])
    hrs, mins = jump_time.split(':')
    jump_tokens = jump_time.split(':')
    hrs = int(jump_tokens[0])
    mins = int(jump_tokens[1])
    if mins % 5 != 0: 
        mins += (5 - mins % 5)
    error_log = ""
    if hrs < 9 or hrs > 15 or (hrs == 9 and mins < 30):
        error_log += f"Input jump time {jump_time} outside 09:30--15:55 "
        logging.error(f"Input jump time {jump_time} outside 09:30--15:55")
    if market_hrs > hrs or (market_hrs == hrs and market_mins > mins):
        error_log += f" Input jump time {jump_time} less than " \
            f"current market time {market_time}"
        logging.error(f" Input jump time {jump_time} less than "
            f"current market time {market_time}")
    if error_log == "":
        mdt = f"{mkt_date} {hrs}:{mins}:00"
        q = sql.Composed([
            sql.SQL("UPDATE "), sql.Identifier('market_caches'),
            sql.SQL(" SET "), sql.Identifier('mkt_update_dt'),
            sql.SQL("="), sql.Literal(mdt), sql.SQL(" WHERE "),
            sql.Identifier('mkt_name'),sql.SQL('='),sql.Literal(mkt_name)
        ])
        try:
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            error_log = f"Failed to update date for market {mkt_name}"
        if error_log == "":
            return load_market(mkt_name)
    return error_log


@app.route('/market', methods=('GET', 'POST'))
def market():
    if request.method == 'POST':
        requested_action = request.form.get('action', 'NotFound')
        logging.info(f"The action requested is {requested_action}")
        if requested_action == 'create_market':
            return create_market(request)
        elif requested_action == 'load_market':
            mkt_name = request.form.get('market_name')
            return load_market(mkt_name)
        elif requested_action == 'start_intraday':
            mkt_name = request.form.get('market_name')
            mkt_dt = request.form.get('market_dt')
            # _, new_mkt_dt = stxcal.next_market_datetime(mkt_dt)
            try:
                update_market_datetime(mkt_name, mkt_dt)
            except:
                logging.error(f'{mkt_name} update_market_datetime() failed:'
                    f'<br>{tb.print_exc()}')
                return f'{mkt_name} update_market_datetime() failed:'\
                    f'<br>{tb.print_exc()}'
        elif requested_action == 'jump_intraday':
            return jump_market(request)
        else:
            return f'Could not find action {requested_action}'
    # TODO: use DB for this?
    mkt_name = None
    active_market_filename = os.path.join(os.sep, 'tmp', 'active_market.txt')
    if request.method == 'POST':
        mkt_name = request.form.get('market_name')
        with open(active_market_filename, 'w') as f:
            f.write(mkt_name)
    else:
        with open(active_market_filename, 'r') as f:
            mkt_name = f.read()
    if mkt_name is None:
        logging.error("Could not find an active market. Load a market first")
        return "Could not find an active market. Load a market first"
    try:
        mkt_date, mkt_dt, mkt_cache, mkt_realtime = advance_market(mkt_name)
        return get_market(mkt_name, mkt_date, mkt_dt, mkt_cache, mkt_realtime)
    except:
        logging.error(f'{mkt_name} advance_market() failed: {tb.print_exc()}')
        return f'{mkt_name} advance_market() failed: {tb.print_exc()}'


@app.route('/delete_market', methods=('GET', 'POST'))
def delete_market():
    mkt_name = request.form.get('market_name')
    q = sql.Composed([
        sql.SQL("DELETE FROM "), sql.Identifier("market_caches"),
        sql.SQL(" WHERE "),
        sql.Identifier("mkt_name"), sql.SQL("="), sql.Literal(mkt_name)
    ])
    try:
        stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
    except:
        return f'Market {mkt_name} delete failed:<br>{tb.print_exc()}'
    return f'Market {mkt_name} successfully deleted'

def generate_charts(mkt_name, stk_list, end_dt, eod_days, id_days, frequency,
                    id_days1=None, frequency1=None):
    charts = []
    freq = int(frequency[:-3])
    for stk in stk_list:
        if type(stk).__name__ == 'list':
            hlines = stk[1:]
            stk = stk[0]
            alines = []
        else:
            hlines = []
            alines = []
            sr_lines = get_sr(stk, mkt_name)
            for dt1, x1, dt2, x2 in sr_lines:
                if x1 == x2:
                    hlines.append(x1)
                else:
                    alines.append([(dt1, x1), (dt2, x2)])
        if eod_days != 0:
            sp = StxPlotBin(_lib, stk, eod_days, end_dt, intraday=False)

        spid = StxPlotBin(_lib, stk, id_days, end_dt, intraday=True,
            period=freq, hlines=hlines, alines=alines)
        chartdict = {
            'name': stk,
            'id_png': spid.b64_png()
        }
        if eod_days != 0:
            chartdict['eod_png'] = sp.b64_png()
        if id_days1 is not None:
            if frequency1 is not None:
                freq1 = int(frequency1[:-3])
            else:
                freq1 = int(frequency[:-3])
            spid1 = StxPlotBin(_lib, stk, id_days1, end_dt, intraday=True,
                               period=freq1, hlines=hlines, alines=alines)
            chartdict['id1_png'] = spid1.b64_png()
        charts.append(chartdict)
    return charts


@app.route('/market_test')
def market_test():
    exec_start_time = datetime.datetime.now()
    logging.debug(f'exec_start_time = {exec_start_time}')
    # read the market date from database
    global refresh
    global market_date
    global market_time

    logging.info('Start market')
    charts = []
    stks = 'SPY AAPL AMZN TSLA'
    eod_days = 90
    id_days_1 = 2
    freq_1 = '5min'
    id_days_2 = 20
    freq_2 = '60min'

    market_datetime = f'{market_date} {market_time}:00'
    logging.info(f'market_datetime = {market_datetime}')
    stk_list = stks.split(' ')
    market_date, market_time = stxcal.next_intraday(market_datetime)
    if market_time != "09:30":
        frequency_1 = int(freq_1[:-3])
        frequency_2 = int(freq_2[:-3])
        for stk in stk_list:
            # sp = StxPlotBin(_lib, stk, eod_days, market_datetime,
            #                 intraday=False)
            spid_1 = StxPlotBin(_lib, stk, id_days_1, market_datetime,
                                intraday=True, period=frequency_1)
            spid_2 = StxPlotBin(_lib, stk, id_days_2, market_datetime,
                                intraday=True, period=frequency_2)
            chartdict = {
                'eod_png': spid_1.b64_png(),
                'id_png': spid_2.b64_png()
            }
            charts.append(chartdict)
        exec_end_time = datetime.datetime.now()
        logging.debug(f'exec_end_time = {exec_end_time}')
        exec_time = exec_end_time - exec_start_time
        logging.debug(f'exec_time = {exec_time}')
        refresh_time = 60000 * refresh - int( 1000 *
            exec_time.total_seconds())
        logging.debug(f'refresh_time = {refresh_time}')
        return render_template('market.html', refresh=refresh_time,
                                charts=charts, datetime=market_datetime)
    return render_template('eod.html', datetime=market_datetime)


@app.route('/markets')
def markets():
    q = sql.Composed([
        sql.SQL("SELECT DISTINCT "), sql.Identifier("mkt_name"),
        sql.SQL(" FROM "), sql.Identifier("market_caches")
    ])
    cnx = stxdb.db_get_cnx()
    market_list = []
    with cnx.cursor() as crs:
        crs.execute(q.as_string(cnx))
        market_list = [x[0] for x in crs]
    mkt_name = ''
    dt_date, _ = stxcal.current_intraday_busdatetime()
    refresh_minutes = 5
    min_up_cs = 90
    max_down_cs = 10
    num_up = 5
    num_down = 5
    min_activity = 10000
    return render_template(
        'markets.html',
        market_list=market_list,
        mkt_name=mkt_name,
        dt_date=dt_date,
        refresh_minutes=refresh_minutes,
        min_up_cs=min_up_cs,
        max_down_cs=max_down_cs,
        num_up=num_up,
        num_down=num_down,
        min_activity=min_activity,
    )


@app.route('/wl_mgmt', methods=['POST'])
def watchlist_mgmt():
    stk = request.form.get("stk")
    market_name = request.form.get("market_name")
    action = request.form.get("action")
    if action == 'Add':
        q = sql.Composed([
            sql.SQL("INSERT INTO "), sql.Identifier("market_watch"),
            sql.SQL("VALUES ("), sql.SQL(',').join([
                sql.Literal(market_name),
                sql.Literal(stk)
            ]),
            sql.SQL(") ON CONFLICT DO NOTHING")
        ])
        try:
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            return f'Failed to add {stk} to {market_name} watchlist:'\
                f'<br>{tb.print_exc()}'
        return f"Added {stk} to {market_name} watchlist"
    else:
        q = sql.Composed([
            sql.SQL("DELETE FROM "), sql.Identifier("market_watch"),
            sql.SQL(" WHERE "),
            sql.Identifier("stk"), sql.SQL("="), sql.Literal(stk),
            sql.SQL("AND"),
            sql.Identifier("mkt"), sql.SQL("="), sql.Literal(market_name)
        ])
        try:
            print(f"DELETE SQL: {q.as_string(stxdb.db_get_cnx())}")
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            return f'Failed to remove {stk} from {market_name} watchlist:'\
                f'<br>{tb.print_exc()}'
        return f"Removed {stk} from {market_name} watchlist"

def gen_analysis_page(request):
    chart_params = {}
    stk = request.form['stk']
    dt = request.form['stk_dt'].replace("16:00:00", "15:55:00")
    market_name = request.form['market_name']
    logging.info(f"Market {market_name}: analyzing {stk} as of {dt}")
    include_spy = False
    stk_list = [stk]
    if include_spy:
        stk_list.append('SPY')
    logging.info(f"dt is of type: {dt.__class__}")
    charts = []
    dt_date, dt_time = stxcal.current_intraday_busdatetime()
    # TODO: later replace with market configuration
    eod_days = request.form.get("eod_days", "")
    id_days1 = request.form.get("id_days1", "")
    id_days2 = request.form.get("id_days2", "")
    freq1 = request.form.get("freq1", "30min")
    freq2 = request.form.get("freq2", "5min")
    try:
        eod_days = int(eod_days)
    except:
        eod_days = 120
    try:
        id_days1 = int(id_days1)
    except:
        id_days1 = 10
    try:
        id_days2 = int(id_days2)
    except:
        id_days2 = 2
    chart_params["eod_days"] = eod_days
    chart_params["id_days1"] = id_days1
    chart_params["id_days2"] = id_days2
    chart_params["freq1"] = freq1
    chart_params["freq2"] = freq2
    charts = generate_charts(market_name, stk_list, dt, eod_days, id_days1,
                             freq1, id_days2, freq2)
    return charts, dt, chart_params


@app.route('/stk_analysis', methods=['POST'])
def stk_analysis():
    mkt = request.form['market_name']
    charts, dt, chart_params = gen_analysis_page(request)
    stk = request.form['stk']
    jl_html = get_jl_html(stk, dt)
    return render_template(
        "stk_analysis.html",
        chart = charts[0],
        dt = dt,
        market_name = mkt,
        jl_html = jl_html,
        eod_days = chart_params["eod_days"],
        id_days1 = chart_params["id_days1"],
        id_days2 = chart_params["id_days2"],
        freq1 = chart_params["freq1"],
        freq2 = chart_params["freq2"],
    )

def init_trade(request):
    stk = request.form['stk']
    dt = request.form['dt']
    mkt = request.form['market_name']
    _lib.stx_get_trade_input.restype = ctypes.c_void_p
    res = _lib.stx_get_trade_input(
        ctypes.c_char_p(stk.encode('UTF-8')),
        ctypes.c_char_p(dt.encode('UTF-8'))
    )
    trade_input_str = ctypes.cast(res, ctypes.c_char_p).value
    logging.debug(f"trade_input_str = {trade_input_str}")
    trade_input_json = json.loads(trade_input_str)
    logging.debug(f"trade input: {json.dumps(trade_input_json, indent=2)}")
    _lib.stx_free_text.argtypes = (ctypes.c_void_p,)
    _lib.stx_free_text.restype = None
    _lib.stx_free_text(ctypes.c_void_p(res))
    current_price = trade_input_json['current_price']
    avg_volume = trade_input_json["avg_volume"]
    avg_range = trade_input_json["avg_range"]
    # TODO: replace 30000 with market parameters
    shares = avg_volume /100
    logging.info(f'volume size = {shares}')
    volatility_size = 30000 * 5 / (3 * avg_range)
    logging.info(f'volatility_size = {volatility_size}')
    if shares > volatility_size:
        shares = volatility_size
    trading_power_size = 30000 * 100 / current_price
    logging.info(f'trading_power_size = {trading_power_size}')
    if shares > trading_power_size:
        shares = trading_power_size
    shares = int(shares)
    logging.info(f'shares = {shares}')
    portfolio = get_portfolio(mkt, stk, dt.replace('16:00:00', '15:55:00'))
    logging.info(f'portfolio = {portfolio}')
    if portfolio:
        direction_str = portfolio[0][1]
        direction = 1 if direction_str == 'Long' else -1
        size = portfolio[0][2]
        in_price = portfolio[0][3]
        current_price = portfolio[0][4]
        target = portfolio[0][6]
        stop_loss = portfolio[0][7]
    else:
        direction = 0
        direction_str = ''
        target = ''
        stop_loss = ''
        in_price = current_price
        size = 0

    return render_template('trade.html', stk=stk, dt=dt, direction=direction,
        direction_str=direction_str, market_name=mkt, in_price=in_price,
        current_price=current_price, size=int(size), shares=shares,
        target=target, stop_loss=stop_loss, log_msg=''
    )

def check_trade_params(request):
    try:
        stop_loss = int(request.form.get('stop_loss'))
    except:
        stop_loss = 0
    try:
        target = int(request.form.get('target'))
    except:
        target = 0
    try:
        shares = int(request.form.get('shares'))
    except:
        shares = 0
    valid_params = (stop_loss != 0 and target != 0 and shares != 0)
    if not valid_params:
        log_msg = f"Invalid trade inputs: "\
            f"{'shares ' if shares == 0 else ''}"\
            f"{'stop-loss ' if stop_loss == 0 else ''}"\
            f"{'target ' if target == 0 else ''}"
    else:
        log_msg = ''
    return log_msg, stop_loss, target, shares

def get_risk(request):
    stk = request.form['stk']
    dt = request.form['dt']
    mkt = request.form['market_name']
    current_price = int(request.form.get('current_price'))
    in_price = int(request.form.get('in_price'))
    direction_str = request.form.get('direction_str')
    direction = int(request.form.get('direction'))
    size = int(request.form.get('size', 0))
    invalid_log, stop_loss, target, shares = check_trade_params(request)
    if not invalid_log:
        # TODO: replace with market params
        if direction == 0:
            max_loss_size = 30000 * 2 / (abs(stop_loss - current_price))
            if shares > max_loss_size:
                shares = max_loss_size
            if target < current_price:
                direction = -1
            else:
                direction = 1
        log_msg = ''
    return render_template('trade.html', stk=stk, dt=dt, direction=direction,
        direction_str=direction_str, market_name=mkt, in_price=in_price,
        current_price=current_price, size=int(size), shares=shares,
        target=target, stop_loss=stop_loss, log_msg=invalid_log
    )

def update_risk(mkt, stk, dt, direction, stop_loss, target):
    dt_date, _ = dt.split(' ')
    db_stop_loss, db_target = get_stop_loss_target(stk, dt, dt_date, direction)
    direction_str = 'Long' if direction == 1 else 'Short'
    description_str = f" for {stk} {direction_str} as of {dt}"
    if db_stop_loss == stop_loss and db_target == target:
        log_msg = f'No risk update needed{description_str}; ' \
            'stop_loss and target unchanged'
        logging.info(log_msg)
        return log_msg
    stop_loss_msg = f" stop-loss from {db_stop_loss} to {stop_loss}" \
        if db_stop_loss != stop_loss else ""
    target_msg = f" target from {db_target} to {target}" \
        if db_target != target else ""
    q = sql.Composed([
        sql.SQL("INSERT INTO "), sql.Identifier('stx_risk'),
        sql.SQL(" VALUES ("), sql.SQL(',').join([
            sql.Literal(mkt), sql.Literal(stk), sql.Literal(dt),
            sql.Literal(direction), sql.Literal(stop_loss),
            sql.Literal(target)
        ]), sql.SQL(")")
    ])
    try:
        stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        log_msg = f"Changed{stop_loss_msg}{target_msg}{description_str}"
        logging.info(log_msg)
    except:
        log_msg = f'Change{stop_loss_msg}{target_msg} failed' \
            f'{description_str}: {tb.print_exc()}'
        logging.error(log_msg)
    return log_msg

def risk_mgmt(request):
    stk = request.form['stk']
    dt = request.form['dt']
    mkt = request.form['market_name']
    current_price = int(request.form.get('current_price'))
    in_price = int(request.form.get('in_price'))
    size = int(request.form.get('size', 0))
    direction_str = request.form.get('direction_str', '')
    direction = 0
    if direction_str == 'Long':
        direction = 1
    elif direction_str == 'Short':
        direction = -1
    invalid_log, stop_loss, target, size = check_trade_params(request)
    if invalid_log:
        log_msg = invalid_log
    else:
        log_msg = update_risk(mkt, stk, dt, direction, stop_loss, target)
        # TODO: replace with market params
        max_loss_size = 30000 * 2 / (abs(stop_loss - in_price))
        if size > max_loss_size:
            size = max_loss_size
    return render_template('trade.html', stk=stk, dt=dt, market_name=mkt,
        direction=direction, direction_str=direction_str, size=int(size),
        in_price=in_price, shares=size, current_price=current_price,
        target=target, stop_loss=stop_loss, log_msg=log_msg
    )

def exec_trade(request, buy_sell):
    if buy_sell == 'buy':
        trade_direction = 1
    elif buy_sell == 'sell':
        trade_direction = -1
    else:
        trade_direction = 0
    if trade_direction == 0:
        return "Error: to place a trade select a direction (Long/Short)"
    stk = request.form['stk']
    dt = request.form['dt']
    mkt = request.form['market_name']
    current_price = int(request.form.get('current_price'))
    in_price = int(request.form.get('in_price'))
    size = int(request.form.get('size', 0))
    direction_str = request.form.get('direction_str', '')
    direction = 0
    if direction_str == 'Long':
        direction = 1
    elif direction_str == 'Short':
        direction = -1
    invalid_log, stop_loss, target, shares = check_trade_params(request)
    if invalid_log:
        return render_template(
            'trade.html', stk=stk, dt=dt, direction=direction,
            direction_str=direction_str, market_name=mkt, in_price=in_price,
            current_price=current_price, size=int(size), shares=shares,
            target=target, stop_loss=stop_loss, log_msg=invalid_log
        )
    logging.info(f"{dt}: {' buying' if trade_direction == 1 else ' selling'}"
                 f" {shares} shares of {stk} at {current_price}")
    # if add to an existing position, or open new position (if size == 0)
    if trade_direction == direction or size == 0:
        new_size = size + shares
        new_in_price = int((size * in_price + shares * current_price) / new_size)
        action = -1 * trade_direction
        if size == 0:
            new_direction = trade_direction
            new_direction_str = 'Long' if new_direction == 1 else 'Short'
        else:
            new_direction = direction
            new_direction_str = direction_str
    # (partially) close a position; if shares > size, this will only close
    # the existing position; need to place another trade to open position in
    # the opposite direction
    else:
        new_size = size - shares
        if new_size <= 0:
            new_direction = 0
            new_direction_str = ''
            new_size = 0
            shares = size
            new_in_price = current_price
        else:
            new_in_price = in_price
            new_direction = direction
            new_direction_str = direction_str
        action = trade_direction
    if new_size > 0:
        # update the risk (stop-loss and target in the overall direction)
        update_risk(mkt, stk, dt, new_direction, stop_loss, target)
    q = sql.Composed([
        sql.SQL("INSERT INTO "), sql.Identifier("trades"),
        sql.SQL(" VALUES ("), sql.SQL(',').join([
            sql.Literal(mkt), sql.Literal(stk), sql.Literal(dt),
            sql.Literal(trade_direction), sql.Literal(action),
            sql.Literal(current_price), sql.Literal(shares)
        ]), sql.SQL(")")
    ])
    try:
        stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
    except:
        log_msg = f'Trade failed:<br>{tb.print_exc()}'
        return render_template('trade.html', stk=stk, dt=dt, market_name=mkt,
            direction=direction, direction_str=direction_str, size=int(size),
            in_price=in_price, current_price=current_price, shares=shares,
            target=target, stop_loss=stop_loss, log_msg=log_msg
        )
    log_msg = f"{'Bought ' if trade_direction == 1 else 'Sold ' }, {shares} "\
        f"shares of {stk} at {current_price}"
    return render_template('trade.html', stk=stk, dt=dt, market_name=mkt,
        direction=new_direction, direction_str=new_direction_str,
        size=int(new_size), in_price=new_in_price, shares=shares,
        current_price=current_price, target=target, stop_loss=stop_loss,
        log_msg=log_msg
    )


@app.route('/trade', methods=['POST'])
def trade():
    requested_action = request.form.get('action')
    logging.info(f"The action requested is {requested_action}")
    if not requested_action:
        logging.error('No action specified; this page was reached by error')
        return 'No action specified; this page was reached by error'
    if requested_action == 'init_trade':
        return init_trade(request)
    elif requested_action == 'risk_mgmt':
        return risk_mgmt(request)
    elif requested_action == 'get_risk':
        return get_risk(request)
    elif requested_action == 'buy':
        return exec_trade(request, 'buy')
    elif requested_action == 'sell':
        return exec_trade(request, 'sell')
    else:
        logging.error(f"Wrong action '{requested_action}'specified; "
                       f"should be one of: 'init_trade', 'get_risk', "
                       "'risk_mgmt', 'buy' or 'sell'")
        return f"Wrong action '{requested_action}'specified; should be one "\
            "of 'init_trade', 'get_risk', 'risk_mgmt', 'buy', or 'sell'"

def get_jl_html(stk, dt):
    _lib.stx_get_jl.restype = ctypes.c_void_p    
    res = _lib.stx_get_jl(
        ctypes.c_char_p(stk.encode('UTF-8')),
        ctypes.c_char_p(dt.encode('UTF-8')),
        ctypes.c_bool(True),
    )
    jl_str = ctypes.cast(res, ctypes.c_char_p).value
    jl_json = json.loads(jl_str)
    logging.debug(f"JL records = {json.dumps(jl_json, indent=2)}")
    _lib.stx_free_text.argtypes = (ctypes.c_void_p,)
    _lib.stx_free_text.restype = None
    _lib.stx_free_text(ctypes.c_void_p(res))
    jl_table_style = ''
    jl_html = '\n'.join([
        '<table style="overflow-y: scroll; border-collapse: collapse;"',
        '  <thead>',
        '    <tr>',
        '      <th>Datetime</th>',
        '      <th>SRa</th>',
        '      <th>NRa</th>',
        '      <th>UT</th>',
        '      <th>DT</th>',
        '      <th>NRe</th>',
        '      <th>SRe</th>',
        '    </tr>',
        '  </thead>',
        '  <tbody>'
    ])
    for jl_rec in jl_json:
        jl_row_html = f"  <tr><td>"\
            f"{jl_rec.get('date', 'YYYY-mm-dd HH:MM:SS')}</td>"
        jl_state = jl_rec.get('state', 0)
        jl_pivot = jl_rec.get('pivot', False)
        for _ in range(jl_state):
            jl_row_html += '<td></td>'
        jl_style = ''
        if jl_state == 2:
            jl_style = ' style="color: #006600;"'
        elif jl_state == 3:
            jl_style = ' style="color: #660000;"'
        start_u = "<u>" if jl_pivot else ""
        end_u = "</u>" if jl_pivot else ""
        jl_row_html += f"<td{jl_style}>{start_u}{jl_rec.get('price', -1)}"\
            f"{end_u}</td>"
        for _ in range(5 - jl_state):
            jl_row_html += '<td></td>'
        jl_row_html += '</tr>\n'
        jl_html += jl_row_html
    jl_html += '</tbody>'
    jl_html += '</table>'
    return jl_html

def get_sr(stk, mkt):
    # get existing support/resistance from DB
    q = sql.Composed([
        sql.SQL("SELECT * FROM "), sql.Identifier("stx_sr"),
        sql.SQL(" WHERE "),
        sql.Identifier("mkt"),
        sql.SQL("="),
        sql.Literal(mkt),
        sql.SQL("AND"),
        sql.Identifier("stk"),
        sql.SQL("="),
        sql.Literal(stk),
        sql.SQL("ORDER BY"),
        sql.Identifier('px1')
    ])
    res_db = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    logging.info(f"res_db = {res_db}")
    sr_levels = [(x[2], x[3], x[4], x[5]) for x in res_db]
    return sr_levels

def get_sr_html(stk, mkt):
    sr_lines = get_sr(stk, mkt)
    sr_levels = [(x[0], x[1], x[2], x[3], f"{x[0]}_{x[1]}_{x[2]}_{x[3]}")
        for x in sr_lines]
    logging.info(f"sr_levels = {sr_levels}")
    return render_template('sr.html', stk=stk, market_name=mkt,
        sr_levels=sr_levels)

def add_sr(stk, mkt, dt1, price1, dt2, price2):
    if not dt2:
        dt2 = dt1
    if not price2:
        price2 = price1
    q = sql.Composed([
        sql.SQL("INSERT INTO"),
        sql.Identifier("stx_sr"),
        sql.SQL("VALUES ("),
        sql.SQL(',').join([
            sql.Literal(stk), sql.Literal(mkt),
            sql.Literal(dt1), sql.Literal(price1),
            sql.Literal(dt2), sql.Literal(price2)
        ]),
        sql.SQL(") ON CONFLICT DO NOTHING")
    ])
    try:
        stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
    except:
        return f'Create SR ({dt1}, {price1}), ({dt2}, {price2}) failed:<br>'\
            f'{tb.print_exc()}'
    return get_sr_html(stk, mkt)

def delete_sr(stk, mkt, selected_srs):
    for sr in selected_srs:
        dt1, price1, dt2, price2 = sr.split('_')
        q = sql.Composed([
            sql.SQL("DELETE FROM "), sql.Identifier("stx_sr"),
            sql.SQL(" WHERE "),
            sql.Identifier("mkt"), sql.SQL("="), sql.Literal(mkt),
            sql.SQL(" AND "),
            sql.Identifier("stk"), sql.SQL("="), sql.Literal(stk),
            sql.SQL(" AND "),
            sql.Identifier("dt1"), sql.SQL("="), sql.Literal(dt1),
            sql.SQL(" AND "),
            sql.Identifier("px1"), sql.SQL("="), sql.Literal(price1),
            sql.SQL(" AND "),
            sql.Identifier("dt2"), sql.SQL("="), sql.Literal(dt2),
            sql.SQL(" AND "),
            sql.Identifier("px2"), sql.SQL("="), sql.Literal(price2)
        ])
        try:
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            return f'Delete SR ({dt1}, {price1}), ({dt2}, {price2}) failed:'\
                f'<br>{tb.print_exc()}'
    return get_sr_html(stk, mkt)


@app.route('/support_resistance', methods=['GET', 'POST'])
def support_resistance():
    requested_action = request.form.get('action')
    logging.info(f"The action requested is {requested_action}")
    stk = request.form.get('stk')
    mkt = request.form.get('market_name')
    if not requested_action:
        logging.error('No action specified; this page was reached by error')
        return 'No action specified; this page was reached by error'
    if requested_action == 'sr_init':
       return get_sr_html(stk, mkt)
    elif requested_action == 'sr_delete_selected':
        print(f"request form: {request.form}")
        selected_srs = request.form.getlist('sr_list')
        print(f"SRs selected: {','.join(selected_srs)}")
        return delete_sr(stk, mkt, selected_srs)
    elif requested_action == 'sr_add':
        dt1 = request.form.get('dt1')
        price1 = request.form.get('price1')
        dt2 = request.form.get('dt2')
        price2 = request.form.get('price2')
        return add_sr(stk, mkt, dt1, price1, dt2, price2)
    else:
        logging.error(f"Wrong action '{requested_action}'specified; should be "
                       "one of 'sr_init', 'sr_delete_selected', or 'sr_add'")
        return f"Wrong action '{requested_action}'specified; should be "\
            "one of 'sr_init', 'sr_delete_selected', or 'sr_add'"
