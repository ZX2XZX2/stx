import ctypes
import datetime
import json
import logging
import os
import pandas as pd
from psycopg2 import sql
import stxcal
import stxdb
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
from stx247 import StxAnalyzer
from stxplot import StxPlot
from stxplotid import StxPlotID
from stxtsid import StxTSID

ixxx = 0
refresh = 1 # refrsh time in minutes. for realtime, it is 5 minutes

app = Flask(__name__)
indicators='CS_10,CS_20,CS_45,OBV_10,OBV_20,OBV_45,RS_10,RS_252,RS_4,RS_45'
indicator_list = indicators.split(',')
indicator_tenor_list = [x.split('_') for x in indicator_list]
indicators_df = pd.DataFrame(indicator_tenor_list,
                             columns=['indicator', 'tenor'])
indicators_df['tenor'] = indicators_df['tenor'].astype('int')
indicators_df.sort_values(by=['indicator', 'tenor'], inplace=True)
indicator_names = sorted(indicators_df['indicator'].unique())
indicator_tenors = sorted(indicators_df['tenor'].unique())
display_days = 90

stx_ana = StxAnalyzer(indicator_names, indicator_tenors, display_days)

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
            charts = generate_charts(stk_list, end_dt, eod_days, id_days, freq)
    return render_template('analysis.html', charts=charts, stx=stks,
                           dt_date=dt_date, dt_time=dt_time,
                           eod_days=eod_days, id_days=id_days, freq=freq,
                           frequencydict=frequencydict)

@app.route('/eod_market', methods=('GET', 'POST'))
def eod_market_analysis():
    charts = []
    stks = ''
    end_date, end_time = stxcal.current_intraday_busdatetime()
    min_up_cs = 90
    max_down_cs = 10
    eod_days = 90
    id_days = 20
    freq = '60min'
    """1. Get all the JC_1234 and JC_5DAYS setups for the current day.
    2. Use the same DB query as in the report generator
    3. Filter on the indicators
    4. Check, by using the intraday data, which setups have been
    triggered as of the current date and time.  Calculate the time
    when it was triggered.
    5. Generate the eod and intraday charts for each triggered setup
    6. Replace the setup time retrieved from the database with the
    setup time calculated in 4.
    """
    end_dt = ''
    if request.method == 'POST':
        end_date = request.form['dt_date']
        end_time = "15:55:00"
        end_dt = f'{end_date} {end_time}'
        eod_days = int(request.form['eod_days'])
        id_days = int(request.form['id_days'])
        freq = request.form['frequency']
        if not end_dt:
            flash('Date is required!')
            return render_template(
                'eod_market.html', charts=[], end_date='', end_time='',
                eod_days=eod_days, id_days=id_days,
                min_up_cs=min_up_cs, max_down_cs=max_down_cs,
                frequencydict=frequencydict, freq=freq)
        # num_recs = ctypes.c_int(0)
        # self._lib = _lib
        # self._lib.stx_get_ohlcv.argtypes = (
        #     ctypes.c_char_p,
        #     ctypes.c_char_p,
        #     ctypes.c_int,
        #     ctypes.c_bool,
        #     ctypes.c_bool,
        #     ctypes.POINTER(ctypes.c_int),
        # )
        # self._lib.stx_get_ohlcv.restype = ctypes.POINTER(ChartStruct)
        # realtime = False
        # logging.info('getting stock data')
        # res = self._lib.stx_get_ohlcv(
        #     ctypes.c_char_p(stk.encode('UTF-8')),
        #     ctypes.c_char_p(end_dt.encode('UTF-8')),
        #     ctypes.c_int(num_days),
        #     ctypes.c_bool(intraday),
        #     ctypes.c_bool(realtime),
        #     ctypes.byref(num_recs)
        # )

        if request.form.get('untriggered') is not None:
            eod = True
            triggered = False
        else:
            eod = False
            triggered = True
            if request.form['action'] == 'Next':
                end_date, end_time = stxcal.next_intraday(end_dt)
                end_dt = f'{end_date} {end_time}'
        end_date, end_time = end_dt.split(' ')
        start_date = stxcal.move_busdays(end_date, -220)
        date_1 = stxcal.prev_busday(end_date)
        frequency = int(freq[:-3])
        # Return all triggered setups for the day
        if not eod:
            setup_df = stx_ana.get_triggered_setups(
                end_date, eod, triggered)
        else:
            setup_df = stx_ana.get_triggered_setups(
                stxcal.next_busday(end_date), eod, triggered)
        min_up_cs = int(request.form['min_up_cs'])
        max_down_cs = int(request.form['max_down_cs'])
        # Filter out:
        # 1. UP setups with CS_45 rank below a threshold
        # 2. DOWN setups with CS_45 rank above a threshold
        sdf = setup_df.query("(direction=='U' and bucket_rank>=@min_up_cs) or "
                             "(direction=='D' and bucket_rank<=@max_down_cs)")
        sdf = stx_ana.add_indicators(sdf, end_date, indicator_list, eod)
        # 3. Setups not triggered yet
        for _, row in sdf.iterrows():
            try:
                tsid = StxTSID(row['stk'], start_date, end_date, end_time)
                tsid.mpf_id(end_date)
                if ((not eod and row['direction'] == 'U' and
                     tsid.df.loc[end_date, 'High']<tsid.df.loc[date_1, 'High'])
                    or (not eod and row['direction'] == 'D' and
                        tsid.df.loc[end_date,'Low']>tsid.df.loc[date_1,'Low'])):
                    continue
                res = tsid.getchartstreams(end_dt, eod_days=eod_days,
                                           id_days1=id_days,
                                           id_mins1=frequency)
                indicator_tbl = stx_ana.build_indicators_table(row)
                res['indicator_table'] = ''.join(indicator_tbl)
                charts.append(res)
            except:
                logging.error(f"Intraday analysis failed for {row['stk']}")
                tb.print_exc()
    else:
        min_up_cs = 90
        max_down_cs = 10
    return render_template('eod_market.html', charts=charts, dt_date=end_date,
                           dt_time=end_time, min_up_cs=min_up_cs,
                           max_down_cs=max_down_cs,
                           eod_days=eod_days,
                           id_days=id_days,
                           frequencydict=frequencydict, freq=freq)

def get_portfolio(mkt_name, stx, mkt_dt):
    dt_date, dt_time = mkt_dt.split()
    _lib.stx_get_portfolio.restype = ctypes.c_void_p
    res = _lib.stx_get_portfolio(
        ctypes.c_char_p(mkt_name.encode('UTF-8')),
        ctypes.c_char_p(stx.encode('UTF-8')),
        ctypes.c_char_p(mkt_dt.encode('UTF-8')),
        ctypes.c_char_p(dt_date.encode('UTF-8')),
        ctypes.c_char_p(dt_time.encode('UTF-8'))
    )
    portfolio_str = ctypes.cast(res, ctypes.c_char_p).value
    portfolio_json = json.loads(portfolio_str)
    logging.info(f"portfolio_json = {json.dumps(portfolio_json, indent=2)}")
    _lib.stx_free_text.argtypes = (ctypes.c_void_p,)
    _lib.stx_free_text.restype = None
    _lib.stx_free_text(ctypes.c_void_p(res))
    open_positions, closed_positions = [], []
    for stk, position in portfolio_json.items():
        pos = position.get('Long')
        if pos:
            direction = 'Long'
            dir = 1
        else:
            pos = position.get('Short')
            direction = 'Short'
            dir = -1
        out_shares = pos.get('out_shares', 0)
        open_shares = pos['in_shares'] - out_shares
        stop_loss = pos['stop_loss']
        target = pos['target']
        crt_price = pos['current_price']
        avg_in_price = pos['avg_in_price']
        unrealized_pnl = open_shares * dir * (crt_price - avg_in_price)
        rrr = -1
        if target != -1 and stop_loss != -1:
            rrr = (target - crt_price) / (crt_price - stop_loss)
        portfolio_line = [
            stk, direction, open_shares, pos['avg_in_price'],
            pos['current_price'], unrealized_pnl,
            '' if target == -1 else target,
            '' if stop_loss == -1 else stop_loss,
            '' if rrr == -1 else int(100 * rrr) / 100.0
        ]
        if open_shares != 0:
            open_positions.append(portfolio_line)
        else:
            closed_positions.append(portfolio_line)
    return open_positions

def get_market(mkt_name, mkt_date, mkt_dt, mkt_cache, mkt_realtime):
    if isinstance(mkt_dt, datetime.datetime):
        mkt_dt = mkt_dt.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(mkt_date, datetime.date):
        mkt_date = mkt_date.strftime("%Y-%m-%d")
    eod_market = mkt_dt.endswith('16:00:00')
    portfolio = get_portfolio(mkt_name, '*',
        mkt_dt.replace('16:00:00', '15:55:00'))
    q = sql.Composed([
        sql.SQL("SELECT"),
        sql.Identifier("stk"),
        sql.SQL("FROM"),
        sql.Identifier("market_watch"),
        sql.SQL("WHERE"),
        sql.Identifier("mkt"),
        sql.SQL("="),
        sql.Literal(mkt_name)
    ])
    res_db = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    logging.info(f"res_db = {res_db}")
    watchlist = [x[0] for x in res_db]
    logging.info(f"watchlist = {watchlist}")

    if eod_market:
        min_activity = mkt_cache.get('min_activity', 10000)
        up_limit = mkt_cache.get('up_limit', 2)
        down_limit = mkt_cache.get('down_limit', 2)
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
        # portfolio = res_json.get('portfolio')
        # watchlist = res_json.get('watchlist')
        indicators = res_json.get('indicators')
        pf_list = [x[0] for x in portfolio]
        pf_charts, wl_charts, indicator_charts = [], [], {}
        if pf_list:
            pf_charts = generate_charts(pf_list, f'{mkt_date} 15:55:00',
                                        0, 2, '5min')
        if watchlist:
            wl_charts = generate_charts(watchlist, f'{mkt_date} 15:55:00',
                                        120, 20, '60min')
        if indicators:
            for indicator in indicators:
                indicator_name = indicator.get('name')
                ind_up = indicator.get('Up')
                ind_down = indicator.get('Down')
                stx_up = [x['ticker'] for x in ind_up]
                stx_down = [x['ticker'] for x in ind_down]
                up_charts = generate_charts(stx_up, f'{mkt_date} 15:55:00',
                                            120, 20, '60min')
                down_charts = generate_charts(stx_down, f'{mkt_date} 15:55:00',
                                              120, 20, '60min')
                indicator_charts[indicator_name] = {
                    "up": up_charts,
                    "down": down_charts
                }
        print(f'res = {json.dumps(res_json, indent=2)}')
        return render_template(
            'eod.html',
            market_name=mkt_name,
            market_date=mkt_date,
            market_dt=mkt_dt,
            pf_charts=pf_charts,
            wl_charts=wl_charts,
            watchlist=watchlist,
            indicator_charts=indicator_charts,
            portfolio=portfolio
        )
    else:
        return f"Intraday market {mkt_name}, datetime = {mkt_dt}"


@app.route('/create_market', methods=('GET', 'POST'))
def create_market():
    mkt_name = request.form.get('mkt_name')
    mkt_date  = request.form.get('dt_date')
    mkt_dt = f"{mkt_date} 16:00:00"
    mkt_realtime = "TRUE" if request.form.get('realtime') else "FALSE"  
    q = sql.Composed([
        sql.SQL("SELECT"),
        sql.Identifier("mkt_name"),
        sql.SQL("FROM"),
        sql.Identifier("market_caches"),
        sql.SQL("WHERE"),
        sql.Identifier("mkt_name"),
        sql.SQL("="),
        sql.Literal(mkt_name)
    ])
    res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    if res:
        return f'A market named {mkt_name} already exists'
    mkt_cache = {"portfolio": [], "watchlist": [], "setups": {}}
    q = sql.Composed([
        sql.SQL("INSERT INTO"),
        sql.Identifier("market_caches"),
        sql.SQL("VALUES ("),
        sql.SQL(',').join(
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

@app.route('/load_market', methods=('GET', 'POST'))
def load_market():
    mkt_name = request.form.get('market_name')
    q = sql.Composed([
        sql.SQL("SELECT * FROM"),
        sql.Identifier("market_caches"),
        sql.SQL("WHERE"),
        sql.Identifier("mkt_name"),
        sql.SQL("="),
        sql.Literal(mkt_name)
    ])
    res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    if not res:
        return f'Could not load market {mkt_name}, it does not exist'
    mkt_date = res[0][1]
    mkt_dt = res[0][2]
    mkt_cache = res[0][3]
    mkt_realtime = res[0][4]
    return get_market(mkt_name, mkt_date, mkt_dt, mkt_cache, mkt_realtime)

@app.route('/delete_market', methods=('GET', 'POST'))
def delete_market():
    mkt_name = request.form.get('market_name')
    q = sql.Composed([
        sql.SQL("DELETE FROM"),
        sql.Identifier("market_caches"),
        sql.SQL("WHERE"),
        sql.Identifier("mkt_name"),
        sql.SQL("="),
        sql.Literal(mkt_name)
    ])
    try:
        stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
    except:
        return f'Market {mkt_name} delete failed:<br>{tb.print_exc()}'
    return f'Market {mkt_name} successfully deleted'

def generate_charts(stk_list, end_dt, eod_days, id_days, frequency,
                    id_days1=None, frequency1=None):
    charts = []
    end_date, _ = end_dt.split()
    freq = int(frequency[:-3])
    for stk in stk_list:
        if eod_days != 0:
            sp = StxPlotBin(_lib, stk, eod_days, end_dt, intraday=False)
        spid = StxPlotBin(_lib, stk, id_days, end_dt, intraday=True,
            period=freq)
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
                               period=freq1)
            chartdict['id1_png'] = spid1.b64_png()
        charts.append(chartdict)
    return charts


@app.route('/market')
def market():
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
            # sp = StxPlotBin(_lib, stk, eod_days, market_datetime, intraday=False)
            spid_1 = StxPlotBin(_lib, stk, id_days_1, market_datetime, intraday=True,
                period=frequency_1)
            spid_2 = StxPlotBin(_lib, stk, id_days_2, market_datetime, intraday=True,
                period=frequency_2)
            chartdict = {
                'eod_png': spid_1.b64_png(),
                'id_png': spid_2.b64_png()
            }
            charts.append(chartdict)
        exec_end_time = datetime.datetime.now()
        logging.debug(f'exec_end_time = {exec_end_time}')
        exec_time = exec_end_time - exec_start_time
        logging.debug(f'exec_time = {exec_time}')
        refresh_time = 60000 * refresh - int( 1000 * exec_time.total_seconds())
        logging.debug(f'refresh_time = {refresh_time}')
        return render_template('market.html', refresh=refresh_time, charts=charts,
                                datetime=market_datetime)
    return render_template('eod.html', datetime=market_datetime)

@app.route('/markets')
def markets():
    q = sql.Composed([sql.SQL("SELECT DISTINCT mkt_name FROM market_caches")])
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
            sql.SQL("INSERT INTO"),
            sql.Identifier("market_watch"),
            sql.SQL("VALUES ("),
            sql.SQL(',').join([
                sql.Literal(market_name),
                sql.Literal(stk)
            ]),
            sql.SQL(") ON CONFLICT DO NOTHING")
        ])
        try:
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            return f'Failed to add {stk} to {market_name} watchlist:<br>{tb.print_exc()}'
        return f"Added {stk} to {market_name} watchlist"
    else:
        q = sql.Composed([
            sql.SQL("DELETE FROM"),
            sql.Identifier("market_watch"),
            sql.SQL("WHERE"),
            sql.Identifier("stk"),
            sql.SQL("="),
            sql.Literal(stk),
            sql.SQL("AND"),
            sql.Identifier("mkt"),
            sql.SQL("="),
            sql.Literal(market_name)
        ])
        try:
            print(f"DELETE SQL: {q.as_string(stxdb.db_get_cnx())}")
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            return f'Failed to remove {stk} from {market_name} watchlist:<br>{tb.print_exc()}'
        return f"Removed {stk} from {market_name} watchlist"

def gen_analysis_page(request):
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
    eod_days = 120
    id_days1 = 20
    id_days2 = 2
    freq1 = '60min'
    freq2 = '5min'
    charts = generate_charts(stk_list, dt, eod_days, id_days1, freq1,
        id_days2, freq2)
    return charts, dt

@app.route('/stk_analysis', methods=['POST'])
def stk_analysis():
    market_name = request.form['market_name']
    charts, dt = gen_analysis_page(request)
    return render_template('stk_analysis.html', chart=charts[0], dt=dt, market_name=market_name)
    # return render_template('stk_analysis.html', charts=charts, dt=request.form['stk_dt'])

def init_trade(request):
    stk = request.form['stk']
    dt = request.form['dt']
    market_name = request.form['market_name']
    _lib.stx_get_trade_input.restype = ctypes.c_void_p
    res = _lib.stx_get_trade_input(
        ctypes.c_char_p(stk.encode('UTF-8')),
        ctypes.c_char_p(dt.encode('UTF-8'))
    )
    trade_input_str = ctypes.cast(res, ctypes.c_char_p).value
    trade_input_json = json.loads(trade_input_str)
    logging.info(f"trade_input_json = {json.dumps(trade_input_json, indent=2)}")
    _lib.stx_free_text.argtypes = (ctypes.c_void_p,)
    _lib.stx_free_text.restype = None
    _lib.stx_free_text(ctypes.c_void_p(res))
    in_price = trade_input_json['current_price']
    avg_volume = trade_input_json["avg_volume"]
    avg_range = trade_input_json["avg_range"]
    # TODO: replace 30000 with market parameters
    size = avg_volume /100
    logging.info(f'volume size = {size}')
    volatility_size = 30000 * 5 / (3 * avg_range)
    logging.info(f'volatility_size = {volatility_size}')
    if size > volatility_size:
        size = volatility_size
    trading_power_size = 30000 * 100 / in_price
    logging.info(f'trading_power_size = {trading_power_size}')
    if size > trading_power_size:
        size = trading_power_size
    logging.info(f'size = {size}')
    return render_template('trade.html', stk=stk, dt=dt,
        market_name=market_name, current_price=in_price, size=int(size)
    )

def check_trade_params(request):
    try:
        stop_loss = int(request.form.get('stop_loss'))
    except:
        stop_loss = 'N/A'
    try:
        target = int(request.form.get('target'))
    except:
        target = 'N/A'
    try:
        size = int(request.form.get('size'))
    except:
        size = 'N/A'
    valid_params = (stop_loss != 'N/A' and target != 'N/A' and size != 'N/A')
    return valid_params, stop_loss, target, size

def risk_mgmt(request):
    stk = request.form['stk']
    dt = request.form['dt']
    market_name = request.form['market_name']
    in_price = int(request.form.get('current_price'))
    valid_params, stop_loss, target, size = check_trade_params(request)
    if valid_params:
        # TODO: replace with market params
        max_loss_size = 30000 * 2 / (abs(stop_loss - in_price))
        if size > max_loss_size:
            size = max_loss_size
        max_loss = size * (abs(stop_loss - in_price))
        max_profit = size * (abs(target - in_price))
        reward_risk_ratio = 100 * max_profit // max_loss * 0.01
    else:
        max_loss = 'N/A'
        max_profit = 'N/A'
        reward_risk_ratio = 'N/A'
    return render_template(
        'trade.html', stk=stk, dt=dt, market_name=market_name,
        current_price=in_price, stop_loss=stop_loss, target=target, size=size,
        max_loss=max_loss, max_profit=max_profit,
        reward_risk_ratio=reward_risk_ratio)

def exec_trade(request):
    stk = request.form['stk']
    dt = request.form['dt']
    market_name = request.form['market_name']
    in_price = int(request.form.get('current_price'))
    valid_params, stop_loss, target, size = check_trade_params(request)
    if valid_params:
        if in_price > stop_loss and in_price < target and size > 0:
            action = -1
            direction = 1
            action_str = "BOT"
        elif in_price < stop_loss and in_price > target and size > 0:
            action = 1
            direction = -1
            action_str = 'SSD'
        else:
            action = 0 # this means the trade is invalid
            direction = 0 # this means the trade is invalid
            return "Invalid trade. Check the input and try again"
        trd_info = {}
        trd_info['stop-loss'] = stop_loss
        trd_info['target'] = target
        q = sql.Composed([
            sql.SQL("INSERT INTO"),
            sql.Identifier("trades"),
            sql.SQL("VALUES ("),
            sql.SQL(',').join([
                sql.Literal(market_name),
                sql.Literal(stk),
                sql.Literal(dt),
                sql.Literal(direction),
                sql.Literal(action),
                sql.Literal(in_price),
                sql.Literal(size),
                sql.Literal(json.dumps(trd_info))
            ]),
            sql.SQL(") ON CONFLICT DO NOTHING")
        ])
        log_msg = f"{market_name}, {stk}, {dt}, {action_str}, {in_price}, "\
            f"{size}, {trd_info['stop-loss']}, {trd_info['target']}"
        try:
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            return f'Failed to trade {log_msg}:<br>{tb.print_exc()}'
    else:
        log_msg = f"Invalid trade inputs: {'size ' if size == 'N/A' else ''}"\
            f"{'stop-loss ' if stop_loss == 'N/A' else ''}"\
            f"{'target ' if target == 'N/A' else ''}"
    return log_msg

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
    elif requested_action == 'exec_trade':
        return exec_trade(request)
    else:
        logging.error(f"Wrong action '{requested_action}'specified; should be "
                       "one of 'init_trade', 'risk_mgmt', or 'exec_trade'")
        return f"Wrong action '{requested_action}'specified; should be "\
            "one of 'init_trade', 'risk_mgmt', or 'exec_trade'"

@app.route('/support_resistance', methods=['GET', 'POST'])
def support_resistance():
    stk = request.form.get('stk')
    dt = request.form.get('dt')
    market = request.form.get('market_name')
    _lib.stx_get_jl.restype = ctypes.c_void_p    
    res = _lib.stx_get_jl(
        ctypes.c_char_p(stk.encode('UTF-8')),
        ctypes.c_char_p(dt.encode('UTF-8'))
    )
    jl_str = ctypes.cast(res, ctypes.c_char_p).value
    jl_json = json.loads(jl_str)
    logging.info(f"trade_input_json = {json.dumps(jl_json, indent=2)}")
    _lib.stx_free_text.argtypes = (ctypes.c_void_p,)
    _lib.stx_free_text.restype = None
    _lib.stx_free_text(ctypes.c_void_p(res))

    jl_html = '<table>\n  <tr><th>Datetime</th><th>SRa</th><th>NRa</th>'\
        '<th>UT</th><th>DT</th><th>NRa</th><th>SRa</th></tr>\n'
    for jl_rec in jl_json:
        jl_row_html = f"  <tr><td>"\
            f"{jl_rec.get('date', 'YYYY-mm-dd HH:MM:SS')[5:]}</td>"
        for _ in range(jl_rec.get('state', 0)):
            jl_row_html += '<td></td>'
        jl_row_html += f"<td>{jl_rec.get('price', -1)}</td>"
        for _ in range(5 - jl_rec.get('state', 5)):
            jl_row_html += '<td></td>'
        jl_row_html += '</tr>\n'
        jl_html += jl_row_html
    jl_html += '</table>'
    return render_template('jl.html', jl_html=jl_html)

