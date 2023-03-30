import ctypes
import logging
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
import os
import pandas as pd
import stxcal
from stx247 import StxAnalyzer
from stxplot import StxPlot
from stxplotid import StxPlotID
from stxtsid import StxTSID
import traceback as tb

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
    use_c = True
    charts = []
    id_charts = []
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
            start_date = stxcal.move_busdays(end_date, -eod_days + 1)
            start_iddate = stxcal.move_busdays(end_date, -id_days + 1)
            start_dt = f'{start_iddate} 09:30'
            frequency = int(freq[:-3])
            for stk in stk_list:
                if (use_c):
                    sp = StxPlotBin(_lib, stk, eod_days, end_dt,
                                    intraday=False)
                    spid = StxPlotBin(_lib, stk, id_days, end_dt,
                                      intraday=True, period=frequency)
                else:
                    sp = StxPlot(None, stk, start_date, end_date, stk=stk)
                    spid = StxPlotID(None, start_dt, end_dt, stk, frequency)
                chartdict = {
                    'eod_png': sp.b64_png(),
                    'id_png': spid.b64_png()
                }
                charts.append(chartdict)
    return render_template('analysis.html', charts=charts, stx=stks,
                           dt_date=dt_date, dt_time=dt_time,
                           eod_days=eod_days, id_days=id_days, freq=freq,
                           frequencydict=frequencydict)

@app.route('/scanners', methods=('GET', 'POST'))
def scanners():
    charts = []
    stks = ''
    end_date, end_time = stxcal.current_intraday_busdatetime()
    min_up_cs = 90
    max_down_cs = 10
    eod_num_days = 90
    id_num_days = 10
    freq = '15min'
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
        end_time = request.form['dt_time']
        end_dt = f'{end_date} {end_time}'
        eod_num_days = int(request.form['eod_num_days'])
        id_num_days = int(request.form['id_num_days'])
        freq = request.form['frequency']
        if not end_dt:
            flash('Date is required!')
            return render_template(
                'scanner.html', charts=[], end_date='', end_time='',
                eod_num_days=eod_num_days, id_num_days=id_num_days,
                min_up_cs=min_up_cs, max_down_cs=max_down_cs,
                frequencydict=frequencydict, freq=freq)
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
                res = tsid.getchartstreams(end_dt, eod_days=eod_num_days,
                                           id_days1=id_num_days,
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
    return render_template('scanner.html', charts=charts, dt_date=end_date,
                           dt_time=end_time, min_up_cs=min_up_cs,
                           max_down_cs=max_down_cs,
                           eod_num_days=eod_num_days,
                           id_num_days=id_num_days,
                           frequencydict=frequencydict, freq=freq)


@app.route('/rtscanners')
def rtscanners():
    return "This will show the realtime scanners"

@app.route('/market')
def market():
    return f'this will show the market'
