from flask import Flask, render_template, request, url_for, flash
import logging
import matplotlib
matplotlib.use('Agg')
import os
import pandas as pd
import stxcal
from stx247 import StxAnalyzer
from stxplot import StxPlot
from stxplotid import StxPlotID
from stxtsid import StxTSID

logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
    '%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
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
    '30min': '30min'
}


@app.route('/')
def index():
    charts = []
    date_dict = {}
    end_date = stxcal.current_busdate(hr=10)
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
    end_date = stxcal.current_busdate(hr=10)
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
    dt = stxcal.current_busdate(hr=10)
    if request.method == 'POST':
        stks = request.form['stocks']
        dt = request.form['datetime']
        num_days = int(request.form['days'])
        if not stks:
            flash('Stocks are required!')
        elif not dt:
            flash('Date is required!')
        else:
            stk_list = stks.split(' ')
            end_date = dt
            start_date = stxcal.move_busdays(end_date, -num_days)
            for stk in stk_list:
                sp = StxPlot(None, stk, start_date, end_date, stk=stk)
                chartdict = { 'figdata_png': sp.b64_png() }
                charts.append(chartdict)
    return render_template('charts.html', charts=charts, stx=stks, dt=dt)


@app.route('/idcharts', methods=('GET', 'POST'))
def idcharts():
    charts = []
    stks = ''
    end_date = stxcal.current_busdate(hr=10)
    end_time = '16:00'
    num_days = 5
    freq = '5min'
    if request.method == 'POST':
        logging.info(f"action = {request.form['action']}")
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
                logging.info(f'end_dt = {end_dt}')
                end_date, end_time = stxcal.next_intraday(end_dt)
                end_dt = f'{end_date} {end_time}'                
                logging.info(f'end_date = {end_date} end_time = {end_time}')
            stk_list = stks.split(' ')
            start_date = stxcal.move_busdays(end_date, -num_days + 1)
            start_dt = f'{start_date} 09:35'
            frequency = int(freq[:-3])
            for stk in stk_list:
                sp = StxPlotID(None, start_dt, end_dt, stk, frequency)
                chartdict = { 'figdata_png': sp.b64_png() }
                charts.append(chartdict)
        return render_template(
            'idcharts.html', charts=charts, stx=stks,
            dt_date=end_date, dt_time=end_time, num_days=num_days,
            frequencydict=frequencydict, freq=freq)
    return render_template(
        'idcharts.html', charts=charts, stx=stks,
        dt_date=end_date, dt_time=end_time, num_days=num_days,
        frequencydict=frequencydict, freq=freq)


@app.route('/scanners', methods=('GET', 'POST'))
def scanners():
    charts = []
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
        end_dt = request.form['datetime']
        if not end_dt:
            flash('Date is required!')
            return render_template(
                'scanner.html', charts=[], dt=end_dt,
                min_up_cs=min_up_cs, max_down_cs=max_down_cs)
        end_date, end_time = end_dt.split(' ')
        start_date = stxcal.move_busdays(end_date, -220)
        date_1 = stxcal.prev_busday(end_date)
        eod = False
        # Return all triggered setups for the day
        setup_df = stx_ana.get_triggered_setups(end_date, eod, triggered=True)
        min_up_cs = int(request.form['min_up_cs'])
        max_down_cs = int(request.form['max_down_cs'])
        # Filter out:
        # 1. UP setups with CS_45 rank below a threshold
        # 2. DOWN setups with CS_45 rank above a threshold
        sdf = setup_df.query("(direction=='U' and bucket_rank>@min_up_cs) or "
                             "(direction=='D' and bucket_rank<@max_down_cs)")
        sdf = stx_ana.add_indicators(sdf, end_date, indicator_list, False)
        # 3. Setups not triggered yet
        for _, row in sdf.iterrows():
            tsid = StxTSID(row['stk'], start_date, end_date, end_time)
            tsid.mpf_id(end_date)
            if ((row['direction'] == 'U' and
                 tsid.df.loc[end_date, 'High'] < tsid.df.loc[date_1, 'High']) or
                (row['direction'] == 'D' and
                 tsid.df.loc[end_date, 'Low'] > tsid.df.loc[date_1, 'Low'])):
                continue
            res = tsid.getchartstreams(end_dt, eod_days=90, id_days1=10,
                                       id_mins1=30, id_days2=5, id_mins2=10)
            indicator_tbl = stx_ana.build_indicators_table(row)
            res['indicator_table'] = ''.join(indicator_tbl)
            charts.append(res)
            # start_date = stxcal.move_busdays(end_date, -90)
            # id_start_date_1 = f'{stxcal.move_busdays(end_date, -10)} 09:35'
            # id_start_date_2 = f'{stxcal.move_busdays(end_date, -5)} 09:35'
            # for stk in stk_list:
            #     sp = StxPlotID(stk, start_dt, end_dt, 15)
            #     chartdict = { 'figdata_png': sp.b64_png() }
            #     charts.append(chartdict)
    else:
        min_up_cs = 90
        max_down_cs = 10
    return render_template('scanner.html', charts=charts, dt=end_dt,
                           min_up_cs=min_up_cs, max_down_cs=max_down_cs)


@app.route('/rtscanners')
def rtscanners():
    return "This will show the realtime scanners"
