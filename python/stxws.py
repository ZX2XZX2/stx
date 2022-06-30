from flask import Flask, render_template, request, url_for, flash
import matplotlib
matplotlib.use('Agg')
import os
import stxcal
from stxplot import StxPlot

app = Flask(__name__)


@app.route('/')
def index():
    end_date = stxcal.current_busdate(hr=10)
    start_date = stxcal.move_busdays(end_date, -90)
    res = []
    res.append('<html>')
    res.append(f'<title>Indexes {end_date}</title>')
    res.append('<body>')
    for stxindex in ['^GSPC', '^IXIC', '^DJI']:
        sp = StxPlot(None, stxindex, start_date, end_date, stk=stxindex)
        res.append(sp.webchartstream())
    res.append('</body>')
    res.append('</html>')
    return ''.join(res)


@app.route('/indexes')
def show_indexes():
    charts = []
    end_date = stxcal.current_busdate(hr=10)
    start_date = stxcal.move_busdays(end_date, -90)
    for stxindex in ['^GSPC', '^IXIC', '^DJI']:
        sp = StxPlot(None, stxindex, start_date, end_date, stk=stxindex)
        chartdict = { 'figdata_png': sp.b64_png() }
        charts.append(chartdict)
    return render_template('indexes.html', charts=charts)


@app.route('/charts', methods=('GET', 'POST'))
def charts():
    charts = []
    stks = ''
    dt = stxcal.current_busdate(hr=10)
    if request.method == 'POST':
        stks = request.form['stocks']
        dt = request.form['datetime']
        if not stks:
            flash('Stocks are required!')
        elif not dt:
            flash('Date is required!')
        else:
            stk_list = stks.split(' ')
            end_date = dt
            start_date = stxcal.move_busdays(end_date, -90)
            for stk in stk_list:
                sp = StxPlot(None, stk, start_date, end_date, stk=stk)
                chartdict = { 'figdata_png': sp.b64_png() }
                charts.append(chartdict)
    return render_template('charts.html', charts=charts, stx=stks, dt=dt)


@app.route('/scanners')
def scanners():
    return "This will show the scanners"


@app.route('/rtscanners')
def rtscanners():
    return "This will show the realtime scanners"
