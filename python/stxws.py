from flask import Flask, render_template
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
