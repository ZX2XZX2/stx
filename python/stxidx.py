import argparse
from datetime import datetime
import json
import logging
import requests
import stxcal
import stxdb


class StxIndex:
    def __init__(self):
        self.headers = requests.utils.default_headers()
        self.headers['User-Agent'] = ''.join([
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0)'
            ' Gecko/20100101 Firefox/88.0'
        ])

    def get_quote(self, idx, sdt, edt):
        long_start_date = stxcal.long_date(sdt)
        long_end_date = stxcal.long_date(edt)
        req = ''.join([
            "https://query2.finance.yahoo.com/v8/finance/chart/",
            idx,
            "?formatted=true&"
            "crumb=5dLyddyx4FN&",
            "lang=en-US&",
            "region=US&",
            "includeAdjustedClose=true&",
            "interval=1d&",
            f"period1={long_start_date}&",
            f"period2={long_end_date}&",
            "events=div|split&",
            "useYfid=true&",
            "corsDomain=finance.yahoo.com"
        ])
        res = requests.get(req, headers=self.headers)
        if res.status_code != 200:
            logging.error(f'Something went wrong for {idx}: {res.text}')
            return
        res_json = res.json()
        logging.debug(f'status code = {res.status_code}; '
                      f'response = {json.dumps(res_json, indent=2)}')
        res_error = res_json.get('chart').get('error')
        if res_error is not None:
            logging.error(f'Error in data for {idx}: {res_error}')
            return
        res_list = res_json.get('chart').get('result')
        if len(res_list) < 1:
            logging.error(f'Got empty data for {idx}')
            return
        res_data = res_list[0]
        res_dates = res_data.get('timestamp')
        res_quote_list = res_data.get('indicators').get('quote')
        if len(res_quote_list) < 1:
            logging.error(f'Got empty quote for {idx}')
            return
        res_quote = res_quote_list[0]
        res_hi = res_quote.get('high')
        res_close = res_quote.get('close')
        res_volume = res_quote.get('volume')
        res_lo = res_quote.get('low')
        res_open = res_quote.get('open')
        if (len(res_dates) != len(res_hi) or
            len(res_dates) != len(res_close) or
            len(res_dates) != len(res_volume) or
            len(res_dates) != len(res_lo) or
            len(res_dates) != len(res_open)):
            logging.error(f'Inconsistent lists in quote for {idx}: '
                          f'lengths for dates ({len(res_dates)}), '
                          f'opens ({len(res_open)}), '
                          f'highs ({len(res_hi)}), '
                          f'lows ({len(res_lo)}), '
                          f'closes ({len(res_close)}), '
                          f'volumes ({len(res_volume)}) do not match')
            return
        date_list = [ str(datetime.fromtimestamp(x).date()) for x in res_dates ]
        open_list = [ int(round(x * 100, 0)) for x in res_open ]
        hi_list = [ int(round(x * 100, 0)) for x in res_hi ]
        lo_list = [ int(round(x * 100, 0)) for x in res_lo ]
        close_list = [ int(round(x * 100, 0)) for x in res_close ]
        volume_list = [ x // 1000 for x in res_volume ]
        for dt, o, hi, lo, c, v in zip(date_list, open_list, hi_list, lo_list,
                                       close_list, volume_list):
            # logging.info(f'{idx} {dt} {o} {hi} {lo} {c} {v}')
            db_cmd = ''.join([
                "insert into eods values",
                f"('{idx}', '{dt}', {o}, {hi}, {lo}, {c}, {v}, 0)",
                "on conflict (stk, dt) do update set ",
                f"o={o}, hi={hi}, lo={lo}, c={c}, v={v}, oi=0"
            ])
            stxdb.db_write_cmd(db_cmd)
        logging.info(f'Updated {len(date_list)} records for {idx}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--index', type=str, required=True,
                        help='Comma-separated list of quoted Indexes')
    parser.add_argument('-s', '--startdate', type=str, 
                        default=stxcal.move_busdays(
                            stxcal.current_busdate(hr=9), -5),
                        help='Start date for quote history')
    parser.add_argument('-e', '--enddate', type=str,
                        default=stxcal.current_busdate(hr=9),
                        help='End date for quote history')
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    idx_list = args.index.split(',')
    si = StxIndex()
    for idx in idx_list:
        si.get_quote(f'^{idx}', args.startdate, args.enddate)
