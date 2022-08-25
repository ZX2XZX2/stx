import argparse
from datetime import datetime
import logging
import os
import requests
import stxcal

class StxGetSplits:
    def __init__(self, dt):
        self.dt = dt

    def parsesplit(self, tokens, startindex):
        ix1 = tokens[startindex + 1].find('>')
        ix2 = tokens[startindex + 1].find('</a>')
        exchange = tokens[startindex + 1][ix1 + 1: ix2]
        ix1 = tokens[startindex + 2].find('>')
        ix2 = tokens[startindex + 2].find('</a>')
        ticker = tokens[startindex + 2][ix1 + 1: ix2]
        splitdate = tokens[startindex + 3][:-5]
        # splitdate = str(datetime.strptime(tokens[startindex + 3][:-5],
        #                                   '%m/%d/%Y').date())
        ix1 = tokens[startindex + 4].find("<")
        splitratio = tokens[startindex + 4][:ix1]
        return [exchange, ticker, splitdate, splitratio]

    def getsplits(self):
        fname = os.path.join(
            os.getenv('HOME'),
            'Downloads',
            f"splits_{self.dt.replace('-', '')}.txt"
        )
        res = ['\t'.join(['Exchange', 'Symbol', 'Date', 'Ratio'])]
        headers = requests.utils.default_headers()
        data = requests.get('https://eoddata.com/splits.aspx', headers=headers)
        fn = os.path.join('/tmp', f"splits_{self.dt.replace('-', '')}.txt")
        with open(fn, 'w') as fp:
            fp.write(data.text)
        srch_str1 = '<table class="quotes">'
        ix1 = data.text.find(srch_str1)
        ix2 = data.text[ix1: ].find('</table>')
        splits_tbl = data.text[ix1: ix1 + ix2]
        tokens = splits_tbl.split('<tr class="r0">')
        for token in tokens[1: ]:
            token = token.replace('\r\n', '')
            tkns = token.split('<td>')
            if len(tkns) < 5:
                continue
            try:
                lineres = self.parsesplit(tkns, 0)
                res.append('\t'.join(lineres))
            except:
                logging.warn(f'Failed to parse line {tkns[:4]}')
            if len(tkns) < 9:
                continue
            try:
                lineres = self.parsesplit(tkns, 4)
                res.append('\t'.join(lineres))
            except:
                logging.warn(f'Failed to parse line {tkns[:4]}')
        with open(fname, 'w') as fp:
            fp.write('\n'.join(res))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', type=str,
                        help='Date in the splits file name',
                        default=stxcal.today_date())
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    sgs = StxGetSplits(args.date)
    sgs.getsplits()
