from datetime import datetime
import os
import requests
import stxcal

def parsesplit(tokens, startindex):
    ix1 = tokens[startindex + 1].find('>')
    ix2 = tokens[startindex + 1].find('</a>')
    exchange = tokens[startindex + 1][ix1 + 1: ix2]
    ix1 = tokens[startindex + 2].find('>')
    ix2 = tokens[startindex + 2].find('</a>')
    ticker = tokens[startindex + 2][ix1 + 1: ix2]
    splitdate = str(datetime.strptime(tokens[startindex + 3][:-5],
                                      '%m/%d/%Y').date())
    ix1 = tokens[startindex + 4].find("<")
    splitratio = tokens[startindex + 4][:ix1]
    return [exchange, ticker, splitdate, splitratio]


dt = stxcal.today_date()
fname = os.path.join(os.getenv('HOME'), 'Downloads', f"split_{dt.replace('-', '')}.txt")
res = ['\t'.join(['Exchange', 'Symbol', 'Date', 'Ratio'])]
headers = requests.utils.default_headers()
data = requests.get('https://eoddata.com/splits.aspx', headers=headers)
srch_str1 = '<table class="quotes">'
ix1 = data.text.find(srch_str1)
ix2 = data.text[ix1:].find('<table>')
splits_tbl = data.text[ix1:ix2]
tokens = splits_tbl.split('<tr class="r0">')
for token in tokens[1: ]:
    token = token.replace('\r\n', '')
    tkns = token.split('<td>')
    if len(tkns) < 5:
        continue
    lineres = parsesplit(tkns, 0)
    res.append('\t'.join(lineres))
    if len(tkns) < 9:
        continue
    lineres = parsesplit(tkns, 4)
    res.append('\t'.join(lineres))
with open(fname, 'w') as fp:
    fp.write('\n'.join(res))
