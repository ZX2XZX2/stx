import json
import logging
import os
import pandas as pd
from psycopg2 import sql
import requests
import string
import stxcal
import stxdb
import sys
import time
import traceback as tb

logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
    '%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

# TODO:
# Iterate through the alphabet letters
# Get https://etfdb.com/alpha/A/, ... https://etfdb.com/alpha/Z/
# To get the next page do:
# https://etfdb.com/alpha/B/#etfs&sort_name=symbol&sort_order=asc&page=2
# Get each ETF:
# https://etfdb.com/etf/AAAU/#etf-ticker-profile
# etfdb only has top 15 holdings, can I get full list from yhoo or somewhere else
# zacks seems to have a list of holdings for each ETF: https://www.zacks.com/funds/etf/ACES/holding
# https://etfdb.com/data_set/?tm=40278&cond={"by_alphabetical":"A"}&no_null_sort=true&count_by_id=&limit=25,25&sort=symbol&order=asc&offset=50

# for letter in list(string.ascii_uppercase):
#     etf_url = f'https://etfdb.com/alpha/{letter}/'

# filename = os.path.join(os.getenv('HOME'), 'etfs_a.json')
# with open(filename) as f:
#     etfs_json = json.load(f)

# etf_rows = etfs_json.get('rows', [])
# if not etf_rows:
#     print('No ETFs found')

# etf_ix, processed_etfs = 0, 0
# for etf in etf_rows:
#     etf_ix += 1
#     print(f'{json.dumps(etf, indent=2)}')
#     etf_symbol_raw = etf.get('symbol')
#     if not etf_symbol_raw:
#         print(f'ETF {etf_ix} missing symbol')
#         continue
#     symbol_tokens = etf_symbol_raw.split('\u003e')
#     etf_symbol = symbol_tokens[2][:-6]
#     etf_name_raw = etf.get('name')
#     if not etf_name_raw:
#         print(f'ETF {etf_ix} missing name')
#         continue
#     name_tokens = etf_name_raw.split('\u003e')
#     if len(name_tokens) < 4:
#         print(f'ETF {etf_ix} failed to parse name text (not enough tokens)')
#         continue
#     name_raw = name_tokens[3]
#     iix = name_raw.find('<span')
#     if iix == -1:
#         print(f'ETF {etf_ix} failed to parse name text (no span)')
#         continue
#     etf_name = name_raw[:iix].strip()
#     etf_category = etf.get('etf_category', 'N/A')
#     print(f'ETF ticker {etf_symbol}, name {etf_name}, category {etf_category}')
#     processed_etfs += 1
# print(f'Processed {processed_etfs} out of {len(etf_rows)} ETFs')

# filename = os.path.join(os.getenv('HOME'), 'etf_b.txt')
# with open(filename, 'r') as f:
#     text = f.read()
# lines = text.split('\n')
# start_ix = 1000000
# end_ix = -1
# for i, line in enumerate(lines):
#     stripped_line = line.strip()
#     if stripped_line == '<tbody>':
#         start_ix = i
#     if stripped_line == '</tbody>':
#         end_ix = i
#     if i <= start_ix:
#         continue
#     if i <= end_ix:
#         break
#     if stripped_line in ['<tr>', '</tr>']:
#         continue
#     line_tokens = stripped_line.split('</td> <td class="" data-th=')
#     print(f'{line_tokens}')
#     tkns0 = line_tokens[0].split('>')
#     etf_ticker = tkns0[2][:-3] if len(tkns0) > 2 else ''
#     tkns1 = line_tokens[1].split('>')
#     etf_name = tkns1[2][:-3] if len(tkns1) > 2 else ''
#     tkns2 = line_tokens[2].split('>')
#     etf_category = tkns2[2][:-3] if len(tkns2) > 2 else ''
#     print(f'etf_ticker = {etf_ticker}, etf_name = {etf_name} etf_category = {etf_category}')


# etf_rows = etfs_json.get('rows', [])
# if not etf_rows:
#     print('No ETFs found')

# etf_ix, processed_etfs = 0, 0
# for etf in etf_rows:
#     etf_ix += 1
#     print(f'{json.dumps(etf, indent=2)}')
#     etf_symbol_raw = etf.get('symbol')
#     if not etf_symbol_raw:
#         print(f'ETF {etf_ix} missing symbol')
#         continue
#     iix1 = etf_symbol_raw.find('href=\"/etf/')
#     iix2 = etf_symbol_raw.find('/\"')
#     etf_symbol = etf_symbol_raw[iix1 + 12: iix2]
#     etf_name_raw = etf.get('name')
#     if not etf_name_raw:
#         print(f'ETF {etf_ix} missing name')
#         continue
#     name_tokens = etf_name_raw.split('\u003e')
#     if len(name_tokens) < 4:
#         print(f'ETF {etf_ix} failed to parse name text (not enough tokens)')
#         continue
#     name_raw = name_tokens[3]
#     iix = name_raw.find('<span')
#     if iix == -1:
#         print(f'ETF {etf_ix} failed to parse name text (no span)')
#         continue
#     etf_name = name_raw[:iix].strip()
#     etf_category = etf.get('etf_category', 'N/A')
#     print(f'ETF ticker {etf_symbol}, name {etf_name}, category {etf_category}')
#     processed_etfs += 1
# print(f'Processed {processed_etfs} out of {len(etf_rows)} ETFs')

# filename = os.path.join(os.getenv('HOME'), 'etf_b.txt')
# with open(filename, 'r') as f:
#     text = f.read()
# lines = text.split('\n')
# start_ix = 1000000
# end_ix = -1
# for i, line in enumerate(lines):
#     stripped_line = line.strip()
#     if stripped_line == '<tbody>':
#         start_ix = i
#     if stripped_line == '</tbody>':
#         end_ix = i
#     if i <= start_ix:
#         continue
#     if i <= end_ix:
#         break
#     if stripped_line in ['<tr>', '</tr>']:
#         continue
#     line_tokens = stripped_line.split('</td> <td class="" data-th=')
#     print(f'{line_tokens}')
#     tkns0 = line_tokens[0].split('>')
#     etf_ticker = tkns0[2][:-3] if len(tkns0) > 2 else ''
#     tkns1 = line_tokens[1].split('>')
#     etf_name = tkns1[2][:-3] if len(tkns1) > 2 else ''
#     tkns2 = line_tokens[2].split('>')
#     etf_category = tkns2[2][:-3] if len(tkns2) > 2 else ''
#     print(f'etf_ticker = {etf_ticker}, etf_name = {etf_name} etf_category = {etf_category}')

this = sys.modules[__name__]
this.elim_dict = None

def get_etf_holdings():
    filename = os.path.join(os.getenv('HOME'), 'reports', 'etf_list.txt')
    with open(filename, 'r') as f:
        text = f.read()
        lines = text.split('\n')
    for line in lines:
        tokens = line.split('\t')
        if len(tokens) > 4:
            etf_ticker = tokens[0]
            etf_name = tokens[1].replace("'", '')
            etf_category = tokens[2]
            stxdb.db_write_cmd(
                f"INSERT INTO etfs VALUES('{etf_ticker}', '{etf_name}', "
                f"'{etf_category}') ON CONFLICT (ticker) DO NOTHING"
            )
    # begin - parse the holdings info for each fund
    etf_list = stxdb.db_read_cmd('select ticker from etfs')
    num_etfs = len(etf_list)
    print(f'Getting data for {num_etfs} ETFs')
    headers = requests.utils.default_headers()
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) '\
        'Gecko/20100101 Firefox/88.0'
    etf_name = 'XLE'
    for i, etf_record in enumerate(etf_list):
        etf_name = etf_record[0]
        req = requests.get(
            f"https://www.zacks.com/funds/etf/{etf_name}/holding",
            headers=headers
        )
        if req.status_code != 200:
            print(f'Request for ETF {etf_name} failed with status '
                  f'{req.status_code}, error: {req.text}')
            continue
        lines = req.text.split('\n')
        # print(f'The request response has {len(lines)} lines')
        holdings_line = ''
        for line in lines:
            if line.startswith('etf_holdings.formatted_data'):
                holdings_line = line
        if not holdings_line:
            print(f'No holdings line found for {etf_name}, skipping...')
            continue
        holdings_tokens = holdings_line[34: -5].split(' ] ,  [ ')
        # print(f'There are {len(holdings_tokens)} holdings tokens')
        hold_list = []
        for holding_row in holdings_tokens:
            # print(f'holding_row = {holding_row}')
            holding_tokens = holding_row.split(', ')
            # print(f'There are {len(holding_tokens)} holding tokens')
            if len(holding_tokens) < 2:
                continue
            ticker_token = holding_tokens[1]
            # print(f'Ticker token = {ticker_token}')
            ticker_index = ticker_token.find('rel=')
            # print(f'Ticker index = {ticker_index}')
            if ticker_index == -1:
                continue
            ticker_tokens = ticker_token[ticker_index:].split('\\"')
            # print(f'There are {len(ticker_tokens)} ticker tokens')
            if len(ticker_tokens) >= 2:
                ticker = ticker_tokens[1]
                hold_list.append(ticker)
                stxdb.db_write_cmd(
                    f"INSERT INTO stk_etfs VALUES('{ticker}', '{etf_name}') "
                    f"ON CONFLICT(stk, etf) DO NOTHING"
                )
        print(f'ETF {etf_name} has {len(hold_list)} equity holdings: '
              f'{hold_list}')
        if i > 0 and (i % 100 == 0 or i == num_etfs - 1):
            print(f'Got data for {i} out of {num_etfs} ETFS')
        time.sleep(2)
# end - parse the holdings info for each fund

# start logic that maps a set of labels to etf names
def get_etf_words(elim_dct={}):
    q = sql.Composed([sql.SQL("SELECT name FROM etfs")])
    res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    etf_words = ' '.join([x[0] for x in res])
    etf_words_list = [ x for x in etf_words.split() if x not in elim_dct ]
    dct = {}
    for w in etf_words_list:
        count = dct.get(w, 0)
        dct[w] = count + 1
    return dct
# end logic that maps a set of labels to etf names


# start logic that gets a set of labels for a given stock
def get_eliminated_words():
    if this.elim_dict is None:
        eliminated_file = os.path.join(
            os.getcwd(),
            'eliminated_words_etf.txt'
        )
        file_str = open(eliminated_file, 'r', errors='ignore').read()
        lines = file_str.split()
        this.elim_dct = { x.strip(): '' for x in lines }
    return this.elim_dct

def stock_labels(stk, elim_dct=get_eliminated_words()):
    q = sql.Composed(
        [
            sql.SQL("SELECT name FROM etfs WHERE ticker IN "),
            sql.SQL("(SELECT etf FROM stk_etfs WHERE stk = "),
            sql.Literal(stk),
            sql.SQL(")")
        ]
    )
    res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
    etf_words = ' '.join([x[0] for x in res])
    etf_words_list = etf_words.split()
    labels = [x for x in etf_words_list if x not in elim_dct]
    labels = list(set(labels))
    return labels
# end logic that gets a set of labels for a given stock


# start: get industry, sector and biz description for each stock
# if stx is an empty list, will get info for all the stocks in DB
# otherwise, will get info only for the stocks in the list
def stock_groups(stx=[]):
    crumb = '80n0eI1buNM'
    mapping_source = 'yf'
    base_url = 'https://query1.finance.yahoo.com/v10/finance/quoteSummary'
    headers = requests.utils.default_headers()
    headers['User-Agent'] = ''.join([
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0)'
        ' Gecko/20100101 Firefox/88.0'
    ])
    crt_bus_date = stxcal.current_busdate(0)
    if not stx:
        stx_start_date = stxcal.move_busdays(crt_bus_date, -20)
        q = sql.Composed([
            sql.SQL("SELECT DISTINCT stk FROM eods WHERE dt BETWEEN"),
            sql.Literal(stx_start_date),
            sql.SQL("AND"),
            sql.Literal(crt_bus_date)
        ])
        res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
        stx = [x[0] for x in res]
    logging.info(f'{crt_bus_date}: Getting profiles for {len(stx)} stocks')
    num = 0
    for stk in stx:
        num += 1
        if num % 100 == 0:
            logging.info(f'Got profiles for {num}/{len(stx)} stocks')
        req = '&'.join([
            f'{base_url}/{stk}?formatted=true',
            f'crumb={crumb}',
            'lang=en-US',
            'region=US',
            'modules=summaryProfile',
            'corsDomain=finance.yahoo.com'
        ])
        res = requests.get(req, headers=headers)
        if res.status_code != 200:
            logging.warn(f'Failed to get profile for {stk}, error code '
                         f'{res.status_code}, error {res.text}')
            continue
        res_json = res.json()
        results = res_json.get('quoteSummary', {}).get('result', [])
        profile = results[0] if results else None
        summary_profile = profile.get('summaryProfile') if profile else None
        if not profile or not summary_profile:
            logging.warn(f'Could not get profile for {stk}, wrong quote format')
            logging.warn(json.dumps(res_json, indent=2))
            continue
        industry = summary_profile.get('industry', 'N/A')
        sector = summary_profile.get('sector', 'N/A')
        descr = summary_profile.get('longBusinessSummary', 'N/A')
        q = sql.Composed([
            sql.SQL("INSERT INTO ind_groups VALUES ("),
            sql.SQL(',').join(
                [
                    sql.Literal(stk),
                    sql.Literal(crt_bus_date),
                    sql.Literal(mapping_source),
                    sql.Literal(industry),
                    sql.Literal(sector),
                    sql.Literal(descr)
                ]
            ),
            sql.SQL(") ON CONFLICT(stk, dt, source) DO NOTHING")
        ])
        try:
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            logging.error(f'Failed to insert in DB profile for {stk}')
            tb.print_exc()
            
    logging.info(f'Got profiles for {num}/{len(stx)} stocks')

# 1. get all the sectors in the ind_groups as of a specific date
# 2. check if all of these sectors already exist in the database in
#    the sectors_industries tables, and they already have an id
#    assigned to them.
# 3. if a sector has an id, re-use it, otherwise, assign a new ID,
#     next available ID
# 4. upload the sectors and their ids in the sectors_industries table
def populate_sectors(dt):
    current_sectors = []
    sector_indices = {}
    q1 = sql.Composed([
        sql.SQL("SELECT DISTINCT sector FROM ind_groups WHERE dt="),
        sql.Literal(dt),
        sql.SQL(" AND source="),
        sql.Literal("yf"),
        sql.SQL(" ORDER BY sector")
    ])
    try:
        res = stxdb.db_read_cmd(q1.as_string(stxdb.db_get_cnx()))
        current_sectors = [x[0] for x in res if x[0] not in ['', 'N/A']]
    except:
        logging.error(f'Failed to retrieve the sectors for {dt}')
        tb.print_exc()
        return
    q2 = sql.Composed([
        sql.SQL("SELECT DISTINCT group_name, group_id FROM sectors_industries "
                "WHERE group_type="),
        sql.Literal("Sector"),
        sql.SQL(" AND group_name IN ("),
        sql.SQL(',').join([sql.Literal(x) for x in current_sectors]),
        sql.SQL(") ORDER BY group_name")
    ])
    try:
        res = stxdb.db_read_cmd(q2.as_string(stxdb.db_get_cnx()))
        duplicate_sectors = []
        for x in res:
            gid = sector_indices.get(x[0])
            if gid is None:
                sector_indices[x[0]] = x[1]
            else:
                logging.error(f'Multiple ids ({gid}, {x[1]}) found for sector '
                              f'{x[0]}')
                duplicate_sectors.append(x[0])
        if duplicate_sectors:
            logging.error(f"Sectors {','.join(duplicate_sectors)} have "
                          f"multiple IDs. This should not happen")
            return
    except:
        logging.error(f'Failed to retrieve the sectors and IDs from the '
                      f' sectors_industries table')
        tb.print_exc()
        return
    max_sector_id = 'S00000'
    q3 = sql.Composed([
        sql.SQL("SELECT MAX(group_id) FROM sectors_industries "
                "WHERE group_type="),
        sql.Literal("Sector")
    ])
    try:
        res = stxdb.db_read_cmd(q3.as_string(stxdb.db_get_cnx()))
        if res[0][0] is not None:
            max_sector_id = res[0][0]
    except:
        logging.error(f'Failed to retrieve max sector id')
        tb.print_exc()
        return
    last_sector_id = int(max_sector_id[1:])
    for sector in current_sectors:
        sector_id = sector_indices.get(sector)
        if sector_id is None:
            last_sector_id += 1
            sector_id = f'S{last_sector_id:05d}'
        q = sql.Composed([
            sql.SQL("INSERT INTO sectors_industries VALUES ("),
            sql.SQL(',').join([
                sql.Literal(dt),
                sql.Literal('Sector'),
                sql.Literal(sector),
                sql.Literal(sector_id),
                sql.Literal('')
            ]),
            sql.SQL(") ON CONFLICT(dt, group_type, group_name) DO NOTHING")
        ])
        try:
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            logging.error(f'Failed to insert info for sector {sector}')
            tb.print_exc()

# same as populate sectors, but also keep track of the sector for each
# industry
def populate_industries(dt):
    current_industries = []
    industry_sectors = {}
    industry_indices = {}
    q1 = sql.Composed([
        sql.SQL("SELECT DISTINCT sector, industry FROM ind_groups WHERE dt="),
        sql.Literal(dt),
        sql.SQL(" AND source="),
        sql.Literal("yf"),
        sql.SQL(" ORDER BY sector, industry")
    ])
    try:
        res = stxdb.db_read_cmd(q1.as_string(stxdb.db_get_cnx()))
        current_industries = [x[1] for x in res if x[1] not in ['', 'N/A']]
        industry_sectors = {x[1]: x[0] for x in res if x[1] not in ['', 'N/A']}
    except:
        logging.error(f'Failed to retrieve the industries for {dt}')
        tb.print_exc()
        return
    q2 = sql.Composed([
        sql.SQL("SELECT DISTINCT group_name, group_id FROM sectors_industries "
                "WHERE group_type="),
        sql.Literal("Industry"),
        sql.SQL(" AND group_name IN ("),
        sql.SQL(',').join([sql.Literal(x) for x in current_industries]),
        sql.SQL(") ORDER BY group_name")
    ])
    try:
        res = stxdb.db_read_cmd(q2.as_string(stxdb.db_get_cnx()))
        duplicate_industries = []
        for x in res:
            iid = industry_indices.get(x[0])
            if iid is None:
                industry_indices[x[0]] = x[1]
            else:
                logging.error(f'Multiple ids ({iid}, {x[1]}) found for '
                              f'industry {x[0]}')
                duplicate_industries.append(x[0])
        if duplicate_industries:
            logging.error(f"Industries {','.join(duplicate_industries)} have "
                          f"multiple IDs. This should not happen")
            return
    except:
        logging.error(f'Failed to retrieve the industries and IDs from the '
                      f' sectors_industries table')
        tb.print_exc()
        return
    max_industry_id = 'I00000'
    q3 = sql.Composed([
        sql.SQL("SELECT MAX(group_id) FROM sectors_industries "
                "WHERE group_type="),
        sql.Literal("Industry")
    ])
    try:
        res = stxdb.db_read_cmd(q3.as_string(stxdb.db_get_cnx()))
        if res[0][0] is not None:
            max_industry_id = res[0][0]
    except:
        logging.error(f'Failed to retrieve max industry id')
        tb.print_exc()
        return
    last_industry_id = int(max_industry_id[1:])
    for industry in current_industries:
        industry_id = industry_indices.get(industry)
        if industry_id is None:
            last_industry_id += 1
            industry_id = f'I{last_industry_id:05d}'
        q = sql.Composed([
            sql.SQL("INSERT INTO sectors_industries VALUES ("),
            sql.SQL(',').join([
                sql.Literal(dt),
                sql.Literal('Industry'),
                sql.Literal(industry),
                sql.Literal(industry_id),
                sql.Literal('')
            ]),
            sql.SQL(") ON CONFLICT(dt, group_type, group_name) DO NOTHING")
        ])
        try:
            logging.info(q.as_string(stxdb.db_get_cnx()))
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            logging.error(f'Failed to insert info for industry {industry}')
            tb.print_exc()
