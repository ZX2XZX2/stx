import json
import logging
import os
import pandas as pd
from psycopg2 import sql
import requests
import string
import stxcal
import stxdb
import traceback as tb

logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
    '%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

# start: get industry, sector and biz description for each stock
# if stx is an empty list, will get info for all the stocks in DB
# otherwise, will get info only for the stocks in the list
def stock_groups(as_of_date, stx=[]):
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
                    sql.Literal(as_of_date),
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
    sector_indices = {}
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
    q3 = sql.Composed([
        sql.SQL("SELECT DISTINCT group_name, group_id FROM sectors_industries "
                "WHERE group_type="),
        sql.Literal("Sector")
    ])
    try:
        res = stxdb.db_read_cmd(q3.as_string(stxdb.db_get_cnx()))
        sector_indices = {x[0]: x[1] for x in res}
    except:
        logging.error(f'Failed to retrieve sector indices')
        tb.print_exc()
        return        
    max_industry_id = 'I00000'
    q4 = sql.Composed([
        sql.SQL("SELECT MAX(group_id) FROM sectors_industries "
                "WHERE group_type="),
        sql.Literal("Industry")
    ])
    try:
        res = stxdb.db_read_cmd(q4.as_string(stxdb.db_get_cnx()))
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
                sql.Literal(sector_indices.get(industry_sectors.get(industry)))
            ]),
            sql.SQL(") ON CONFLICT(dt, group_type, group_name) DO NOTHING")
        ])
        try:
            stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
        except:
            logging.error(f'Failed to insert info for industry {industry}')
            tb.print_exc()

def clone_groups_for_date(cloned_date, dt):
    last_expiry = stxcal.prev_expiry(dt)
    q = sql.Composed([
        sql.SQL("INSERT INTO "),
        sql.Identifier("ind_groups"),
        sql.SQL('('),
        sql.SQL(',').join([
            sql.Identifier('stk'),
            sql.Identifier('dt'),
            sql.Identifier('source'),
            sql.Identifier('industry'),
            sql.Identifier('sector'),
            sql.Identifier('profile')
        ]),
        sql.SQL(') SELECT '),
        sql.SQL(',').join([
            sql.Identifier('stk'),
            sql.Literal(last_expiry),
            sql.Identifier('source'),
            sql.Identifier('industry'),
            sql.Identifier('sector'),
            sql.Identifier('profile')
        ]),
        sql.SQL(' FROM '),
        sql.Identifier('ind_groups'),
        sql.SQL(' WHERE dt='),
        sql.Literal(cloned_date)
    ])
    try:
        stxdb.db_write_cmd(q.as_string(stxdb.db_get_cnx()))
    except:
        logging.error('Failed to clone stock profile data from '
                      f'{cloned_date} to {last_expiry}')
        tb.print_exc()

def calc_group_indicator(indicator_name, dt):
    # get the sectors/industries as of the last expiry
    group_dt = stxcal.prev_expiry(dt)
    q = sql.Composed([
        sql.SQL('SELECT '),
        sql.SQL(',').join([
            sql.Identifier('stk'),
            sql.Identifier('industry'),
            sql.Identifier('sector')
        ]),
        sql.SQL(' FROM '),
        sql.Identifier('ind_groups'),
        sql.SQL(' WHERE dt='),
        sql.Literal(group_dt)
    ])
    group_df = pd.read_sql(q, stxdb.db_get_cnx())
    group_df.columns = ['ticker', 'industry', 'sector']
    # get the indicator values as of the date dt
    q = sql.Composed([
        sql.SQL('SELECT '),
        sql.SQL(',').join([
            sql.Identifier('ticker'),
            sql.Identifier('dt'),
            sql.Identifier('name'),
            sql.Identifier('value'),
            sql.Identifier('rank')
        ]),
        sql.SQL(' FROM '),
        sql.Identifier('indicators_1'),
        sql.SQL(' WHERE dt='),
        sql.Literal(dt),
        sql.SQL(' AND name='),
        sql.Literal(indicator_name)
    ])
    indicator_df = pd.read_sql(q, stxdb.db_get_cnx())
    # merge the group and indicator data frames
    df = pd.merge(group_df, indicator_df, on='ticker')
    # create new columns to calculate median and stdev
    df['value_median'] = df['value']
    df['value_std'] = df['value']
    df['rank_median'] = df['rank']
    df['rank_std'] = df['rank']
    # use groupby to calculate mean, median, std for each group
    industry_df = df.groupby('industry').agg({
        'ticker':'size',
        'value':'mean',
        'rank':'mean',
        'value_median':'median',
        'rank_median':'median',
        'value_std':'std',
        'rank_std':'std'
    })
    sector_df = df.groupby('sector').agg({
        'ticker':'size',
        'value':'mean',
        'rank':'mean',
        'value_median':'median',
        'rank_median':'median',
        'value_std':'std',
        'rank_std':'std'
    })
    # filter out groups with one ticker
    industry_df = industry_df.query("ticker>1").copy()
    sector_df = sector_df.query("ticker>1").copy()    
    q = sql.Composed([
        sql.SQL('SELECT '),
        sql.SQL(',').join([
            sql.Identifier('group_type'),
            sql.Identifier('group_name'),
            sql.Identifier('group_id')
        ]),
        sql.SQL(' FROM '),
        sql.Identifier('sectors_industries'),
        sql.SQL(' WHERE dt='),
        sql.Literal(group_dt)
    ])
    group_id_df = pd.read_sql(q, stxdb.db_get_cnx())
    sector_id_df = group_id_df.query("group_type=='Sector'")
    industry_id_df = group_id_df.query("group_type=='Industry'")
    industry_df.reset_index(inplace=True)
    sector_df.reset_index(inplace=True)
    industry_id_df.reset_index(inplace=True)
    sector_id_df.reset_index(inplace=True)
    industry_df.columns = ['group_name', 'ticker',
                           'value', 'rank',
                           'value_median', 'rank_median',
                           'value_std','rank_std']
    sector_df.columns = ['group_name', 'ticker',
                         'value', 'rank',
                         'value_median', 'rank_median',
                         'value_std','rank_std']
    industry_id_df.drop(columns='group_type', inplace=True)
    sector_id_df.drop(columns='group_type', inplace=True)

    sector_df = pd.merge(sector_df, sector_id_df, on='group_name')
    industry_df = pd.merge(industry_df, industry_id_df, on='group_name')

    logging.info(f'sector_df: \n columns: {sector_df.columns} \n'
                 f' head: {sector_df.head()}')
    # logging.info(f'sector_id_df: \n columns: {sector_id_df.columns} \n'
    #              f' head: {sector_id_df.head()}')
    logging.info(f'industry_df: \n columns: {industry_df.columns} \n'
                 f' head: {industry_df.head()}')
    # logging.info(f'industry_id_df: \n columns: {industry_id_df.columns} \n'
    #              f' head: {industry_id_df.head()}')
    
    return sector_df, industry_df

    # industry_df = df.groupby(['industry']).mean()
    # sector_df = df.groupby(['sector']).mean()
    # Use this to get count and mean in the same dataframe https://stackoverflow.com/questions/41040132/pandas-groupby-count-and-mean-combined
    # sector_df.sort_values(by=['rank', 'value'], inplace=True)
