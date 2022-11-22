import argparse
from configparser import ConfigParser
import glob
import logging
import os
from psycopg2 import sql
import stxdb
from stxdf import StxDatafeed
import sys
import traceback as tb


def get_available_splits_files():
    q = sql.Composed([
        sql.SQL("SELECT * FROM analyses WHERE analysis="),
        sql.Literal("splits")
    ])
    last_splits_date = None
    try:
        res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
        last_splits_date = str(res[0][0])
        logging.info(f'Last splits analysis date is: {last_splits_date}')
    except:
        logging.error(f'Failed to retrieve last splits analysis date')
        tb.print_exc()
        sys.exit(-1)
    file_pattern = "splits_*_ana.txt"
    config = ConfigParser()
    cfg_file_path = os.path.abspath(os.path.join(os.getenv('HOME'),
                                                 'stx_cfg.ini'))
    config.read(cfg_file_path)
    in_dir = config.get('datafeed', 'data_dir')
    file_list = glob.glob(os.path.join(in_dir, file_pattern))
    file_list.sort(reverse=True)
    available_dates = []
    for data_file in file_list:
        path_tokens = data_file.split(os.sep)
        tokens = path_tokens[-1].split('_')
        file_dt = tokens[1]
        file_date = f'{file_dt[:4]}-{file_dt[4:6]}-{file_dt[6:]}'
        if file_date <= last_splits_date:
            break
        available_dates.append(file_date)
    return sorted(available_dates)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stock splits')
    parser.add_argument('-d', '--splitsdate', type=str, required=False,
                        help='splits analysis date in format yyyy-MM-dd')
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    df = StxDatafeed()
    if not args.splitsdate:
        available_dates = get_available_splits_files()
        logging.info(f"Found {len(available_dates)} split analysis files")
        for available_date in available_dates:
            logging.info(f'stx_splits: splits analysis date {available_date}')
            df.upload_splits(available_date)
    else:
        logging.info(f'stx_splits: splits analysis date {args.splitsdate}')
        df.upload_splits(args.splitsdate)
