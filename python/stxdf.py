import argparse
from configparser import ConfigParser
from contextlib import closing
import csv
import datetime
import errno
from enum import Enum
import glob
import json
import logging
import numpy as np
import os
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values
import requests
import shlex
import shutil
import stxcal
import stxdb
import stxgrps
from stxidx import StxIndex
from stxts import StxTS
import subprocess
import sys
import traceback as tb


class Datafeed(Enum):
    stooq = 'stooq'
    eoddata = 'eoddata'

    def __str__(self):
        return self.value


class StxDatafeed:
    upload_dir = '/tmp'
    eod_name = os.path.join(upload_dir, 'eod_upload.txt')
    status_none = 0
    status_ok = 1
    status_ko = 2
    yhoo_url = 'https://query1.finance.yahoo.com/v7/finance/options/{0:s}?' \
               'formatted=true&crumb=BfPVqc7QhCQ&lang=en-US&region=US&' \
               'date={1:d}&corsDomain=finance.yahoo.com'

    def __init__(self, extension='.txt'):
        self.eod_tbl = 'eods'
        self.rec_name = self.eod_tbl
        self.divi_tbl = 'dividends'
        # instantiate config parser
        self.config = ConfigParser()
        # parse existing configuration file
        cfg_file_path = os.path.abspath(os.path.join(os.getenv('HOME'),
                                                     'stx_cfg.ini'))
        self.config.read(cfg_file_path)
        self.in_dir = self.config.get('datafeed', 'data_dir')


    def get_name(self, x):
        return {
            'ed': 'eoddata',
            'sq': 'stooq'
        }.get(x, 'final')

    # Load data from EODData data source in the database. There is a
    # daily file for each exchange (Amex, Nasdaq, NYSE). Use an
    # overlap of 5 days with the previous reconciliation interval
    # (covering up to 2012-12-31)
    def load_eoddata_files(self, sd, ed, stks='', batch=False):
        print('Loading eoddata files...')
        dt = sd
        # dt = stxcal.move_busdays(sd, -25)
        fnames = [os.path.join(self.in_dir, 'AMEX_{0:s}.txt'),
                  os.path.join(self.in_dir, 'NASDAQ_{0:s}.txt'),
                  os.path.join(self.in_dir, 'NYSE_{0:s}.txt')]
        while dt <= ed:
            print('eoddata: {0:s}'.format(dt))
            data_available = True
            dtc = dt.replace('-', '')
            for fname in fnames:
                fname_dt = fname.format(dtc)
                if not os.path.isfile(fname_dt):
                    print('Could not find file {0:s}'.format(fname_dt))
                    data_available = False
            if not data_available:
                print('Data is missing for date {0:s}. Exiting.'.format(dt))
                return
            for fname in fnames:
                fname_dt = fname.format(dtc)
                self.load_eoddata_file(fname_dt, dt, dtc, stks, batch=batch)
            stxdb.db_write_cmd("update analyses set dt='{0:s}' where "
                               "analysis='eod_datafeed'".format(dt))
            dt = stxcal.next_busday(dt)
        print('Loaded eoddata files')

    # Load data from a single EODData file in the database Perform
    # some quality checks on the data: do not upload days where volume
    # is 0, or where the open/close are outside the [low, high] range.
    def load_eoddata_file(self, ifname, dt, dtc, stks='', batch=False):
        upload_lines = []
        stk_list = [] if stks == '' else stks.split(',')
        # db_stx, _ = self.create_exchange()
        with open(ifname, 'r') as ifile:
            lines = ifile.readlines()
        for line in lines[1:]:
            tokens = line.replace(dtc, dt).strip().split(',')
            stk = tokens[0].strip()
            if (stk_list and stk not in stk_list) or ('/' in stk) or \
               ('*' in stk) or (stk in ['AUX', 'PRN']):
                continue
            # if stk not in db_stx:
            #     insert_stx = "INSERT INTO equities VALUES "\
            #                  "('{0:s}', '', 'US Stocks', 'US')".format(stk)
            #     stxdb.db_write_cmd(insert_stx)
            o = int(100 * float(tokens[2]))
            hi = int(100 * float(tokens[3]))
            lo = int(100 * float(tokens[4]))
            c = int(100 * float(tokens[5]))
            v = int(tokens[6])
            if v == 0 or o < lo or o > hi or c < lo or c > hi or \
               len(tokens[0]) > 6 or o >= 2147483647 or hi >= 2147483647 \
               or lo >= 2147483647 or c >= 2147483647:
                continue
            v = v // 1000
            if v == 0:
                v = 1
            if batch:
                upload_lines.append(
                    '{0:s}\t{1:s}\t{2:d}\t{3:d}\t{4:d}\t{5:d}\t{6:d}\t0\n'.
                    format(stk, dt, o, hi, lo, c, v))
            else:
                upload_lines.append([stk, dt, o, hi, lo, c, v, 0])
        if batch:
            with open(self.eod_name, 'w') as f:
                for line in upload_lines:
                    f.write(line)
            stxdb.db_upload_file(self.eod_name, self.eod_tbl, '\t')
        else:
            stxdb.db_insert_eods(upload_lines)

    def handle_splits(self, start_date):
        split_files = [x for x in os.listdir(self.in_dir)
                       if x.startswith('splits_')]
        split_files.sort()
        valid_prefixes = ['NYSE', 'NASDAQ', 'AMEX']
        split_dct = {}
        for split_file in split_files:
            with open(os.path.join(self.in_dir, split_file), 'r') as f:
                lines = reversed(f.readlines())
            for line in lines:
                tokens = line.strip().split()
                if len(tokens) < 4:
                    continue
                if tokens[0] not in valid_prefixes:
                    continue
                stk = tokens[1].strip()
                stk_splits = split_dct.get(stk, {})
                dt = str(datetime.datetime.strptime(tokens[2].strip(),
                                                    '%m/%d/%Y').date())
                if dt < start_date:
                    continue
                denominator, nominator = tokens[3].strip().split('-')
                split_ratio = float(nominator) / float(denominator)
                delete_split_dates = []
                for split_date in stk_splits.keys():
                    if abs(stxcal.num_busdays(split_date, dt)) < 5:
                        delete_split_dates.append(split_date)
                for split_date in delete_split_dates:
                    del stk_splits[split_date]
                stk_splits[dt] = split_ratio
                split_dct[stk] = stk_splits
        for stk, stk_splits in split_dct.items():
            for dt, ratio in stk_splits.items():
                self.print_split_report(stk, dt, ratio)

    def print_split_report(self, stk, dt, ratio):
        print(f'{stk} {dt} {ratio}')
        try:
            s_date = stxcal.move_busdays(dt, -10)
            e_date = stxcal.move_busdays(dt, 10)
            ts = StxTS(stk, s_date, e_date)
            ts.set_day(e_date, c=-1)
            print(f'{ts.df}')
        except:
            logging.error(f'print_split_report failed for {stk}, {dt}, '
                          f'{ratio}')
            tb.print_exc()


    def upload_splits(self, splits_date):
        splits_dt = splits_date.replace('-', '')
        splits_file = f"{os.getenv('DOWNLOAD_DIR')}/splits_{splits_dt}_ana.txt"
        print(f'Uploading stocks from file {splits_file}')
        with open(splits_file, 'r') as f:
            lines = f.readlines()
        num = 0
        for line in lines:
            tokens = line.split()
            if len(tokens) < 3:
                print(f'Skipping line {line}')
                continue
            stk = tokens[0].strip()
            dt = stxcal.prev_busday(tokens[1].strip())
            ratio = float(tokens[2].strip())
            db_cmd = "insert into {0:s} values('{1:s}','{2:s}',{3:f},0) "\
                "on conflict (stk, dt) do update set ratio={4:f}".format(
                self.divi_tbl, stk, dt, ratio, ratio)
            try:
                stxdb.db_write_cmd(db_cmd)
                num += 1
            except Exception as ex:
                print(f'Failed to upload split {stk}, {dt}, error {str(ex)}')
        print(f'Uploaded {num} out of {len(lines)} stock splits')

    def get_available_dates(self, file_pattern, last_date):
        file_list = glob.glob(os.path.join(self.in_dir, file_pattern))
        file_list.sort(reverse=True)
        available_dates = []
        for data_file in file_list:
            tokens = data_file.split('/')
            file_dt = tokens[-1][:8]
            file_date = '{0:s}-{1:s}-{2:s}'.format(
                file_dt[:4], file_dt[4:6], file_dt[6:])
            if not stxcal.is_busday(file_date):
                continue
            if file_date <= last_date:
                break
            available_dates.append(file_date)
        return available_dates

    def backup_database(self):
        # Get current DB name from POSTGRES_DB env variable
        # db_name = os.getenv('POSTGRES_DB')
        db_name = self.config.get('postgres_db', 'db_name')
        # Ensure root backup directory is created
        db_backup_dir = os.path.join(os.getenv('HOME'), 'db_backup', db_name)
        try:
            os.makedirs(db_backup_dir)
            logging.info('Creating directory {0:s}'.format(db_backup_dir))
        except OSError as e:
            if e.errno != errno.EEXIST:
                logging.error('Exception while creating {0:s}: {1:s}'.
                              format(db_backup_dir, str(e)))
                raise
        db_bkp_dirs = sorted(os.listdir(db_backup_dir))
        db_bkp_dirs_str = '\n  '.join(db_bkp_dirs)
        logging.debug(f"db_bkp_dirs = ")
        logging.debug(f"  {db_bkp_dirs_str}")
        # Create backup if < 2 DB backups, or last backup older than a week
        crt_date = datetime.datetime.now()
        backup_needed = False
        if len(db_bkp_dirs) < 2:
            backup_needed = True
        else:
            last_bkp_date = datetime.datetime.strptime(db_bkp_dirs[-1],
                                                       '%Y-%m-%d_%H%M%S')
            diff = crt_date - last_bkp_date
            diff_seconds = diff.days * 24 * 3600 + diff.seconds
            if diff_seconds > (24 * 7 - 1) * 3600:
                backup_needed = True
        if not backup_needed:
            logging.info('Found {0:d} DB backups, most recent is {1:d} days '
                         'old, no new backup needed'.
                         format(len(db_bkp_dirs),
                                (crt_date - last_bkp_date).days))
        else:
            # Create directory to store current database backup 
            db_bkp_dir = os.path.join(db_backup_dir,
                                      crt_date.strftime('%Y-%m-%d_%H%M%S'))
            try:
                os.makedirs(db_bkp_dir)
                logging.info('Creating directory {0:s}'.format(db_bkp_dir))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    logging.error('Exception while creating {0:s}: {1:s}'.
                                  format(db_bkp_dir, str(e)))
                    raise
            # launch the subprocesses that back up the database
            try:
                cmd1 = f'sudo -u postgres /usr/bin/pg_dump -Fc {db_name}'
                cmd2 = f'split -b 1000m - {db_bkp_dir}/{db_name}'
                p1 = subprocess.Popen(shlex.split(cmd1),
                                      stdout=subprocess.PIPE,
                                      cwd=db_bkp_dir)
                output = subprocess.check_output(shlex.split(cmd2),
                                                 stdin=p1.stdout,
                                                 cwd=db_bkp_dir)
                res = p1.wait()
                logging.info('Backed up DB, return status: {0:d}'.format(res))
                # if DB backup successful, remove older backups
                if res == 0:
                    for dir_to_remove in db_bkp_dirs[:-2]:
                        try:
                            dir_to_remove = os.path.join(db_backup_dir,
                                                         dir_to_remove)
                            shutil.rmtree(dir_to_remove)
                        except OSError as e:
                            logging.error(f'{e.filename} - {e.strerror}')
            except subprocess.CalledProcessError as cpe:
                logging.error('Database backup failed: {}'.format(cpe))
                # if backup failed, remove the backup directory
                try:
                    shutil.rmtree(db_bkp_dir)
                except OSError as e:
                    print ("Error: %s - %s." % (e.filename, e.strerror))
        # Manage DB backups for any USBs are plugged in
        self.db_usb_backup(db_name)

    def db_usb_backup(self, db_name):
        logging.info('Starting USB backup')
        db_backup_dir = os.path.join(os.getenv('HOME'), 'db_backup', db_name)
        usb_list = [
            self.config.get('postgres_db', 'usb_1'),
            self.config.get('postgres_db', 'usb_2'),
            self.config.get('postgres_db', 'usb_3')
        ]
            # os.getenv('USB_1'), os.getenv('USB_2'), os.getenv('USB_3')]
        for usb in usb_list:
            if not os.path.exists(usb):
                logging.info('{0:s} not found; skipping'.format(usb))
                continue
            logging.info('Backing up DB to USB {0:s}'.format(usb))
            usb_backup_dir = os.path.join(usb, 'db_backup', db_name)
            try:
                os.makedirs(usb_backup_dir)
                logging.info('Creating directory {0:s}'.format(usb_backup_dir))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    logging.error('Exception while creating {0:s}: {1:s}'.
                                  format(db_backup_dir, str(e)))
                    continue
            db_bkp_dirs = sorted(os.listdir(db_backup_dir))
            for db_bkp_dir in db_bkp_dirs:
                db_bkp_dir_path = os.path.join(usb_backup_dir, db_bkp_dir)
                if os.path.exists(db_bkp_dir_path):
                    logging.info('{0:s} already exists, skipping'.
                                 format(db_bkp_dir_path))
                    continue
                try:
                    shutil.copytree(os.path.join(db_backup_dir, db_bkp_dir),
                                    db_bkp_dir_path)
                    logging.info('Copied {0:s} to {1:s}'.format(
                            os.path.join(db_backup_dir, db_bkp_dir),
                            db_bkp_dir_path))
                except OSError as e:
                    print ("Error: %s - %s." % (e.filename, e.strerror))
            usb_db_bkp_dirs = sorted(os.listdir(usb_backup_dir))
            # Keep on the USB only two latest DB backups, remove the rest
            for dir_to_remove in usb_db_bkp_dirs[:-2]:
                try:
                    path_to_remove = os.path.join(usb_backup_dir,
                                                  dir_to_remove)
                    shutil.rmtree(path_to_remove)
                    logging.info('Removed old DB backup {0:s}'.
                                 format(path_to_remove))
                except OSError as e:
                    print ("Error: %s - %s." % (e.filename, e.strerror))

    def parse_stooq_eod(self, last_db_date):
        logging.info('Checking if a new stooq file has been downloaded')
        # stooq_file = os.path.join(os.getenv('DOWNLOAD_DIR'), 'data_d.txt')
        download_dir = self.config.get('datafeed', 'download_dir')
        stooq_file = os.path.join(download_dir, 'data_d.txt')
        if not os.path.exists(stooq_file):
            logging.info('No new stooq data file found.  Nothing to do.')
            return
        logging.info('Reading stooq file, renaming columns, getting daily '
                     'US stocks data')
        try:
            df = pd.read_csv(stooq_file, dtype={
                "<TICKER>": "string",
                "<PER>": "string",
                "<DATE>": "string",
                "<TIME>": "string",
                "<OPEN>": float,
                "<HIGH>": float,
                "<LOW>": float,
                "<CLOSE>": float,
                "<VOL>": float,
                "<OPENINT>": int
            })
        except:
            with open(stooq_file) as stooqfile:
                frdr = csv.reader(stooqfile)
                line_num = 0
                for row in frdr:
                    line_num += 1
                    try:
                        stk = str(row[0])
                        per = str(row[1])
                        dt = str(row[2])
                        tm = str(row[3])
                        o = float(row[4])
                        hi = float(row[5])
                        lo = float(row[6])
                        c = float(row[7])
                        v = float(row[8])
                        oi = int(row[9])
                    except:
                        logging.error(f"Failed to parse line {line_num}")
            sys.exit(-1)
        df.columns = [x[1: -1].lower() for x in df.columns]
        stx_df = df.query('ticker.str.endswith(".US") and per == "D"',
                          engine='python').copy()
        logging.info('Getting {0:d} daily US stocks out of {1:d} records'.
                     format(len(stx_df), len(df)))
        stx_df['date'] = stx_df['date'].astype(str)
        stx_df['vol'] = stx_df['vol'].astype(int)
        stx_df['date'] = stx_df.apply(
            lambda r: '{0:s}-{1:s}-{2:s}'.
            format(r['date'][0:4], r['date'][4:6], r['date'][6:8]), axis=1)
        logging.info('Converted stx_df dates in yyyy-mm-dd format')
        dates = stx_df.groupby(by='date')['ticker'].count()
        next_date = stxcal.next_busday(last_db_date)
        ix0, num_dates = 0, len(dates)
        logging.info('Data available for {0:d} dates, from {1:s} to {2:s}; DB '
                     'needs data starting from {3:s}'.format(
                len(dates), dates.index[0], dates.index[num_dates - 1],
                next_date))
        db_dates = []
        while ix0 < num_dates:
            if dates.index[ix0] == next_date:
                break
            ix0 += 1
        for ixx in range(ix0, num_dates):
            if dates.index[ixx] == next_date and dates.values[ixx] > 9000:
                db_dates.append(dates.index[ixx])
            else:
                if dates.index[ixx] != next_date:
                    logging.error(f'Missing date {next_date}; got '
                                  f'{dates.index[ixx]} instead') 
                    
                if dates.values[ixx] < 9000:
                    logging.error(f'Not enough records ({dates.values[ixx]}) '
                                  f'available for {dates.index[ixx]}') 
                break
            next_date = stxcal.next_busday(next_date)

        if not db_dates:
            logging.info('No new data available for processing. Exiting')
            return
        logging.info('Check that there are no time gaps between DB data and '
                     'upload data')
        start_date = stxcal.next_busday(last_db_date)
        num_bdays = stxcal.num_busdays(start_date, db_dates[0])
        if num_bdays > 0:
            logging.warn('No data for {0:d} days ({1:s} - {2:s}). Exiting ...'.
                         format(num_bdays, start_date,
                                stxcal.prev_busday(db_dates[0])))
            return
        logging.info('Check that there are no time gaps in the upload data')
        for ixx in range(len(db_dates) - 1):
            if stxcal.next_busday(db_dates[ixx]) != db_dates[ixx + 1]:
                logging.warn('Inconsistent dates {0:s} and {1:s} '
                             'at indexes {2:d} and {3:d}'.
                             format(db_dates[ixx], db_dates[ixx + 1],
                                    ixx, ixx + 1))

        sel_stx_df = stx_df.query('date in @db_dates').copy()
        logging.info('{0:d}/{1:d} records found for following dates: [{2:s}]'.
                     format(len(sel_stx_df), len(stx_df), ', '.join(db_dates)))
        sel_stx_df['invalid'] = sel_stx_df.apply(
            lambda r: np.isnan(r['open']) or
            np.isnan(r['high']) or
            np.isnan(r['low']) or
            np.isnan(r['close']) or
            np.isnan(r['vol']) or r['vol'] == 0 or
            r['open'] > r['high'] or r['open'] < r['low'] or
            r['close'] > r['high'] or r['close'] < r['low'], 
            axis=1)
        valid_stx_df = sel_stx_df.query('not invalid').copy()
        logging.info('Found {0:d} valid records out of {1:d} records'.
                     format(len(valid_stx_df), len(sel_stx_df)))
        
        def process_row(r):
            stk = r['ticker'][:-3].replace("-.", ".P.").replace(
                "_", ".").replace('-', '.')
            o = int(100 * r['open'])
            hi = int(100 * r['high'])
            lo = int(100 * r['low'])
            c = int(100 * r['close'])
            v = int(r['vol'])
            v = v // 1000
            if v == 0:
                v = 1
            lst = [stk, o, hi, lo, c, v]
            return pd.Series(lst)
            
        valid_stx_df[['ticker', 'open', 'high', 'low', 'close', 'vol']] = \
            valid_stx_df.apply(process_row, axis=1)
        valid_stx_df['openint'] = 2
        valid_stx_df.drop(columns=['per', 'time', 'invalid'], axis=1,
                          inplace=True)
        valid_stx_df.columns = ['stk', 'dt', 'o', 'hi', 'lo', 'c', 'v', 'oi']

        with closing(stxdb.db_get_cnx().cursor()) as crs:
            sql = 'CREATE TEMPORARY TABLE temp_table ('\
                'stk VARCHAR(16) NOT NULL, '\
                'dt DATE NOT NULL, '\
                'o INTEGER NOT NULL, '\
                'hi INTEGER NOT NULL, '\
                'lo INTEGER NOT NULL, '\
                'c INTEGER NOT NULL, '\
                'v INTEGER, '\
                'oi INTEGER, '\
                'PRIMARY KEY(stk, dt))'
            crs.execute(sql)
            logging.info('Created temporary table')
            upload_data = valid_stx_df.values.tolist()
            execute_values(crs, 'INSERT INTO temp_table '
                           '(stk, dt, o, hi, lo, c, v, oi) VALUES %s',
                           upload_data)
            logging.info('Uploaded dataframe into temporary table')
            stxdb.db_write_cmd(
                'INSERT INTO eods (stk, dt, o, hi, lo, c, v, oi) '
                'SELECT * FROM temp_table ON CONFLICT (stk, dt) DO '
                'UPDATE SET o = EXCLUDED.o, hi = EXCLUDED.hi, '
                'lo = EXCLUDED.lo, c = EXCLUDED.c, v = EXCLUDED.v, '
                'oi = EXCLUDED.oi')
            logging.info('Uploaded data into eods table')
        last_upload_date = valid_stx_df['dt'].max()
        stxdb.db_write_cmd("UPDATE analyses SET dt='{0:s}' WHERE "
                           "analysis='eod_datafeed'".format(last_upload_date))
        logging.info('Updated latest eod datafeed date {0:s} in DB'.
                     format(last_upload_date))
        self.rename_stooq_file(dates.index[0], dates.index[num_dates - 1])

    def parse_stooq_intraday(self, intraday_file=None, intraday_date=None):
        logging.info('Checking if a new stooq file has been downloaded')
        if intraday_file is None:
            download_dir = self.config.get('datafeed', 'download_dir')
            stooq_file = os.path.join(download_dir, 'data_5.txt')
        else:
            stooq_file = intraday_file
        if not os.path.exists(stooq_file):
            logging.info('No new intraday file found.  Nothing to do.')
            return
        logging.info('Reading stooq file, renaming columns, getting intraday '
                     'US stocks data')
        try:
            df = pd.read_csv(stooq_file, dtype={
                "<TICKER>": "string",
                "<PER>": "string",
                "<DATE>": "string",
                "<TIME>": "int",
                "<OPEN>": float,
                "<HIGH>": float,
                "<LOW>": float,
                "<CLOSE>": float,
                "<VOL>": float,
                "<OPENINT>": int
            })
        except:
            with open(stooq_file) as stooqfile:
                frdr = csv.reader(stooqfile)
                line_num = 0
                for row in frdr:
                    line_num += 1
                    try:
                        stk = str(row[0])
                        per = str(row[1])
                        dt = str(row[2])
                        tm = int(row[3])
                        o = float(row[4])
                        hi = float(row[5])
                        lo = float(row[6])
                        c = float(row[7])
                        v = float(row[8])
                        oi = int(row[9])
                    except:
                        logging.error(f"Failed to parse line {line_num}")
            sys.exit(-1)
        df.columns = [x[1: -1].lower() for x in df.columns]
        stx_df = df.query('ticker.str.endswith(".US") and per == "5"',
                          engine='python')
        logging.info(f'Getting {len(stx_df)} intraday US stocks out of '
                     f'{len(df)} records')
        stx_df['date'] = stx_df['date'].astype(str)
        stx_df['date'] = stx_df.apply(
            lambda r: f"{r['date'][0:4]}-{r['date'][4:6]}-{r['date'][6:8]}",
            axis=1)
        if intraday_date:
            stx_df = stx_df.query('date == @intraday_date').copy()
        stx_df['time'] -= 60000
        stx_df.time = stx_df.time.astype(str).str.pad(6, fillchar='0')
        stx_df['dt'] = stx_df.apply(lambda r: f"{r['date']} {r['time'][0:2]}:"
                                    f"{r['time'][2:4]}:{r['time'][4:6]}",
                                    axis=1)
        stx_df['vol'] = stx_df['vol'].astype(int)
        logging.info('Formatted intraday timestamps like yyyy-mm-dd hh:mm:ss')
        logging.info(f"Intraday records available by date:")
        dates = stx_df.groupby(by='date')['ticker'].count()
        logging.info(f"\n{dates}")
        for dd in dates.index:
            logging.info(f'Processing intraday data for {dd}')
            daily_df = stx_df.query('date == @dd').copy()
            self.process_daily_intraday(dd, daily_df)
        if intraday_file is None:
            self.rename_stooq_file(dates.index[0], dates.index[-1],
                                   intraday=True)

    def process_daily_intraday(self, dt, daily_df):
        daily_df['invalid'] = daily_df.apply(
            lambda r: np.isnan(r['open']) or
            np.isnan(r['high']) or
            np.isnan(r['low']) or
            np.isnan(r['close']) or
            np.isnan(r['vol']) or r['vol'] == 0 or
            r['open'] > r['high'] or r['open'] < r['low'] or
            r['close'] > r['high'] or r['close'] < r['low'] or
            r['vol'] >= 2147483647 or r['high'] >= 21474836.47,
            axis=1)
        valid_stx_df = daily_df.query('not invalid').copy()
        invalid_stx_df = daily_df.query('invalid').copy()
        if len(invalid_stx_df) > 0:
            logging.warn(f'{dt}: found {len(invalid_stx_df)} invalid records:')
            for ixx, row in invalid_stx_df.iterrows():
                print(f"{ixx}: {row['ticker']} {row['dt']} {row['open']} "
                      f"{row['high']} {row['low']} {row['close']} "
                      f"{row['vol']}")
        logging.info(f'{dt}: {len(valid_stx_df)}/{len(daily_df)} valid/total '
                     f'records')
        valid_stx_df['open'] *= 100
        valid_stx_df['high'] *= 100
        valid_stx_df['low'] *= 100
        valid_stx_df['close'] *= 100
        valid_stx_df['open'] = valid_stx_df['open'].astype(int)
        valid_stx_df['high'] = valid_stx_df['high'].astype(int)
        valid_stx_df['low'] = valid_stx_df['low'].astype(int)
        valid_stx_df['close'] = valid_stx_df['close'].astype(int)
        valid_stx_df['ticker'] = valid_stx_df.apply(
            lambda r: r['ticker'][:-3].replace("-.", ".P.").replace(
                "_", ".").replace('-', '.'), axis=1)
        valid_stx_df['openint'] = 2
        valid_stx_df.drop(columns=['per', 'date', 'time', 'invalid'], axis=1,
                          inplace=True)
        logging.debug(f'\n{valid_stx_df}')
        valid_stx_df.columns = ['stk', 'o', 'hi', 'lo', 'c', 'v', 'oi', 'dt']
        with closing(stxdb.db_get_cnx().cursor()) as crs:
            sql1 = 'DROP TABLE IF EXISTS temp_intraday_table'
            crs.execute(sql1)
            sql2 = 'CREATE TEMPORARY TABLE temp_intraday_table ('\
                'stk VARCHAR(16) NOT NULL, '\
                'dt TIMESTAMP NOT NULL, '\
                'o INTEGER NOT NULL, '\
                'hi INTEGER NOT NULL, '\
                'lo INTEGER NOT NULL, '\
                'c INTEGER NOT NULL, '\
                'v INTEGER, '\
                'oi INTEGER, '\
                'PRIMARY KEY(stk, dt))'
            crs.execute(sql2)
            logging.debug('Created temporary table')
            upload_data = valid_stx_df.values.tolist()
            execute_values(crs, 'INSERT INTO temp_intraday_table '
                           '(stk, o, hi, lo, c, v, oi, dt) VALUES %s',
                           upload_data)
            logging.debug(f'{dt}: uploaded dataframe into temporary table')
            stxdb.db_write_cmd(
                'INSERT INTO intraday (stk, dt, o, hi, lo, c, v, oi) '
                'SELECT * FROM temp_intraday_table ON CONFLICT (stk, dt) DO '
                'UPDATE SET o = EXCLUDED.o, hi = EXCLUDED.hi, '
                'lo = EXCLUDED.lo, c = EXCLUDED.c, v = EXCLUDED.v, '
                'oi = EXCLUDED.oi')
            logging.info(f'{dt}: uploaded data into intraday table')

    def rename_stooq_file(self, first_date, last_date, intraday=False):
        data_dir = self.config.get('datafeed', 'data_dir')
        stooq_file = os.path.join(data_dir,
                                  'data_5.txt' if intraday else 'data_d.txt')
        if os.path.exists(stooq_file):
            archive_file = os.path.join(
                data_dir,
                f"stooq{'ID' if intraday else ''}_{first_date}_{last_date}.txt")
            os.rename(stooq_file, archive_file)
            logging.info(f'Moved {stooq_file} into {archive_file}')

    def get_profile_count(self, dt):
        profile_count = 0
        logging.info(f'Checking if sectors, industries and stock profiles '
                     f'have been downloaded for the last expiry date {dt}')
        q = sql.Composed([
            sql.SQL("SELECT COUNT(*) FROM ind_groups WHERE dt="),
            sql.Literal(dt),
            sql.SQL(" AND sector!="),
            sql.Literal('N/A')
        ])
        try:
            res = stxdb.db_read_cmd(q.as_string(stxdb.db_get_cnx()))
            profile_count = res[0][0]
        except:
            logging.error(f'Failed to retrieve the profile count from DB')
            tb.print_exc()
            sys.exit(-1)
        logging.info(f'Found {profile_count} stock profiles in the DB '
                     f'as of {last_expiry}')
        return profile_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', type=Datafeed,
                        choices=list(Datafeed), default=Datafeed.stooq)
    parser.add_argument('-d', '--data_dir',
                        help='download directory for EOD files',
                        type=str,
                        default=os.path.join(os.getenv('HOME'), 'Downloads'))
    parser.add_argument('-i', '--intraday_file',
                        help='Intraday data file to load in DB',
                        type=str)
    parser.add_argument('-x', '--intraday_date',
                        help='Particular date to parse from intraday file',
                        type=str)
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    if args.intraday_file is not None:
        logging.info(f'Process intraday data from file {args.intraday_file}')
        if args.intraday_date:
            logging.info(f' for this date: {args.intraday_date}')
        sdf = StxDatafeed()
        sdf.parse_stooq_intraday(args.intraday_file, args.intraday_date)
        sys.exit(0)
    logging.info('Getting index (S&P500, Nasdaq, Dow Jones) quotes')
    si = StxIndex()
    index_end_date = stxcal.current_busdate(hr=9)
    index_start_date = stxcal.move_busdays(index_end_date, -5)
    for idx in ['^GSPC', '^IXIC', '^DJI']:
        try:
            si.get_quote(idx, index_start_date, index_end_date)
        except:
            logging.error(f'Get index quote failed for {idx}')
            tb.print_exc()
    sdf = StxDatafeed()
    res = stxdb.db_read_cmd("SELECT dt FROM analyses WHERE "
                            "analysis='eod_datafeed'")
    start_date = str(res[0][0]) if res else '2000-01-01'
    logging.info('The start date is: {0:s}'.format(start_date))
    res = stxdb.db_read_cmd("SELECT MAX(dt) FROM dividends")
    splits_start_date = str(res[0][0]) if res else '2000-01-01'
    sdf.parse_stooq_eod(start_date)
    sdf.parse_stooq_intraday()
    last_expiry = stxcal.prev_expiry(str(datetime.datetime.now().date()))
    profile_count = sdf.get_profile_count(last_expiry)
    if profile_count < 5000:
        logging.info(f'Not sufficient data available for {last_expiry}, '
                     f'downloading stock profiles')
        stxgrps.stock_groups(last_expiry)
        stxgrps.populate_sectors(last_expiry)
        stxgrps.populate_industries(last_expiry)
    sdf.handle_splits(splits_start_date)
    sdf.backup_database()
