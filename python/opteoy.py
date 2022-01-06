import argparse
import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import os
from shutil import rmtree
import stxcal
import stxdb
import sys
import traceback
import zipfile


class OptEOY:
    data_dir = os.getenv('DATA_DIR')
    upload_dir = '/tmp'
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April',
                   5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September',
                   10: 'October', 11: 'November', 12: 'December'}

    def __init__(self, in_dir, opt_tbl='options', spot_tbl='opt_spots',
                 upload_options=True, upload_spots=True):
        self.in_dir = in_dir
        self.opt_tbl = opt_tbl
        self.spot_tbl = spot_tbl
        self.upload_spots = upload_spots
        self.upload_options = upload_options
        logging.info('Using downloaded options archives from {0:s}'.
                     format(in_dir))
        logging.info('Will {0:s} spots in table {1:s}'.
                     format('upload' if upload_spots else 'not upload',
                            self.spot_tbl))
        logging.info('Will {0:s} options in table {1:s}'.
                     format('upload' if upload_options else 'not upload',
                            self.opt_tbl))

    def load_opts(self, start_date, end_date):
        s_tokens = start_date.split('-')
        start_day = 0
        if len(s_tokens) == 2:
            start_year, start_month = int(s_tokens[0]), int(s_tokens[1])
        elif len(s_tokens) == 3:
            start_year, start_month, start_day = int(s_tokens[0]), int(
                s_tokens[1]), int(s_tokens[2])
#         start_year, start_month = [int(x) for x in start_date.split('-')]
        end_year, end_month = [int(x) for x in end_date.split('-')]
        for year in range(start_year, end_year + 1):
            y_spots, y_opts = 0, 0
            for month in range(1, 13):
                if (year == start_year and month < start_month) or \
                   (year == end_year and month > end_month):
                    continue
                if year == start_year and month == start_month:
                    day = start_day
                else:
                    day = 0
                zip_fname = os.path.join(
                    self.in_dir,
                    'bb_{0:d}_{1:s}.zip'.format(year, self.month_names[month]))
                if os.path.exists(zip_fname):
                    m_spots, m_opts = self.load_opts_archive(zip_fname, year,
                                                             month, day)
                    y_spots += m_spots
                    y_opts += m_opts
            print('{0:s}\t{1:10d}\t{2:5d}\t{3:6d}'.
                  format(stxcal.print_current_time(), year, y_spots, y_opts))

    def load_opts_archive(self, zip_fname, year, month, day=0):
        # remove the tmp directory (if it already exists, and recreate it
        tmp_dir = '{0:s}/tmp'.format(self.data_dir)
        if os.path.exists(tmp_dir):
            rmtree(tmp_dir)
        os.makedirs(tmp_dir)
        m_spots, m_opts = 0, 0
        with zipfile.ZipFile(zip_fname, 'r') as zip_file:
            zip_file.extractall(tmp_dir)
        daily_opt_files = os.listdir(tmp_dir)
        for opt_file in daily_opt_files:
            file_day = int(opt_file[17:19])
            if file_day < day:
                logging.info('Skipping file {0:s}, because it is before '
                             'start date {1:d}-{2:02d}-{3:02d}'.
                             format(opt_file, year, month, day))
                continue
            d_spots, d_opts = self.load_opts_daily(os.path.join(tmp_dir,
                                                                opt_file))
            m_spots += d_spots
            m_opts += d_opts
        # Remove the tmp directory after we are done with the current
        # archive
        rmtree(tmp_dir)
        print('{0:s}\t{1:7d}-{2:02d}\t{3:5d}\t{4:6d}'.format
              (stxcal.print_current_time(), year, month, m_spots, m_opts))
        return m_spots, m_opts

    def load_opts_daily(self, opt_fname):
        # print('{0:s} -load_opts_daily'.format(stxcal.print_current_time()))
        invalid_days = ['2002-02-01', '2002-02-04', '2002-02-05', '2002-02-06',
                        '2002-02-07', '2002-05-30', '2002-05-31', '2002-06-14',
                        '2002-06-17', '2002-12-02', '2002-12-03', '2002-12-04',
                        '2002-12-05', '2002-12-06', '2002-12-09', '2002-12-10']
        dt = '{0:s}-{1:s}-{2:s}'.format(opt_fname[-12:-8], opt_fname[-8:-6],
                                        opt_fname[-6:-4])
        if dt in invalid_days:
            return 0, 0
        # print('dt = {0:s}'.format(dt))
        exp_0 = stxcal.next_expiry(dt, min_days=0)
        # print('exp_0 = {0:s}'.format(exp_0))
        exp_1 = stxcal.next_expiry(exp_0)
        # print('exp_1 = {0:s}'.format(exp_1))
        exps = [str(exp_0), str(exp_1)]
        spot_dct, opt_dct, spots, opts = {}, {}, [], []
        stx = {}
        sep = ' '
        # print('opt_fname = {0:s}'.format(opt_fname))
        with open(opt_fname) as csvfile:
            frdr = csv.reader(csvfile)
            for row in frdr:
                stk = row[0]
                # print('dt = {0:s}, stk = {1:s}, exps = {2:s}'.
                #       format(dt, stk, str(exps)))
                # print('row = {0:s}'.format(str(row)))
                try:
                    spot = int(100 * float(row[1]))
                    cp = row[5][:1]
                    exp = str(datetime.strptime(row[6], '%m/%d/%Y').date())
                    strike = int(100 * float(row[8]))
                    bid = int(100 * float(row[10]))
                    ask = int(100 * float(row[11]))
                    volume = int(row[12])
                    # print('exp = {0:s}'.format(exp))
                except:
                    # print(traceback.print_exc())
                    continue
                # print('1')
                if exp not in exps or ask == 0 or spot >= 2147483647 or \
                   strike >= 2147483647 or bid >= 2147483647 or \
                   ask >= 2147483647 or volume >= 2147483647:
                    continue
                # print('2')
                if stk not in spot_dct:
                    spot_dct[stk] = spot
                    spots.append([stk, dt, spot])
                opt_key = ':'.join([exp, stk, cp, str(strike)])
                if opt_key not in opt_dct:
                    opt_dct[opt_key] = [bid, ask, volume]
                    opts.append([exp, stk, cp, strike, dt, bid, ask, volume])
                # print('len(spots) {0:d}, len(opts) = {1:d}'.format(
                #     len(spots), len(opts)))

        spots_upload_file = '{0:s}/spots.txt'.format(self.upload_dir)
        opts_upload_file = '{0:s}/opts.txt'.format(self.upload_dir)
        with open(spots_upload_file, 'w') as spots_file:
            for s in spots:
                spots_file.write('{0:s}\t{1:s}\t{2:d}\n'.
                                 format(s[0], s[1], s[2]))
        with open(opts_upload_file, 'w') as opts_file:
            for o in opts:
                opts_file.write('{0:s}\t{1:s}\t{2:s}\t{3:d}\t{4:s}\t{5:d}\t'
                                '{6:d}\t{7:d}\t0\n'.format(o[0], o[1], o[2],
                                                           o[3], o[4], o[5],
                                                           o[6], o[7]))
        if self.upload_spots:
            stxdb.db_upload_file(spots_upload_file, self.spot_tbl, '\t')
        if self.upload_options:
            stxdb.db_upload_file(opts_upload_file, self.opt_tbl, '\t')
        d_spots, d_opts = len(spots), len(opts)
        print('{0:s}\t{1:s}\t{2:5d}\t{3:6d}'.format
              (stxcal.print_current_time(), dt, d_spots, d_opts))
        return d_spots, d_opts

    # These deltaneutral days contain garbage that sets off the reconciliation
    # process and trading simulations.  We are better off removing those days
    # from the opts and opt_spots tables
    def remove_invalid_days(self):
        invalid_days = "('2002-02-01', '2002-02-04', '2002-02-05', "\
                       "'2002-02-06', '2002-02-07', '2002-05-30', "\
                       "'2002-05-31', '2002-06-14', '2002-06-17', "\
                       "'2002-12-02', '2002-12-03', '2002-12-04', "\
                       "'2002-12-05', '2002-12-06', '2002-12-09', "\
                       "'2002-12-10')"
        print('{0:s}: removing invalid days from opt_spots and opts tables.'.
              format(stxcal.print_current_time()))
        stxdb.db_write_cmd('delete from {0:s} where dt in {1:s}'.
                           format(self.spot_tbl, invalid_days))
        print('{0:s}: removed invalid days from {1:s} table.'.
              format(stxcal.print_current_time(), self.spot_tbl))
        stxdb.db_write_cmd('delete from {0:s} where dt in {1:s}'.
                           format(self.opt_tbl, invalid_days))
        print('{0:s}: removed invalid days from {1:s} table.'.
              format(stxcal.print_current_time(), self.opt_tbl))


    def move_downloaded_options(self, start_yymm, end_yymm):
        start_yy, start_mm = start_yymm.split('-')
        start_year = int(start_yy)
        start_month = int(start_mm)
        end_yy, end_mm = end_yymm.split('-')
        end_year = int(end_yy)
        end_month = int(end_mm)
        start_date = '{0:d}-{1:02d}-01'.format(start_year, start_month)
        logging.info('start_date = {0:s}'.format(start_date))
        if not stxcal.is_busday(start_date):
            start_date = stxcal.next_busday(start_date)
        if end_month == 12:
            end_year += 1
        end_month = (end_month + 1) % 12
        end_date = '{0:d}-{1:02d}-01'.format(end_year, end_month)
        logging.info('end_date = {0:s}'.format(end_date))
        end_date = stxcal.prev_busday(end_date)
        logging.info('Moving to downloaded_options table all options dated '
                     'between {0:s} and {1:s}'.format(start_date, end_date))
        sql = 'INSERT INTO downloaded_options '\
            '(expiry, und, cp, strike, dt, bid, ask, v, oi) '\
            "SELECT * FROM options WHERE dt BETWEEN '{0:s}' AND '{1:s}' "\
            'ON CONFLICT (expiry, und, cp, strike, dt) DO NOTHING'.format(
            start_date, end_date)
        logging.info('sql cmd: {0:s}'.format(sql))
        stxdb.db_write_cmd(sql)
        logging.info('Moved to downloaded_options table all options dated '
                     'between {0:s} and {1:s}'.format(start_date, end_date))
        logging.info('Removing from options table all options downloaded '
                     'between {0:s} and {1:s}'.format(start_date, end_date))
        sql = "DELETE FROM options WHERE dt BETWEEN '{0:s}' AND '{1:s}'"\
            " ".format(start_date, end_date)
        logging.info('sql cmd: {0:s}'.format(sql))
        stxdb.db_write_cmd(sql)
        logging.info('Removed from options table all options downloaded '
                     'between {0:s} and {1:s}'.format(start_date, end_date))


# TODO: make the following changes:
# 1. Use parseargs to parse the arguments
# 2. Pass the year-month for starting and ending upload as parameters
# 3. Create donwloaded_options table and save in that table all the options 
#    that have been downloaded during the year.
# 4. Run the options upload program
# 5. Automatically backup the zip archives on the USB sticks

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--no_spots', action='store_true',
                        help='Do not upload spots')
    parser.add_argument('-o', '--no_options', action='store_true',
                        help='Do not upload options')
    parser.add_argument('-d', '--data_dir',
                        help='download directory for options files',
                        type=str,
                        default=os.path.join(os.getenv('HOME'), 'Downloads'))
    parser.add_argument('-s', '--start',
                        help='yyyy-mm 1st upload month; or specific date',
                        type=str,
                        default='2002-02')
    parser.add_argument('-e', '--end',
                        help='yyyy-mm last month to upload in db',
                        type=str,
                        default='2020-12')
    parser.add_argument('-m', '--move_downloaded_options', action='store_true',
                        help='Move downloaded options')
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    opt_eoy = OptEOY(
        args.data_dir, opt_tbl='options', spot_tbl='opt_spots',
        upload_options=(not args.no_options), upload_spots=(not args.no_spots))
    if args.move_downloaded_options:
        opt_eoy.move_downloaded_options(args.start, args.end)
    opt_eoy.load_opts(args.start, args.end)
