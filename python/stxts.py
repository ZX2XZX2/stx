import numpy as np
import pandas as pd
import stxdb
import stxcal


class StxTS:
    # static variables
    busday_us = pd.tseries.offsets.CDay(holidays=stxcal.get_cal().holidays)

    def __init__(self, stk, sd, ed, eod_tbl='eods', split_tbl='dividends'):
        self.stk = stk
        self.sd = pd.to_datetime(sd)
        self.ed = pd.to_datetime(ed)
        q = "select * from {0:s} where stk='{1:s}' and dt "\
            "between '{2:s}' and '{3:s}' order by dt".format(
            eod_tbl, stk, sd, ed)
        df = pd.read_sql(q, stxdb.db_get_cnx(), index_col='dt',
                         parse_dates=['dt'])
        if self.sd < df.index[0]:
            self.sd = df.index[0]
        if self.ed > df.index[-1]:
            self.ed = df.index[-1]
        self.sd_str = str(self.sd.date())
        self.ed_str = str(self.ed.date())
        self.gaps = self.get_gaps(df)
        df.drop(['stk', 'prev_dt', 'prev_date', 'gap'], axis=1, 
                inplace=True)
        s_lst = stxdb.db_read_cmd("select dt, ratio, divi_type from "
                                  "{0:s} where stk='{1:s}'".
                                  format(split_tbl, stk))
        # print('stk = {0:s}, s_lst = {1:s}'.format(stk, str(s_lst)))
        self.splits = {pd.to_datetime(stxcal.next_busday(s[0])):
                       [float(s[1]), int(s[2])] for s in s_lst}
        self.df = self.fill_gaps(df)
        self.l = len(self.df)
        self.pos = 0
        self.num_gaps = [tuple([self.find(str(x[0].date())),
                                self.find(str(x[1].date()))])
                         for x in self.gaps]
        self.start = self.num_gaps[0][0]
        self.end = self.num_gaps[0][1]
        self.adj_splits = []

    def get_gaps(self, df):
        df['prev_dt'] = df.index.shift(-1, freq=StxTS.busday_us)
        df['prev_date'] = df.index
        df['prev_date'] = df['prev_date'].shift(1)
        s1 = pd.Series(df['prev_date'])
        s1[0] = pd.to_datetime(stxcal.prev_busday(df.index[0].date()))

        def gapfun(x):
            return stxcal.num_busdays(x['prev_date'].date(),
                                      x['prev_dt'].date())
        df['gap'] = df.apply(gapfun, axis=1)
        glist = df.loc[df['gap'] > 20].index.tolist()
        gaps = []
        if len(glist) == 0:
            gaps.append(tuple([self.sd, self.ed]))
        else:
            gaps.append(tuple([self.sd, df.loc[glist[0]]['prev_date']]))
            for i in range(1, len(glist)):
                gaps.append(
                    tuple([glist[i - 1], df.loc[glist[i]]['prev_date']]))
            gaps.append(tuple([glist[-1], self.ed]))
        return gaps

    def fill_gaps(self, df):
        idx = pd.date_range(self.sd, self.ed, freq=StxTS.busday_us)
        df = df.reindex(idx)
        df['c'] = df['c'].ffill()
        df['v'] = df['v'].fillna(0)
        df.loc[df['o'].isnull(), 'o'] = df['c']
        df.loc[df['hi'].isnull(), 'hi'] = df['c']
        df.loc[df['lo'].isnull(), 'lo'] = df['c']
        return df

    def data(self):
        return self.df

    def find(self, dt, c=0):
        n = stxcal.num_busdays(self.sd_str, dt)
        if n < 0:
            if c > 0:
                return 0
        elif n >= self.l:
            if c < 0:
                return self.l - 1
        else:
            df_dt = str(self.df.index[n].date())
            if df_dt == dt:
                return n
            # df_dt will always be less than dt, if dt is not a business day
            if c < 0:
                return n - 1
            if c > 0:
                return n
        return -1

    def set_day(self, dt, c=0):
        new_pos = self.find(dt, c)
        if new_pos == -1:
            return new_pos
        self.adjust_data(new_pos)
        return new_pos

    def next_day(self, adj_dir=1):
        new_pos = self.pos + adj_dir
        if new_pos < self.start or new_pos > self.end:
            return -1
        self.adjust_data(new_pos)
        return new_pos

    def adjust_data(self, new_pos):
        # A new date can:
        # (1) move into a new date range, or (2) stay in the old one
        if self.pos == new_pos:
            return new_pos
        new_s, new_e = [g for g in self.num_gaps if g[0] <= new_pos <= g[1]][0]
        # in (1): undo all split adjustments in the old date range,
        # adjust for splits in the new date range, & reset self.start/self.end
        if new_s != self.start or new_e != self.end:
            sdd = self.df.index[self.start]
            for adj_dt in self.adj_splits:
                split_ratio, split_divi = self.splits.get(adj_dt)
                self.adjust(sdd, stxcal.prev_busday(adj_dt), 1 / split_ratio,
                            ['o', 'hi', 'lo', 'c'])
                if split_divi in [0, 1, 3, 6]:
                    self.adjust(sdd, stxcal.prev_busday(adj_dt), split_ratio,
                                ['volume'])
            del self.adj_splits[:]
            self.start = new_s
            self.end = new_e
            self.adjust_splits_date_range(self.start, new_pos)
        # in (2), if moving:
        # forward, adjust for splits between the old and the new position,
        # backward, undo split adjustments between the old and the new position
        else:
            if new_pos > self.pos:
                self.adjust_splits_date_range(self.pos, new_pos)
            else:
                self.adjust_splits_date_range(new_pos, self.pos, 1)
        self.pos = new_pos

    def adjust_splits_date_range(self, s_ix, e_ix, inv=0):
        bdd = self.df.index[self.start]
        sdd = self.df.index[s_ix]
        edd = self.df.index[e_ix]
        splts = {k: v for k, v in self.splits.items() if sdd < k <= edd}
        for k, v in splts.items():
            r = v[0] if inv == 0 else 1 / v[0]
            self.adjust(bdd, stxcal.prev_busday(k), r, ['o', 'hi', 
                                                        'lo', 'c'])
            if v[1] in [0, 1, 3, 6]:
                self.adjust(bdd, stxcal.prev_busday(k), 1 / r, ['v'])
            self.adj_splits.append(k)

    def adjust(self, s_idx, e_idx, ratio, col_names):
        for c_name in col_names:
            self.df.loc[s_idx:e_idx, c_name] = self.df[c_name] * ratio

    def mergetbl(self, tbl_name, col_list, addl_cond=None):
        q = "select date,%s from %s where stk='%s' and date between '%s' and '%s'"
        " %s" % (','.join(col_list), tbl_name, self.stk, self.sd, self.ed,
                 "" if addl_cond is None else addl_cond)
        sql_res = pd.read_sql(q, stxdb.db_get_cnx(), index_col='date',
                              parse_dates=['date'])
        self.df = self.df.merge(sql_res, how='left', left_index=True,
                                right_index=True)

    def buckets(self, col_name, lb, ub, bucket, precision, suffix='b'):
        prec_str = '%%.%df' % precision
        bucket_name = "%s%s" % (col_name, suffix)
        self.df[bucket_name] = self.df[col_name].map(lambda x: np.nan if
                                                     np.isnan(x) else
                                                     lb if x < lb else
                                                     ub if x > ub else
                                                     float(prec_str %
                                                           (x / bucket)) *
                                                     bucket)

    def addleader(self, tdf, liq='PctLiq', min_rank=1, max_rank=1000):
        q = "select * from rm_ldrs where stk='%s' and rm_ten='%s' and " \
            "rank>=%d and rank<=%d" % (self.stk, liq, min_rank, max_rank)
        # print("addleader q=%s" % q)
        ldr = pd.read_sql(q, stxdb.db_get_cnx(), parse_dates=['exp'])
        # print("%5s read the leaders: %s" % (" ", datetime.datetime.now()))
        # print("Found %d leader time intervals for %s" % (len(ldr), self.stk))
        ldrs = tdf.merge(ldr, how='left', on=['exp'])
        # print("%5s merged with TDF: %s" % (" ", datetime.datetime.now()))
        ldrs.set_index('date', inplace=True)
        # print("%5s set the index: %s" % (" ", datetime.datetime.now()))

        def ldrfun1(x):
            return 0 if(np.isnan(x['rank']) or x['rank'] == 0) else 1
        ldrs['rank'].fillna(0, inplace=True)
        # print("%5s filled n/a: %s" % (" ", datetime.datetime.now()))
        ldrs['ldr'] = ldrs.apply(ldrfun1, axis=1)
        # print("%5s applied ldrfun1: %s" % (" ", datetime.datetime.now()))

        def ldrfun2(x):
            return -x['ldr'] if x['inv'] == 1 else x['ldr']
        ldrs['ldr'] = ldrs.apply(ldrfun2, axis=1)
        # print("%5s applied ldrfun2: %s" % (" ", datetime.datetime.now()))
        ldrs.drop(['exp', 'rm_ten', 'inv', 'stk'], axis=1, inplace=True)
        # print("%5s dropped columns: %s" % (" ", datetime.datetime.now()))
        self.df = self.df.merge(ldrs, how='left', left_index=True,
                                right_index=True)

    def current_date(self):
        return str(self.df.index[self.pos].date())

    def current(self, col_name):
        return self.df.ix[self.pos][col_name]

    def ix(self, ixx):
        return self.df.ix[ixx]

    def rel_strength(self, dt, w):
        if self.current_date() != dt:
            self.set_day(dt)
        start_ix = self.pos - w
        if start_ix < self.start:
            start_ix = self.start
        rs_w = self.pos - start_ix
        if rs_w < 20:
            return 0
        cc = self.current('c')
        return 40 * (cc / self.ix(self.pos + 1 - rs_w // 4).c - 1) + \
            30 * (cc / self.ix(self.pos + 1 - rs_w // 2).c - 1) + \
            30 * (cc / self.ix(start_ix + 1).c - 1)

    @staticmethod
    def gen_tdf(sd, ed):
        tdf = pd.DataFrame(data={'date': pd.date_range(sd, ed,
                                                       freq=StxTS.busday_us)})

        def funexp(x):
            return stxcal.prev_expiry(np.datetime64(x['date'].date()))
        tdf['exp'] = tdf.apply(funexp, axis=1)
        return tdf


if __name__ == '__main__':
    stk = 'TASR'
    sd = '2002-04-01'
    ed = '2002-04-11'
    # expiries = StxTS.sc.expiries(sd[:-3], ed[:-3])
    ts = StxTS(stk, sd, ed)
    print("find('2002-04-10') = %d" % ts.find('2002-04-10'))
    print("find('2002-04-06', 1) = %d" % ts.find('2002-04-06', -1))
    print("find('2002-04-06', -1) = %d" % ts.find('2002-04-06', 1))
    tdf = StxTS.gen_tdf(sd, ed)
    ts.addleader(tdf, 1000)
    #    ts.mergejl()
    #    ts.analyze()
    stk = 'VXX'
    sd = '2012-10-01'
    ed = '2012-10-10'
    vxx = StxTS(stk, sd, ed)
    dt = '2012-10-04'
    vxx.set_day(dt)
    print('After setting the date to %s, VXX is:\n%s' % (dt, vxx.df))
    dt = '2012-10-02'
    vxx.set_day(dt)
    print('After setting the date to %s, VXX is:\n%s' % (dt, vxx.df))
    dt = '2012-10-04'
    vxx.set_day(dt)
    print('After setting the date to %s, VXX is:\n%s' % (dt, vxx.df))
    dt = '2012-10-08'
    vxx.set_day(dt)
    print('After setting the date to %s, VXX is:\n%s' % (dt, vxx.df))
    dt = '2012-10-01'
    vxx.set_day(dt)
    print('After setting the date to %s, VXX is:\n%s' % (dt, vxx.df))
    x = 0
    while x != -1:
        x = vxx.next_day()
        if x != -1:
            print('Date = %s, VXX is:\n%s' % (vxx.df.index[x], vxx.df))
