import numpy as np
import pandas as pd
import pytz
import stxdb
import sys

from datetime import datetime, timedelta
from dateutil import rrule

this = sys.modules[__name__]
this.cal = None
epoch = datetime.utcfromtimestamp(0)

def get_cal(start=None, end=None):
    if this.cal is None:
        if start is None:
            start = '1901-01-01'
        start = pd.Timestamp(start, tz='UTC')
        if end is None:
            end = datetime.now().date()
            end += timedelta(weeks=52)
        end = pd.Timestamp(end, tz='UTC')
        print('Initializing calendar between {0:s} and {1:s}'.
              format(str(start.date()), str(end.date())))
        non_trading_rules = []
        new_years = rrule.rrule(
            rrule.MONTHLY,
            byyearday=1,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(new_years)

        new_years_sunday = rrule.rrule(
            rrule.MONTHLY,
            byyearday=2,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(new_years_sunday)

        mlk_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=1,
            byweekday=rrule.MO,
            bysetpos=3,
            cache=True,
            dtstart=datetime(1998, 1, 1, tzinfo=pytz.utc),
            until=end
        )
        non_trading_rules.append(mlk_day)

        lincoln_birthday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            bymonthday=12,
            cache=True,
            dtstart=start,
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(lincoln_birthday)

        lincoln_birthday_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            bymonthday=13,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(lincoln_birthday_sunday)

        lincoln_birthday_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            bymonthday=11,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(lincoln_birthday_saturday)

        washington_birthday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            bymonthday=22,
            cache=True,
            dtstart=start,
            until=datetime(1971, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(washington_birthday)

        washington_birthday_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            bymonthday=23,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=datetime(1971, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(washington_birthday_sunday)

        washington_birthday_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            bymonthday=21,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=datetime(1971, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(washington_birthday_saturday)

        presidents_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            byweekday=rrule.MO,
            bysetpos=3,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(presidents_day)

        national_banking_holiday = rrule.rrule(
            rrule.DAILY,
            count=6,
            byweekday=(rrule.MO, rrule.TU, rrule.WE, rrule.TH, rrule.FR),
            cache=True,
            dtstart=datetime(1933, 3, 6, tzinfo=pytz.utc)
        )
        non_trading_rules.append(national_banking_holiday)

        good_friday_0 = rrule.rrule(
            rrule.DAILY,
            byeaster=-2,
            cache=True,
            dtstart=start,
            until=datetime(1906, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(good_friday_0)

        good_friday = rrule.rrule(
            rrule.DAILY,
            byeaster=-2,
            cache=True,
            dtstart=datetime(1908, 1, 1, tzinfo=pytz.utc),
            until=end
        )
        non_trading_rules.append(good_friday)

        memorial_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=5,
            bymonthday=30,
            cache=True,
            dtstart=start,
            until=datetime(1971, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(memorial_day)

        memorial_day_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=5,
            bymonthday=31,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=datetime(1971, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(memorial_day_sunday)

        memorial_day_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=5,
            bymonthday=29,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=datetime(1971, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(memorial_day_saturday)

        memorial_day_new = rrule.rrule(
            rrule.MONTHLY,
            bymonth=5,
            byweekday=rrule.MO,
            bysetpos=-1,
            cache=True,
            dtstart=datetime(1971, 1, 1, tzinfo=pytz.utc),
            until=end
        )
        non_trading_rules.append(memorial_day_new)

        flag_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=6,
            bymonthday=14,
            cache=True,
            dtstart=datetime(1916, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(flag_day)

        flag_day_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=6,
            bymonthday=15,
            byweekday=rrule.MO,
            cache=True,
            dtstart=datetime(1916, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(flag_day_sunday)

        flag_day_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=6,
            bymonthday=13,
            byweekday=rrule.FR,
            cache=True,
            dtstart=datetime(1916, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(flag_day_saturday)

        july_4th = rrule.rrule(
            rrule.MONTHLY,
            bymonth=7,
            bymonthday=4,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(july_4th)

        july_4th_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=7,
            bymonthday=5,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(july_4th_sunday)

        july_4th_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=7,
            bymonthday=3,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(july_4th_saturday)

        labor_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=9,
            byweekday=rrule.MO,
            bysetpos=1,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(labor_day)

        columbus_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=10,
            bymonthday=12,
            cache=True,
            dtstart=datetime(1909, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(columbus_day)

        columbus_day_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=10,
            bymonthday=13,
            byweekday=rrule.MO,
            cache=True,
            dtstart=datetime(1909, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(columbus_day_sunday)

        columbus_day_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=10,
            bymonthday=11,
            byweekday=rrule.FR,
            cache=True,
            dtstart=datetime(1909, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(columbus_day_saturday)

        election_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=11,
            bymonthday=(2, 3, 4, 5, 6, 7, 8),
            byweekday=rrule.TU,
            cache=True,
            dtstart=start,
            until=datetime(1969, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(election_day)

        pres_election_day = rrule.rrule(
            rrule.YEARLY,
            interval=4,
            bymonth=11,
            bymonthday=(2, 3, 4, 5, 6, 7, 8),
            byweekday=rrule.TU,
            cache=True,
            dtstart=datetime(1972, 1, 1, tzinfo=pytz.utc),
            until=datetime(1981, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(pres_election_day)

        veterans_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=11,
            bymonthday=11,
            cache=True,
            dtstart=datetime(1934, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(veterans_day)

        veterans_day_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=11,
            bymonthday=12,
            byweekday=rrule.MO,
            cache=True,
            dtstart=datetime(1934, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(veterans_day_sunday)

        veterans_day_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=11,
            bymonthday=10,
            byweekday=rrule.FR,
            cache=True,
            dtstart=datetime(1934, 1, 1, tzinfo=pytz.utc),
            until=datetime(1954, 1, 1, tzinfo=pytz.utc)
        )
        non_trading_rules.append(veterans_day_saturday)

        thanksgiving = rrule.rrule(
            rrule.MONTHLY,
            bymonth=11,
            byweekday=rrule.TH,
            bysetpos=4,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(thanksgiving)

        christmas = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=25,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(christmas)

        christmas_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=26,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(christmas_sunday)

        # If Christmas is a Saturday then 24th, a Friday is observed.
        christmas_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=24,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(christmas_saturday)

        paper_crisis = rrule.rrule(
            rrule.WEEKLY,
            count=29,
            cache=True,
            dtstart=datetime(1968, 6, 12, tzinfo=pytz.utc)
        )
        non_trading_rules.append(paper_crisis)

        world_war_one = rrule.rrule(
            rrule.DAILY,
            count=96,
            byweekday=(rrule.MO, rrule.TU, rrule.WE, rrule.TH, rrule.FR),
            cache=True,
            dtstart=datetime(1914, 7, 31, tzinfo=pytz.utc)
        )
        non_trading_rules.append(world_war_one)

        non_trading_ruleset = rrule.rruleset()
        for rule in non_trading_rules:
            non_trading_ruleset.rrule(rule)

        non_trading_days = non_trading_ruleset.between(start, end, inc=True)
        hols = []
        for non_trading_day in non_trading_days:
            if non_trading_day.weekday() < 5:
                hols.append(str(non_trading_day.date()))

        hols.append('1901-09-19')  # PresidentialFuneral-WilliamMcKinley
        hols.append('1901-07-05')  # DayAfterIndependenceDay
        hols.append('1903-04-22')  # NewNYSEBuildingOpened
        hols.append('1917-06-05')  # DraftRegistrationDay
        hols.append('1918-01-28')  # HeatlessDay
        hols.append('1918-02-04')  # HeatlessDay
        hols.append('1918-02-11')  # HeatlessDay
        hols.append('1918-09-12')  # DraftRegistrationDay
        hols.append('1918-11-11')  # VeteransDay
        hols.append('1919-03-25')  # HomecomingOf27thDivision
        hols.append('1919-05-06')  # Parade-77thDivision
        hols.append('1919-09-10')  # ReturnOfGeneralJohnPershing
        hols.append('1921-11-11')  # VeteransDay
        hols.append('1923-08-03')  # PresidentialDeath-WarrenHarding
        hols.append('1923-08-10')  # PresidentialFuneral-WarrenHarding
        hols.append('1927-06-13')  # Parade-CharlesLindbergh
        hols.append('1929-11-01')  # ClericalBacklogRelief
        hols.append('1945-08-15')  # VictoryOverJapanDay
        hols.append('1945-08-16')  # VictoryOverJapanDay
        hols.append('1945-12-24')  # Christmas Eve
        hols.append('1950-12-12')  # Saturdayurday-Before-ChristmasEve
        hols.append('1956-12-24')  # Christmas Eve
        hols.append('1961-05-29')  # DayBeforeDecorationDay
        hols.append('1963-11-25')  # PresidentialFuneral-JohnKennedy
        hols.append('1968-02-12')  # LincolnsBirthday
        hols.append('1968-04-09')  # DayOfMourning-MartinLutherKing
        hols.append('1968-07-05')  # DayAfterIndependenceDay
        hols.append('1969-02-10')  # Weather-Snow
        hols.append('1969-03-31')  # PresidentialFuneral-DwightEisenhower
        hols.append('1969-07-21')  # FirstLunarLanding
        hols.append('1972-12-28')  # PresidentialFuneral-HarryTruman
        hols.append('1973-01-25')  # PresidentialFuneral-LyndonJohnson
        hols.append('1977-07-14')  # NewYorkCityBlackout
        hols.append('1985-09-27')  # Weather-HurricaneGloria
        hols.append('1994-04-27')  # PresidentialFuneral-RichardNixon
        hols.append('2001-09-11')  # September 11, 2001
        hols.append('2001-09-12')  # September 11, 2001
        hols.append('2001-09-13')  # September 11, 2001
        hols.append('2001-09-14')  # September 11, 2001
        hols.append('2004-06-11')  # Reagan's funeral
        hols.append('2007-01-02')  # Ford's funeral
        hols.append('2012-10-29')  # Frankenstorm
        hols.append('2012-10-30')  # Frankenstorm
        hols.append('2018-12-05')  # George H.W. Bush funeral
        hols.sort()
        this.cal = np.busdaycalendar(holidays=hols)
    return this.cal


def next_busday(dt):
    return str(pd.to_datetime(np.busday_offset(dt, 1, roll='backward',
                                               busdaycal=get_cal())).date())


def prev_busday(dt):
    return str(pd.to_datetime(np.busday_offset(dt, -1, roll='forward',
                                               busdaycal=get_cal())).date())


def is_busday(dt):
    return np.is_busday(dt, busdaycal=get_cal())


# Move backwards (if bdays < 0) or forwards (if bdays > 0) bdays
# business days.  If bdays is 0, then will not move if dt is already a
# business day, otherwise, will move to the previous business day. The
# parameter bdays can be 0 when we want to calculate the last business
# day when an option is traded, by moving 0 business days from the
# expiry, which would fall on a Saturday before Feb 2015, and on a
# Friday afterwards.
def move_busdays(dt, bdays):
    return str(np.busday_offset(dt, bdays, busdaycal=get_cal(),
                                roll='forward' if bdays < 0 else 'backward'))


def num_busdays(sdt, edt):
    return np.busday_count(sdt, edt, busdaycal=get_cal())


def expiry(dt):
    sat_fri_date = np.datetime64('2015-02')
    exp = np.busday_offset(dt, 2, roll='forward', weekmask='Fri')
    if np.datetime64(dt) < sat_fri_date:
        exp = np.busday_offset(exp, 1, weekmask='1111111')
    else:
        exp = move_busdays(str(exp), 0)
    return exp


# call it like this: expiries('2015-01', '2015-08')
def expiries(sdt, edt):
    return [expiry(dt) for dt in np.arange(sdt, edt, dtype='datetime64[M]')]


def next_expiry(dt, min_days=1):
    ym = np.datetime64(str(dt)[:-3])
    last_exp = move_busdays(expiry(ym), 0)
    while num_busdays(dt, last_exp) < min_days:
        ym += np.timedelta64(1, 'M')
        last_exp = move_busdays(expiry(ym), 0)
    return expiry(ym)


def prev_expiry(dt, min_days=0):
    ym = np.datetime64(str(dt)[:-3])
    last_exp = move_busdays(expiry(ym), 0)
    while num_busdays(last_exp, dt) < min_days:
        ym -= np.timedelta64(1, 'M')
        last_exp = move_busdays(expiry(ym), 0)
    return expiry(ym)


def long_expiries(starting_from=None):
    s_date = datetime.strftime(datetime.now(), '%Y-%m-%d')\
             if starting_from is None else starting_from
    e_date = next_expiry(s_date, 8 * 252 // 12)
    epoch = datetime.utcfromtimestamp(0)
    return [int((datetime.strptime(x, '%Y-%m-%d') - epoch).total_seconds())
            for x in expiries(s_date[:-3], e_date[:-3])]


def long_date(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((datetime.strptime(dt, '%Y-%m-%d') -
                epoch).total_seconds()) + 86400


def print_current_time():
    return str(datetime.now())


def get_seconds(dt):
    return int((datetime.strptime(dt, '%Y-%m-%d') - epoch).total_seconds())


def today_date():
    return datetime.strftime(datetime.now(), '%Y-%m-%d')


def current_date():
    return move_busdays(datetime.strftime(datetime.now(), '%Y-%m-%d'), 0)


def current_busdate(hr=20):
    crt_time = datetime.now()
    crt_date = crt_time.date()
    if crt_time.hour < hr:
        crt_date -= timedelta(days=1)
    return move_busdays(str(crt_date), 0)

def gen_cal(start_date='1984-12-31', end_date='2025-12-31'):
    busday_cal = get_cal(start_date, end_date)
    s_date = np.datetime64(start_date)
    e_date = np.datetime64(end_date)
    day_num = -1
    busday_num = -1
    while s_date <= e_date:
        day_num += 1
        ibd = -1
        if np.is_busday(s_date, busdaycal=busday_cal):
            busday_num += 1
            ibd = 1
        res = ibd * ((busday_num << 16) | day_num)
        sql_cmd = "INSERT INTO calendar VALUES ('{0:s}', {1:d})".format(
            str(s_date), res)
        stxdb.db_write_cmd(sql_cmd)
        if day_num % 1000 == 0:
            print('Inserted {0:s}'.format(str(s_date)))
        s_date += np.timedelta64(1, 'D')


if __name__ == '__main__':
    gen_cal()
