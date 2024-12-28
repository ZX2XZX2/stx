import argparse
import ast
import logging
import polars as pl
from psycopg2 import sql
import stxcal
import stxdb

def generate_market(mkt_date: str, df: pl.DataFrame):
    stxdb.db_write_cmd(
        f"UPDATE market_caches SET "
        f"mkt_update_dt = '{mkt_date} 16:00:00', mkt_date = '{mkt_date}'"
        f" WHERE mkt_name='ind'"
    )
    stx = df["stk"].unique().to_list()
    for stk in stx:
        stxdb.db_write_cmd(f"INSERT INTO market_watch VALUES ('ind', '{stk}')")

def indicator_filter(
    dt: str,
    filter_criteria: list,
    min_activity: int,
    min_close: int,
    min_range: int,
    min_pct_rg: int,
    gen_market: bool,
) -> pl.DataFrame:
    ldr_dt = stxcal.next_expiry(dt)
    ig_dt = stxcal.prev_expiry(dt)
    first_indicator = filter_criteria[0][0]
    first_value = filter_criteria[0][1]
    q = sql.Composed([
        sql.SQL(
            "SELECT l.stk, l.activity, l.range, e.c, g.industry, "
            f"10000 * l.range/e.c AS rg_pct, i.bucket_rank AS {first_indicator}"
            " FROM leaders AS l, eods AS e, ind_groups AS g, indicators_1 AS i "
            "WHERE l.expiry="),
        sql.Literal(ldr_dt),
        sql.SQL(' AND l.activity>'),
        sql.Literal(min_activity),
        sql.SQL(" AND l.stk=e.stk AND e.dt="),
        sql.Literal(dt),
        sql.SQL(
            " AND l.stk NOT IN (SELECT * FROM excludes)"
            " AND l.stk IN (SELECT stk FROM ind_groups WHERE dt="
        ),
        sql.Literal(ig_dt),
        sql.SQL(")"),
        sql.SQL(" AND l.stk=g.stk AND l.stk=i.ticker AND i.dt=e.dt AND i.name="),
        sql.Literal(first_indicator),
        sql.SQL(" AND i.bucket_rank>=" if first_value >= 0 
                else " AND i.bucket_rank<="),
        sql.Literal(first_value),
        sql.SQL(" AND g.dt="),
        sql.Literal(ig_dt),
        sql.SQL(" AND e.c > "),
        sql.Literal(min_close),
        sql.SQL(" AND g.industry != 'N/A' AND range >"),
        sql.Literal(min_range),
        sql.SQL(" ORDER BY rg_pct DESC"),
    ])
    logging.debug(f"leader SQL = {q.as_string(stxdb.db_get_cnx())}")
    df = pl.read_database(q, stxdb.db_get_cnx())
    stk_list = df["stk"].unique().to_list()
    for i_name, i_value in filter_criteria[1:]:
        q = sql.Composed([
            sql.SQL(
                f"SELECT ticker AS stk, bucket_rank AS {i_name} FROM indicators_1 "
                "WHERE dt="
            ),
            sql.Literal(dt),
            sql.SQL(" AND ticker IN ("),
            sql.SQL(', ').join([sql.Literal(stk) for stk in stk_list]),
            sql.SQL(") AND name="),
            sql.Literal(i_name),
            sql.SQL(" AND bucket_rank>=" if i_value >= 0 else " AND bucket_rank<="),
            sql.Literal(i_value),
        ])
        dfi = pl.read_database(q, stxdb.db_get_cnx())
        df = df.join(dfi, on="stk", how="inner")
    # df = df.filter(pl.col("rg_pct") >= min_pct_rg)

    if gen_market:
        stxdb.db_write_cmd("DELETE FROM market_watch WHERE mkt='ind'")
    with pl.Config(tbl_rows= -1, tbl_cols=-1, fmt_str_lengths=10000):
        print(df)
    if gen_market:
        generate_market(dt, df)
    return df

def watchlist_analysis(
    market: str,
    dt: str,
    indicators: list,
    stats: list,
) -> pl.DataFrame:
    if not market:
        exp_date = stxcal.next_expiry(dt=dt)
        q = sql.Composed([
            sql.SQL(f"SELECT stk FROM leaders WHERE expiry ="),
            sql.Literal(exp_date),
        ])
    else:
        q = sql.Composed([
            sql.SQL(f"SELECT stk FROM market_watch WHERE mkt="),
            sql.Literal(market),
        ])
    logging.info(f"watchlist_analysis: market = {market}, dt = {dt}")
    df = pl.read_database(q, stxdb.db_get_cnx())
    stk_list = df["stk"].unique().to_list()
    for i_name in indicators:
        logging.debug(f"i_name = {i_name}")
        q = sql.Composed([
            sql.SQL(
                f"SELECT ticker AS stk, bucket_rank AS {i_name} FROM indicators_1 "
                "WHERE dt="
            ),
            sql.Literal(dt),
            sql.SQL(" AND ticker IN ("),
            sql.SQL(', ').join([sql.Literal(stk) for stk in stk_list]),
            sql.SQL(") AND name="),
            sql.Literal(i_name),
        ])
        logging.debug(f"SQL = {q.as_string(stxdb.db_get_cnx())}")
        dfi = pl.read_database(q, stxdb.db_get_cnx())
        df = df.join(dfi, on="stk", how="inner")
    df = df.with_columns((
        2 * pl.col("rs_252") + 1.5 * pl.col("rs_45") + pl.col("rs_10") +
        pl.col("rs_4") + 0.5 * pl.col("cs_45")
    ).alias("S"))
    df = df.sort(by=pl.col("S"), descending=True)
    if not market:
        df = df.slice(0, 12)
    with pl.Config(tbl_rows= -1, tbl_cols=-1, fmt_str_lengths=10000):
        print(df)
    return df

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    parser = argparse.ArgumentParser(description="Filter, watchlist or intraday analysis.")
    # Define a subparser for each choice
    subparsers = parser.add_subparsers(
        dest="choice",
        required=True,
        help="Choose (filter, watchlist, or intraday) analysis",
    )
    # Parser for filter
    parser_filter = subparsers.add_parser("filter", help="Filter analysis")
    parser_filter.add_argument(
        "-d", "--date",
        type=str,
        default=stxcal.current_busdate(hr=9),
        help="Date for the filter analysis"
    )
    parser_filter.add_argument(
        "-f", "--filter_criteria",
        type=ast.literal_eval,
        default=ast.literal_eval(
            "[('RS_252', 90), ('RS_45', 80), ('CS_45', 50)]"
        ),
        help="Indicator filtering criteria (positive: >=, negative: <=)",
    ),
    parser_filter.add_argument("-p", "--min_pct_rg", type=int, default=200,
                        help="Minimum percentage range")
    parser_filter.add_argument("-a", "--min_activity", type=int, default=1000,
                        help="Minimum activity for leaders")
    parser_filter.add_argument("-c", "--min_close", type=int, default=1000,
                        help="Minimum close (in cents) for leaders")
    parser_filter.add_argument("-r", "--min_range", type=int, default=30,
                        help="Minimum daily range (in cents) for leaders")
    parser_filter.add_argument('-m', '--gen_market', action='store_true',
                        help="Generate mkt view asof mkt date w/ filtered stx")

    # Parser for watchlist
    parser_watchlist = subparsers.add_parser("watchlist", help="Parser for watchlist")
    parser_watchlist.add_argument(
        "-m", "--market",
        type=str,
        default="market",
        help="Market for which we analyze watchlist",
    )
    parser_watchlist.add_argument(
        "-d", "--date",
        type=str,
        default=stxcal.current_busdate(hr=9),
        help='Date of the watchlist analysis'
    )
    parser_watchlist.add_argument(
        "-i", "--indicators",
        type=ast.literal_eval,
        default=ast.literal_eval("['RS_252', 'RS_45', 'RS_10', 'RS_4', 'CS_10', 'CS_20', 'CS_45']"),
        help="Indicators to show for each stock in the watchlist",
    )
    parser_watchlist.add_argument(
        "-s", "--stats",
        type=ast.literal_eval,
        default=ast.literal_eval("['activity', 'range', 'pct_range']"),
        help="Other stats (activity,range,volume,pct_range) for each stock in the watchlist",
    )

    # Parser for intraday
    parser_intraday = subparsers.add_parser("intraday", help="Parser for intraday")
    parser_intraday.add_argument(
        "-d", "--date",
        type=str,
        default=stxcal.current_busdate(hr=9),
        help="Date for the intraday analysis"
    )
    parser_intraday.add_argument(
        "-f", "--filter_criteria",
        type=ast.literal_eval,
        default=ast.literal_eval(
            "[('RS_252', 90), ('RS_45', 80), ('CS_45', 50)]"
        ),
        help="Indicator filtering criteria (positive: >=, negative: <=)",
    ),
    parser_intraday.add_argument("-p", "--min_pct_rg", type=int, default=200,
                        help="Minimum percentage range")
    parser_intraday.add_argument("-a", "--min_activity", type=int, default=1000,
                        help="Minimum activity for leaders")
    parser_intraday.add_argument("-c", "--min_close", type=int, default=1000,
                        help="Minimum close (in cents) for leaders")
    parser_intraday.add_argument("-r", "--min_range", type=int, default=30,
                        help="Minimum daily range (in cents) for leaders")
    parser_intraday.add_argument('-m', '--gen_market', action='store_true',
                        help="Generate mkt view asof mkt date w/ filtered stx")
    parser_intraday.add_argument("-N", "--num_minutes", type=int, default=30,
                        help="Calculate intraday leaders every N minutes")
    parser_intraday.add_argument("-n", "--num_stocks", type=int, default=30,
                        help="Return n intraday leaders every N minutes")

    # Parse arguments
    args = parser.parse_args()

    # Handle choices
    if args.choice == "filter":
        _ = indicator_filter(
            args.date,
            args.filter_criteria,
            args.min_activity,
            args.min_close,
            args.min_range,
            args.min_pct_rg,
            args.gen_market,
        )
    elif args.choice == "watchlist":
        _ = watchlist_analysis(
            args.market,
            args.date,
            args.indicators,
            args.stats,
        )
    elif args.choice == "intraday":
        logging.info("Intraday not implemented yet")


if __name__ == "__main__":
    main()
