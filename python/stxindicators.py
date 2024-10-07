import argparse
import ast
import logging
import polars as pl
from psycopg2 import sql
import stxcal
import stxdb

def indicator_filter(
    dt: str,
    filter_criteria: list,
    min_activity: int,
    min_close: int,
    min_range: int,
    min_pct_rg: int,
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
    df = df.filter(pl.col("rg_pct") >= min_pct_rg)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--date",
        type=str,
        default=stxcal.current_busdate(hr=9),
        help='Date to retrieve indicator leaders'
    )
    parser.add_argument(
        "-f", "--filter",
        type=ast.literal_eval,
        default=ast.literal_eval(
            "[('RS_252', 90), ('RS_45', 80), ('CS_45', 50)]"
        ),
        help="Indicator filtering criteria (positive: >=, negative: <=)",
    ),
    parser.add_argument("-p", "--min_pct_rg", type=int, default=200,
                        help="Minimum percentage range")
    parser.add_argument("-a", "--min_activity", type=int, default=1000,
                        help="Minimum activity for leaders")
    parser.add_argument("-c", "--min_close", type=int, default=1000,
                        help="Minimum close (in cents) for leaders")
    parser.add_argument("-r", "--min_range", type=int, default=30,
                        help="Minimum daily range (in cents) for leaders")
    args = parser.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    stxdb.db_write_cmd("DELETE FROM market_watch WHERE mkt='ind'")
    df = indicator_filter(
        args.date,
        args.filter,
        args.min_activity,
        args.min_close,
        args.min_range,
        args.min_pct_rg,
    )
    with pl.Config(tbl_rows= -1, tbl_cols=-1, fmt_str_lengths=10000):
        print(df)
    stxdb.db_write_cmd(f"UPDATE market_caches SET mkt_update_dt = '{args.date} 16:00:00' WHERE mkt_name='ind'")
    stxdb.db_write_cmd(f"UPDATE market_caches SET mkt_date = '{args.date}' WHERE mkt_name='ind'")
    stx = df["stk"].unique().to_list()
    for stk in stx:
        stxdb.db_write_cmd(f"INSERT INTO market_watch VALUES ('ind', '{stk}')")