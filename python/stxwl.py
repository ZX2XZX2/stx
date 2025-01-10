import argparse
import logging
from psycopg2 import sql
import stxcal
import stxdb


def add_to_watchlist(market: str, stock: str):
    """add stock 'stock' to watchlist of market 'market'
    :param market: name of the market to which we are adding the stock
    :param stock: ticker we are adding to the market watchlist
    """
    stxdb.db_write_cmd(
        f"INSERT INTO market_watch VALUES ('{market}', '{stock}')"
    )
    stxdb.db_write_cmd(
        f"INSERT INTO stx_wl_hist VALUES ('{market}', '{stock}',"
        " (SELECT NOW()), 1)"
    )


def remove_from_watchlist(market: str, stock: str):
    """remove stock 'stock' from watchlist of market 'market'
    :param market: name of the market from which we are removing the stock
    :param stock: ticker we are removing from the market watchlist
    """
    stxdb.db_write_cmd(
        f"DELETE FROM market_watch WHERE mkt='{market}' AND stk='{stock}'"
    )
    stxdb.db_write_cmd(
        f"INSERT INTO stx_wl_hist VALUES ('{market}', '{stock}',"
        " (SELECT NOW()), 0)"
    )


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    parser = argparse.ArgumentParser(description="add/rm stx to mkt watchlist")
    # Define a subparser for each choice (add or rm)
    subparsers = parser.add_subparsers(
        dest="choice",
        required=True,
        help="Choose add/remove to/from watchlist",
    )
    # Parser for add
    parser_add = subparsers.add_parser("add", help="add to watchlist")
    parser_add.add_argument(
        "-m", "--market",
        type=str,
        default='market',
        help="market for which we manage watchlist",
    )
    parser_add.add_argument(
        "-s", "--stk",
        type=str,
        help="stock added to watchlist",
    )
    # Parser for remove
    parser_rm = subparsers.add_parser("rm", help="remove from watchlist")
    parser_rm.add_argument(
        "-m", "--market",
        type=str,
        default='market',
        help="market for which we manage watchlist",
    )
    parser_rm.add_argument(
        "-s", "--stk",
        type=str,
        help="stock removed from watchlist",
    )

    # Parse arguments
    args = parser.parse_args()

    # Handle choices
    if args.choice == "rm":
        remove_from_watchlist(args.market, args.stk)
    else:  # args.choice == "add"
        add_to_watchlist(args.market, args.stk)


if __name__ == "__main__":
    main()
