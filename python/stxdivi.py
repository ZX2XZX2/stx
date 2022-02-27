import argparse
import logging
import os
from stxdf import StxDatafeed


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stock splits')
    parser.add_argument('-d', '--splitsdate', type=str, required=True,
                        help='splits analysis date in format yyyy-MM-dd')
    args = parser.parse_args()

    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    logging.info(f'stx_splits: splits analysis date {args.splitsdate}')
    df = StxDatafeed()
    df.upload_splits(args.splitsdate)
