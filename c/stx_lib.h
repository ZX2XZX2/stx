#ifndef __STX_LIB_H__
#define __STX_LIB_H__

#include <stdio.h>
#include <stdint.h>
#include "stx_core.h"
#include "stx_ts.h"

#define NUM_EOD_DAYS 300
#define NUM_ID_DAYS 20

/**
 *  This is candlev = OHLC candle + volume.  Dates are sent separately
 *  to python, as the index of the dataframe.
 */
typedef struct candlev_t {
    int open;
    int high;
    int low;
    int close;
    int volume;
} candlev, *candlev_ptr;

stx_data_ptr stx_load_stk(char *stk, char *dt, int num_days, bool intraday);

ohlcv_record_ptr stx_get_ohlcv(char *stk, char *dt, int num_days,
                               bool intraday, bool realtime, int *num_recs);
void stx_free_ohlcv(ohlcv_record_ptr *ohlcvs);
#endif
