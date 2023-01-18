/**
 *  This file contains the functions that will be called from python,
 *  by the intraday analysis daemon.
 */

#define _XOPEN_SOURCE
#include <stdio.h>
#include <stdint.h>
#include "stx_core.h"
#include "stx_ts.h"


stx_data_ptr stx_load_stk(char *stk, char *dt, int num_days, bool intraday) {
    return ts_load_stk(stk, dt, num_days, intraday);
}

ohlcv_record_ptr stx_get_ohlcv(char *stk, char *dt, int num_days,
                               bool intraday, bool realtime, int *num_recs) {
    stx_data_ptr data = ts_load_stk(stk, dt, num_days, intraday);
    ts_set_day(data, dt, -1);
    *num_recs = data->pos + 1;
    return data->data;
}
