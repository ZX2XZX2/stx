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

void stx_get_ohlcv(char *stk, char *dt, int num_days, bool intraday,
                   bool realtime, ohlcv_record_ptr *res, int *num_recs) {
    stx_data_ptr data = ts_load_stk(stk, dt, num_days, intraday);
    ts_set_day(data, dt, -1);
    *num_recs = data->pos + 1;
    *res = data->data;
    // return data->data;
}

int main(int argc, char** argv) {
    char stk[16], ed[20];
    strcpy(stk, "TSLA");
    strcpy(ed, cal_current_trading_datetime());
    int num_days = 200;
    bool intraday = false, realtime = false;

    for (int ix = 1; ix < argc; ix++) {
        if (!strcmp(argv[ix], "-s") && (++ix < argc))
            strcpy(stk, argv[ix]);
        else if (!strcmp(argv[ix], "-e") && (++ix < argc))
            strcpy(ed, argv[ix]);
        else if (!strcmp(argv[ix], "-d") && (++ix < argc))
            num_days = atoi(argv[ix]);
        else if (!strcmp(argv[ix], "-i") || !strcmp(argv[ix], "--intraday") )
            intraday = true;
        else
            LOGWARN("Unknown option %s\n", argv[ix]);
    }
    if (intraday) {
        if (strlen(ed) == 10)
            strcat(ed, " 15:55:00");
    } else {
        char *hhmm = strchr(ed, ' ');
        if (hhmm != NULL)
            *hhmm = '\0';
    }
    LOGINFO("ed = %s\n", ed);
    int num_recs = 0;
    ohlcv_record_ptr res = NULL;
    stx_get_ohlcv(stk, ed, num_days, intraday, realtime, &res, &num_recs);
    LOGINFO("num_recs = %d\n", num_recs);
    return 0;
}
