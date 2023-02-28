/**
 *  This file contains the functions that will be called from python,
 *  by the intraday analysis daemon.
 */
#define _XOPEN_SOURCE

#include "stx_lib.h"

ohlcv_record_ptr stx_get_ohlcv(char *stk, char *dt, int num_days,
                               bool intraday, bool realtime, int *num_recs) {
    LOGINFO("stx_get_ohlcv: dt = %s, num_days = %d, intraday = %d\n",
        dt, num_days, intraday);
    char end_date[20], *hhmm = NULL;
    // bool get_intraday_data = true;
    strcpy(end_date, dt);
    hhmm = strchr(end_date, ' ');
    if (hhmm != NULL)
        *hhmm++ = '\0';
    // if (!intraday &&
    //     (hhmm == NULL || !strcmp(hhmm, "16:00") || !strcmp(hhmm, "16:00:00")))
    //     get_intraday_data = false;
    if (!cal_is_busday(end_date)) {
        char *prev_bdate = NULL;
        cal_prev_bday(cal_ix(end_date), &prev_bdate);
        strcpy(end_date, prev_bdate);
        if (hhmm != NULL)
            sprintf(dt, "%s %s", end_date, hhmm);
        else
            strcpy(dt, end_date);
    }
    ht_item_ptr data_ht = intraday? ht_get(ht_id_data(), stk):
        ht_get(ht_data(), stk);
    stx_data_ptr data = NULL;
    if (data_ht == NULL) {
        LOGINFO("No data cached for %s\n", stk);
        data = ts_load_stk(stk, dt, intraday? NUM_ID_DAYS: NUM_EOD_DAYS,
                           intraday);
        if (data == NULL)
            return NULL;
        ts_set_day(data, dt, 0);
        data_ht = ht_new_data(stk, (void*)data);
        if (intraday)
            ht_insert(ht_id_data(), data_ht);
        else
            ht_insert(ht_data(), data_ht);
    } else {
        data = (stx_data_ptr) data_ht->val.data;
        char *current_dt = data->data[data->pos].date;
        if (strcmp(current_dt, end_date) != 0) {
            ts_free_data(data);
            data = ts_load_stk(stk, end_date, NUM_EOD_DAYS, false);
            if (data == NULL)
                return NULL;
            ts_set_day(data, end_date, 0);
            data_ht = ht_new_data(stk, (void*)data);
            ht_insert(ht_data(), data_ht);
        }
    }
    int start_ix = data->pos;
    if (intraday)
        start_ix -= (((data->pos + 1) % 78) + 78 * (num_days - 1));
    else
        start_ix -= (num_days + 1);
    if (start_ix < 0)
        start_ix = 0;
    *num_recs = data->pos - start_ix + 1;
    ohlcv_record_ptr res = (ohlcv_record_ptr)
        calloc((size_t) *num_recs, sizeof(ohlcv_record));
    memcpy(res, data->data + start_ix, *num_recs * sizeof(ohlcv_record));
    return res;
}

void stx_free_ohlcv(ohlcv_record_ptr *ohlcvs) {
    free(*ohlcvs);
    *ohlcvs = NULL;
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
    ohlcv_record_ptr res = stx_get_ohlcv(stk, ed, num_days, intraday,
                                         realtime, &num_recs);
    LOGINFO("num_recs = %d\n", num_recs);
    stx_free_ohlcv(&res);
    return 0;
}
