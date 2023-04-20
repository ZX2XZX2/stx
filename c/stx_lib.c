/**
 *  This file contains the functions that will be called from python,
 *  by the intraday analysis daemon.
 */
#define _XOPEN_SOURCE

#include "stx_ana.h"
#include "stx_jl.h"
#include "stx_lib.h"

/**
 * @brief load stock data from DB into cache
 *
 * @param stk
 * @param dt
 * @param intraday
 * @return stx_data_ptr
 */
stx_data_ptr load_db_data_in_cache(char *stk, char *dt, bool intraday) {
    stx_data_ptr data = ts_load_stk(stk, dt,
        intraday? NUM_ID_DAYS: NUM_EOD_DAYS, intraday);
    if (data == NULL) {
        LOGERROR("Failed to load %s data for %s as of %s\n",
                 intraday? "intraday": "eod", stk, dt);
        return NULL;
    }
    ts_set_day(data, dt, 0);
    ht_item_ptr data_ht = ht_new_data(stk, (void*)data);
    ht_insert(intraday? ht_id_data(): ht_data(), data_ht);
    return data;
}

/**
 * @brief refresh cache data as of dt
 *
 * @param data_ht
 * @param dt
 * @return stx_data_ptr
 */
stx_data_ptr refresh_cache_data(ht_item_ptr data_ht, char *dt) {
    stx_data_ptr data = (stx_data_ptr) data_ht->val.data;
    char stk[16], data_dt[20], *data_hhmm = NULL;
    bool intraday = data->intraday;
    memset(data_dt, 0, 20 * sizeof(char));
    strcpy(data_dt, data->data[data->pos].date);
    memset(stk, 0, 16 * sizeof(char));
    strcpy(stk, data->stk);
    data_hhmm = strchr(data_dt, ' ');
    if (data_hhmm != NULL)
        *data_hhmm++ = '\0';
    if (strncmp(data_dt, dt, 10) != 0) {
        LOGINFO("Refreshing %s data for %s, from %s to %s\n",
                intraday? "intraday": "eod", stk, data_dt, dt);
        ts_free_data(data);
        data = NULL;
        data = load_db_data_in_cache(stk, dt, intraday);
    } else
        ts_set_day(data, dt, 0);
    return data;
}

void stx_get_stx_data_ptrs(char *stk, char *datetime, bool intraday,
                           stx_data_ptr *eod_data, stx_data_ptr *id_data) {
    /** Get intraday data.  This is always needed, even for EOD data, to
     *  display correctly last day, as of the time specified in input.
    */
    ht_item_ptr id_data_ht = ht_get(ht_id_data(), stk);
    if (id_data_ht == NULL) {
        LOGINFO("No intraday data cached for %s, get from DB\n", stk);
        *id_data = load_db_data_in_cache(stk, datetime, true);
        if (*id_data == NULL)
            return;
    } else {
        *id_data = refresh_cache_data(id_data_ht, datetime);
        if (*id_data == NULL)
            return;
    }
    /** Get eod data, if needed */
    if (!intraday) {
        ht_item_ptr eod_data_ht = ht_get(ht_data(), stk);
        if (eod_data_ht == NULL) {
            LOGINFO("No eod data cached for %s, get from DB\n", stk);
            *eod_data = load_db_data_in_cache(stk, datetime, false);
            if (*eod_data == NULL)
                return;
        } else {
            *eod_data = refresh_cache_data(eod_data_ht, datetime);
            if (*eod_data == NULL)
                return;
        }
        /** update last day with intraday data as of dt */
        ts_eod_intraday_update(*eod_data, *id_data);
    }
}

jl_data_ptr stx_get_jl_data_ptr(char *stk, char *dt, char *label, float factor,
                                bool intraday) {
    ht_item_ptr jl_ht = ht_get(intraday? ht_id_jl(label): ht_jl(label), stk);
    char *datetime = cal_datetime(dt);
    jl_data_ptr jl_recs = NULL;
    if (jl_ht == NULL) {
        stx_data_ptr id_data = NULL, eod_data = NULL;
        stx_get_stx_data_ptrs(stk, datetime, intraday, &eod_data, &id_data);
        stx_data_ptr data = intraday? id_data: eod_data;
        if (data == NULL) {
            LOGERROR("Could not load JL_%s for %s, skipping...\n", label, stk);
            return NULL;
        }
        jl_recs = jl_jl(data, datetime, factor);
        jl_ht = ht_new_data(stk, (void*)jl_recs);
        ht_insert(intraday? ht_id_jl(label): ht_jl(label), jl_ht);
    } else {
        jl_recs = (jl_data_ptr) jl_ht->val.data;
        jl_advance(jl_recs, datetime);
    }
    return jl_recs;
}

ohlcv_record_ptr stx_get_ohlcv(char *stk, char *dt, int num_days,
                               bool intraday, bool realtime, int *num_recs) {
    LOGINFO("stx_get_ohlcv: dt = %s, num_days = %d, intraday = %d\n",
        dt, num_days, intraday);
    char *datetime = cal_datetime(dt);
    stx_data_ptr id_data = NULL, eod_data = NULL;
    stx_get_stx_data_ptrs(stk, datetime, intraday, &eod_data, &id_data);
    /** extract the data needed for the chart display */
    stx_data_ptr data = intraday? id_data: eod_data;
    int start_ix = data->pos;
    if (intraday) {
        if (data->pos % 78 == 77)
            num_days++;
        start_ix -= (((data->pos + 1) % 78) + 78 * (num_days - 1) - 1);
    } else
        start_ix -= (num_days - 1);
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

jl_rec_ptr stx_jl_pivots(char *stk, char *dt, bool intraday, int *num_recs) {
    jl_data_ptr jl_recs = stx_get_jl_data_ptr(stk, dt, JL_100, JLF_100, intraday);
    if (jl_recs == NULL) {
        LOGERROR("Could not get jl_recs for %s, as of %s\n", stk, dt);
        return NULL;
    }
    jl_piv_ptr pivs = jl_get_pivots(jl_recs, 20);
    *num_recs = pivs->num;
    jl_rec_ptr pivot_list = (jl_rec_ptr) calloc((size_t)pivs->num,
                                                sizeof(jl_rec));
    for (int ix = 0; ix < pivs->num; ix++) {
        jl_pivot_ptr crs = pivs->pivots + ix;
        strcpy(pivot_list[ix].date, crs->date);
        pivot_list[ix].price = crs->price;
        pivot_list[ix].state = crs->state;
        pivot_list[ix].rg = crs->rg;
        pivot_list[ix].obv = crs->obv;
    }
    jl_free_pivots(pivs);
    for(int ix = 0; ix < *num_recs; ix++) {
        jl_rec_ptr x = pivot_list + ix;
        jl_print_rec(x->date, x->state, x->price, false, x->rg, x->obv);
    }
    return pivot_list;
}

void stx_free_jl_pivots(jl_rec_ptr *pivots) {
    free(*pivots);
    *pivots = NULL;
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
    if (strlen(ed) == 10)
        strcat(ed, " 15:55:00");
    LOGINFO("ed = %s\n", ed);
    int num_recs = 0;
    ohlcv_record_ptr res = stx_get_ohlcv(stk, ed, num_days, intraday,
                                         realtime, &num_recs);
    LOGINFO("num_recs = %d\n", num_recs);
    stx_free_ohlcv(&res);
    *(ed + 12) = '5';
    res = stx_get_ohlcv(stk, ed, num_days, intraday, realtime, &num_recs);
    LOGINFO("num_recs = %d\n", num_recs);

    int num_jl_recs = 0;
    jl_rec_ptr jl_res = stx_jl_pivots(stk, ed, intraday, &num_jl_recs);
    stx_free_ohlcv(&res);
    stx_free_jl_pivots(&jl_res);
    return 0;
}

char* stx_eod_analysis(char *dt) {
    int max_atm_price = 1, max_opt_spread = 1, min_stp_activity = 1000000000;
    int min_ind_activity = MIN_LDR_IND_ACT, max_stp_range = 1;
    bool download_spots = false, download_options = false, eod = true;
    ana_stx_analysis(dt, NULL, max_atm_price, max_opt_spread,
        min_ind_activity, min_stp_activity, max_stp_range, download_spots,
        download_options, eod);
}