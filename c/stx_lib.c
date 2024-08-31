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
#ifdef LDEBUG
    LOGDEBUG("Refresh %s cache data for %s\n",
             data->intraday? "Intraday": "EOD", data->stk);
#endif
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
#ifdef LDEBUG
        LOGDEBUG("Calling ts_set_day for %s dt = %s\n", stk, dt);
#endif
        ts_set_day(data, dt, 0);
#ifdef LDEBUG
        LOGDEBUG("After ts_set_day, active date is %s\n", data->data[data->pos].date);
#endif
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
        if (*id_data == NULL) {
            LOGWARN("after refresh, id_data is NULL\n");
            return;
        }
    }
    /** Get eod data, if needed */
    if (!intraday) {
        ht_item_ptr eod_data_ht = ht_get(ht_data(), stk);
        if (eod_data_ht == NULL) {
            LOGINFO("No eod data cached for %s, get from DB\n", stk);
            *eod_data = load_db_data_in_cache(stk, datetime, false);
            if (*eod_data == NULL) {
                LOGWARN("eod_data is NULL\n");
                return;
            }
        } else {
            *eod_data = refresh_cache_data(eod_data_ht, datetime);
            if (*eod_data == NULL) {
                LOGWARN("after refresh, eod_data is NULL\n");
                return;
            }
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
    if (*ohlcvs != NULL) {
        free(*ohlcvs);
        *ohlcvs = NULL;
    }
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

void stx_add_jl_rec(cJSON *jl_json_recs, char *jl_date, int jl_state,
                    int jl_price, bool jl_pivot, int jl_rg, int jl_piv_obv) {
    cJSON *jlrj = cJSON_CreateObject();
    cJSON_AddStringToObject(jlrj, "date", jl_date);
    cJSON_AddNumberToObject(jlrj, "state", (double)jl_state);
    cJSON_AddNumberToObject(jlrj, "price", (double)jl_price);
    cJSON_AddBoolToObject(jlrj, "pivot", jl_pivot);
    cJSON_AddNumberToObject(jlrj, "range", (double)jl_rg);
    cJSON_AddNumberToObject(jlrj, "obv", (double)jl_piv_obv);
    cJSON_AddItemToArray(jl_json_recs, jlrj);
}

char* stx_get_jl(char *stk, char *dt, bool print_pivots_only) {
    bool intraday = true;
    cJSON *jl_json_recs = cJSON_CreateArray();
    jl_data_ptr jld = stx_get_jl_data_ptr(stk, dt, JL_100, JLF_100, intraday);
    int last_piv = ts_find_date_record(jld->data, jld->pivots->date, 0);
    for(int ix = 0; ix <= jld->pos; ix++) {
        jl_record_ptr jlr = &(jld->recs[ix]);
        if (jlr->state == NONE)
            continue;
        if (ix < last_piv && !jlr->pivot && !jlr->pivot2 && print_pivots_only)
            continue;
        if (!print_pivots_only || jlr->pivot || (ix > last_piv))
            stx_add_jl_rec(jl_json_recs, jld->data->data[ix].date, jlr->state,
                           jlr->price, jlr->pivot, jlr->rg, jlr->piv_obv);
        if (jlr->state2 != NONE && (!print_pivots_only || jlr->pivot2 || 
                                    (ix > last_piv)))
            stx_add_jl_rec(jl_json_recs, jld->data->data[ix].date, jlr->state2,
                           jlr->price2, jlr->pivot2, jlr->rg, jlr->piv_obv2);
    }
    char *res = cJSON_Print(jl_json_recs);
    cJSON_Delete(jl_json_recs);
    return res;
}

void stx_free_jl_pivots(jl_rec_ptr *pivots) {
    if (*pivots != NULL) {
        free(*pivots);
        *pivots = NULL;
    }
}

cJSON* stx_indicator_analysis(char *dt, char *expiry, char *ind_name,
                              int min_activity, int up_limit,
                              int down_limit) {
    char sql_cmd[256];
    memset(sql_cmd, 0, 256 * sizeof(char));
    sprintf(sql_cmd, "SELECT ticker, bucket_rank FROM indicators_1 WHERE "
            "dt='%s' AND name='%s' AND ticker IN "
            "(SELECT stk FROM leaders WHERE expiry='%s' AND activity>=%d) "
            "ORDER BY rank DESC LIMIT %d", dt, ind_name, expiry, min_activity,
            down_limit);
    PGresult *res = db_query(sql_cmd);
    int num_recs = PQntuples(res);
    if(num_recs <= 0) {
        /**
         *  reuse ana_stx_analysis, call it with arguments that
         *  will only trigger indicator analysis
         */
        int max_atm_price = 1, max_opt_spread = 1, min_stp_activity = 1000000000;
        int min_ind_activity = MIN_LDR_IND_ACT, max_stp_range = 1;
        bool download_spots = false, download_options = false, eod = true;
        ana_stx_analysis(dt, NULL, max_atm_price, max_opt_spread,
            min_ind_activity, min_stp_activity, max_stp_range, download_spots,
            download_options, eod);
        PQclear(res);
    }
    res = db_query(sql_cmd);
    num_recs = PQntuples(res);
    if(num_recs <= 0) {
        LOGERROR("Could not get EOD analysis for %s, %d\n",
                    dt, min_activity);
        PQclear(res);
        return NULL;
    }
    cJSON *ind_res = cJSON_CreateObject();
    cJSON_AddStringToObject(ind_res, "name", ind_name);
    cJSON *ind_up = cJSON_AddArrayToObject(ind_res, "Up");
    cJSON *ind_down = cJSON_AddArrayToObject(ind_res, "Down");
    for(int ix = 0; ix < num_recs; ix++) {
        char *stk = PQgetvalue(res, ix, 0);
        int bucket_rank =  atoi(PQgetvalue(res, ix, 1));
        cJSON *stk_rec = cJSON_CreateObject();
        cJSON_AddStringToObject(stk_rec, "ticker", stk);
        cJSON_AddNumberToObject(stk_rec, "rank", (double)bucket_rank);
        cJSON_AddItemToArray(ind_up, stk_rec);
    }
    PQclear(res);
    res = NULL;
    memset(sql_cmd, 0, 256 * sizeof(char));
    sprintf(sql_cmd, "SELECT ticker, bucket_rank FROM indicators_1 WHERE "
            "dt='%s' AND name='%s' AND ticker IN "
            "(SELECT stk FROM leaders WHERE expiry='%s' AND activity>=%d) "
            "ORDER BY rank LIMIT %d", dt, ind_name, expiry, min_activity,
            up_limit);
    res = db_query(sql_cmd);
    num_recs = PQntuples(res);
    for(int ix = 0; ix < num_recs; ix++) {
        char *stk = PQgetvalue(res, ix, 0);
        int bucket_rank =  atoi(PQgetvalue(res, ix, 1));
        cJSON *stk_rec = cJSON_CreateObject();
        cJSON_AddStringToObject(stk_rec, "ticker", stk);
        cJSON_AddNumberToObject(stk_rec, "rank", (double)bucket_rank);
        cJSON_AddItemToArray(ind_down, stk_rec);
    }
    PQclear(res);
    res = NULL;
    return ind_res;
}

char* stx_eod_analysis(char *dt, char *ind_names, int min_activity,
                       int up_limit, int down_limit) {
    char *expiry = NULL, *ind_name = NULL;
    cJSON *mkt = cJSON_CreateObject();
    cJSON *ind_list = cJSON_AddArrayToObject(mkt, "indicators");
    LOGINFO("stx_eod_analysis: dt = %s, ind_names = %s, min_activity = %d"
            "up_limit = %d, down_limit = %d\n", dt, ind_names, min_activity,
            up_limit, down_limit);
    cal_expiry(cal_ix(dt), &expiry);
    ind_name = strtok(ind_names, ",");
    while (ind_name) {
        cJSON* ind_data = stx_indicator_analysis(dt, expiry, ind_name,
                                                 min_activity, up_limit,
                                                 down_limit);
        if (ind_data != NULL)
            cJSON_AddItemToArray(ind_list, ind_data);
        ind_name = strtok(NULL, ",");
    }
    char *res = cJSON_Print(mkt);
    cJSON_Delete(mkt);
    return res;
}

void stx_free_text(char *text) {
    if (text != NULL) {
        free(text);
        text = NULL;
    }
}

char* stx_get_trade_input(char *stk, char *dt) {
    LOGINFO("Entered stx_get_trade_input()\n");
    cJSON *trade_input = cJSON_CreateObject();
    LOGINFO("Created trade_input cJSON object\n");
    stx_data_ptr id_data = NULL, eod_data = NULL;
    bool intraday = false, jl_intraday = false;
    LOGINFO("Before stx_get_stx_data_ptrs()\n");
    stx_get_stx_data_ptrs(stk, dt, intraday, &eod_data, &id_data);
    LOGINFO("After stx_get_stx_data_ptrs()\n");
    LOGINFO("After stx_get_stx_data_ptrs() %d EOD and %d ID records\n", eod_data->num_recs, id_data->num_recs);
    // jl_data_ptr jl_recs = stx_get_jl_data_ptr(stk, dt, JL_100, JLF_100,
    //     jl_intraday);
    // LOGINFO("After stx_get_jl_data_ptr()\n");
    // LOGINFO("After stx_get_jl_data_ptr() %d JL records\n", jl_recs->size);
    LOGINFO("Before ts_find_date_record() dt = %s\n", dt);
    int ix = ts_find_date_record(id_data, dt, 0);
    LOGINFO("After ts_find_date_record() ix = %d\n", ix);
    cJSON_AddNumberToObject(trade_input, "current_price",
        (double)id_data->data[ix].close);
    LOGINFO("current_price = %d\n", id_data->data[ix].close);
    int avg_vol = 1, avg_rg = 1;
    // int avg_vol = jl_get_avg_volume(jl_recs);
    // LOGINFO("avg_vol = %d\n", avg_vol);
    // int avg_rg = jl_get_avg_range(jl_recs);
    // LOGINFO("avg_rg = %d\n", avg_rg);
    cJSON_AddNumberToObject(trade_input, "avg_volume", (double)avg_vol);
    cJSON_AddNumberToObject(trade_input, "avg_range", (double)avg_rg);
    char *res = cJSON_Print(trade_input);
    LOGINFO("res = %s\n", res);
    cJSON_Delete(trade_input);
    LOGINFO("After deleting trade_input, res = %s\n", res);
    return res;
}

int main(int argc, char** argv) {
    char stk[16], ed[20], dt[20], indicator_names[20];
    strcpy(stk, "TSLA");
    strcpy(ed, cal_current_trading_datetime());
    strcpy(dt, "2024-07-03");
    strcpy(indicator_names, "CS_45,RS_45,RS_252");
    int min_activity = 1000, up_limit = 10, down_limit = 0;

    char *eod_json = stx_eod_analysis(dt, indicator_names, min_activity,
                                      up_limit, down_limit);
    if (eod_json != NULL)
        LOGINFO("res_json = \n%s\n", eod_json);
    stx_free_text(eod_json);
    return 0;

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
    char ind_names[64];
    memset(ind_names, 0, 64 * sizeof(char));
    strcpy(ind_names, "CS_45,OBV_45");
    char *res_json = stx_eod_analysis("2023-04-28", ind_names, 10000, 5, 5);
    if (res_json != NULL)
        LOGINFO("res_json = \n%s\n", res_json);
    stx_free_text(res_json);
    res_json = stx_get_trade_input(stk, ed);
    if (res_json != NULL)
        LOGINFO("res_json = \n%s\n", res_json);
    stx_free_text(res_json);
    res_json = stx_get_jl("AMD", "2023-06-06 14:00:00", true);
    if (res_json != NULL)
        LOGINFO("res_json = \n%s\n", res_json);
    stx_free_text(res_json);
    res_json = stx_get_trade_input("AVGO", "2023-06-06 15:55:00");
    if (res_json != NULL)
        LOGINFO("res_json = \n%s\n", res_json);
    stx_free_text(res_json);
    res_json = stx_get_trade_input("AVGO", "2023-06-06 15:55:00");
    if (res_json != NULL)
        LOGINFO("res_json = \n%s\n", res_json);
    stx_free_text(res_json);

    return 0;
}