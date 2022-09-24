#ifndef __STX_ANA_H__
#define __STX_ANA_H__

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include "stx_core.h"
#include "stx_indicators.h"
#include "stx_jl.h"
#include "stx_net.h"
#include "stx_setups.h"
#include "stx_ts.h"

#define AVG_DAYS 50
#define MIN_ACT 8
#define MIN_LDR_IND_ACT 100
#define MIN_LDR_STP_ACT 500
#define MAX_LDR_STP_RANGE 500
#define MIN_RCR 15
#define MAX_OPT_SPREAD 33
#define MAX_ATM_PRICE 1000
#define UP 'U'
#define DOWN 'D'
#define JL_FACTOR 2.00

#define UT2 2
#define UT1 1
#define SIDEWAYS 0
#define DT1 -1
#define DT2 -2

typedef struct ldr_t {
    int activity;
    int avg_range;
    int opt_spread;
    int atm_price;
    bool is_ldr;
} ldr, *ldr_ptr;


/**
 * Analyze option data to determine whether a stock is a leader or
 * not. To be a leader a stock must:
 * 1. Have at least 2 ATM/ITM calls and puts, and 2 ATM/OTM calls and puts.
 * 2. Have a non-negative average spread for the calls and puts found above.
 * 
 * The average spread and ATM price (an average between ATM call and
 * put) are stored for each leader.
 * 
 * Subsequently, leaders can be filtered, by choosing only those with
 * the average spread less than a threshold, and the ATM price less
 * than a max price.
 */
void ana_option_analysis(ldr_ptr leader, PGresult* sql_res, int spot) {
    leader->opt_spread = -1;
    leader->atm_price = -1;
    int itm_calls = 0, otm_calls = 0, itm_puts = 0, otm_puts = 0;
    int avg_spread = 0, bid, ask, strike, num_calls = 0, num_puts = 0;
    char* cp;
    bool call_atm = false, put_atm = false;
    int num = PQntuples(sql_res), num_spreads = 0, atm_price = 0;
    for(int ix = 0; ix < num; ix++) {
        cp = PQgetvalue(sql_res, ix, 0);
        strike = atoi(PQgetvalue(sql_res, ix, 1));
        if (!strcmp(cp, "c")) {
            num_calls++;
            if (strike < spot) itm_calls++;
            if (strike == spot) { itm_calls++; otm_calls++; call_atm = true; }
            if (strike > spot) otm_calls++;
        } else {
            num_puts++;
            if (strike < spot) otm_puts++;
            if (strike == spot) { itm_puts++; otm_puts++; put_atm = true; }
            if (strike > spot) itm_puts++;
        }
    }
    if ((itm_calls < 2) || (otm_calls < 2) || (itm_puts < 2) || (otm_puts < 2))
        return;
    for(int ix = itm_calls - 2; ix < itm_calls; ix++) {
        num_spreads++;
        int bid = atoi(PQgetvalue(sql_res, ix, 2));
        int ask = atoi(PQgetvalue(sql_res, ix, 3));
        avg_spread += (100 - 100 * bid / ask);
        if (ix == itm_calls - 1)
            atm_price += ask;
    }
    for(int ix = itm_calls; ix < itm_calls + (call_atm? 1: 2); ix++) {
        num_spreads++;
        int bid = atoi(PQgetvalue(sql_res, ix, 2));
        int ask = atoi(PQgetvalue(sql_res, ix, 3));
        avg_spread += (100 - 100 * bid / ask);
    }
    for(int ix = num_calls + otm_puts - 2; ix < num_calls + otm_puts; ix++) {
        num_spreads++;
        int bid = atoi(PQgetvalue(sql_res, ix, 2));
        int ask = atoi(PQgetvalue(sql_res, ix, 3));
        avg_spread += (100 - 100 * bid / ask);
        if (ix == num_calls + otm_puts - 1)
            atm_price += ask;
    }
    for(int ix = num_calls + otm_puts; 
        ix < num_calls + otm_puts + (put_atm? 1: 2); ix++) {
        num_spreads++;
        int bid = atoi(PQgetvalue(sql_res, ix, 2));
        int ask = atoi(PQgetvalue(sql_res, ix, 3));
        avg_spread += (100 - 100 * bid / ask);
    }
    leader->opt_spread = avg_spread / num_spreads;
    if (leader->opt_spread >= 0)
        leader->is_ldr = true;
    leader->atm_price = atm_price / 2;
}

/**
 *  This function returns the average option spread for stocks that
 *  are leaders, or -1, if the stock is not a leader.
 */
ldr_ptr ana_leader(stx_data_ptr data, char* as_of_date, char* exp, 
                   bool use_eod_spots, bool download_options) {
    /**
     *  A stock is a leader at a given date if:
     *  1. Its average activity is above a threshold.
     *  2. Its average range is above a threshold.
     *
     *  Disable option analysis in the leader analysis
     *  3. It has call and put options for that date, expiring in one month,
     *  4. For both calls and puts, it has at least 2 strikes >= spot, and
     *     2 strikes <= spot
     */
    ldr_ptr leader = (ldr_ptr) calloc((size_t)1, sizeof(ldr));
    leader->is_ldr = false;
    ts_set_day(data, as_of_date, 0);
    if (data->pos < AVG_DAYS - 1)
        return leader;
    int avg_act = 0, avg_rg = 0;
    for(int ix = data->pos - AVG_DAYS + 1; ix < data->pos; ix++) {
        avg_act += ((data->data[ix].close / 100) * 
                    (data->data[ix].volume / 100));
        avg_rg += ts_true_range(data, ix);
    }
    avg_act /= AVG_DAYS;
    avg_rg /= AVG_DAYS;
    leader->activity = avg_act;
    leader->avg_range = avg_rg;
    if ((avg_act < MIN_ACT) || (avg_rg < MIN_RCR))
        return leader;
    char und[16];
    strcpy(und, data->stk);
    char* dot = strchr(und, '.');
    if (dot != NULL) {
        if (('0' <= *(dot + 1)) && (*(dot + 1) <= '9'))
            *dot = '\0';
    }
    char sql_cmd[256];
    bool current_analysis = !strcmp(as_of_date, cal_current_busdate(5));
    if (use_eod_spots)
        sprintf(sql_cmd, "select c from eods where stk='%s' and dt='%s' "
                "and oi in (0, 2)", und, as_of_date);
    else
        sprintf(sql_cmd, "select spot from opt_spots where stk='%s' and "
                "dt='%s'", und, as_of_date);
    PGresult* res = db_query(sql_cmd);
    if (PQntuples(res) != 1) {
        PQclear(res);
        return leader;
    }
    int spot = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    if (download_options) {
        FILE *opt_fp = fopen("/tmp/options.csv", "w");
        if (opt_fp == NULL) {
            LOGERROR("Failed to open /tmp/options.csv file");
            opt_fp = stderr;
        } else {
            net_get_option_data(NULL, opt_fp, und, as_of_date, exp, 
                                cal_long_expiry(exp));
            fclose(opt_fp);
            db_upload_file("options", "/tmp/options.csv");
        }
    }
    /**
     *  This will bypass the option analysis in the decision whether
     *  stock is a leader or not.  But we stil analyze the options
     */
    leader->is_ldr = true;

    sprintf(sql_cmd, "select cp, strike, bid, ask from options where "
            "und='%s' and dt='%s' and expiry='%s' order by cp, strike",
            und, as_of_date, exp);
    res = db_query(sql_cmd);
    int num = PQntuples(res);
    if (num < 5) {
        PQclear(res);
        return leader;
    }
    ana_option_analysis(leader, res, spot);
    PQclear(res);
    return leader;
}

int ana_expiry_analysis(char* dt, bool use_eod_spots, bool download_spots,
                        bool download_options) {
    /**
     * special case when the date is an option expiry date
     * if the data is NULL, only run for the most recent business day
     * 1. wait until eoddata is downloaded. 
     **/
    LOGINFO("<begin>ana_expiry_analysis(%s)\n", dt);
    char sql_cmd[256], *exp;
    cal_expiry(cal_ix(dt) + 5, &exp);
    sprintf(sql_cmd, "select * from analyses where dt='%s' and "
            "analysis='leaders'", dt);
    PGresult *res = db_query(sql_cmd);
    int rows = PQntuples(res);
    PQclear(res);
    if (rows >= 1) {
        LOGINFO("Found %d leaders analyses for %s (expiry %s)\n", 
                rows, dt, exp);
        LOGINFO("Will skip leaders analyses for %s (expiry %s)\n", dt, exp);
        return 0;
    }
    char *sql_1 = "select distinct stk from eods where dt='";
    char *sql_2 = "' and stk not like '#%' and stk not like '^%' and "
        "oi in (0, 2) and (c/100)*(v/100)>100";
    sprintf(sql_cmd, "%s%s%s", sql_1, dt, sql_2);
    res = db_query(sql_cmd);
    rows = PQntuples(res);
    LOGINFO("loaded %5d stocks\n", rows);
    FILE* fp = NULL;
    char *filename = "/tmp/leaders.csv";
    if((fp = fopen(filename, "w")) == NULL) {
        LOGERROR("Failed to open file %s for writing\n", filename);
        return -1;
    }
    for (int ix = 0; ix < rows; ix++) {
        char stk[16];
        strcpy(stk, PQgetvalue(res, ix, 0));
        ht_item_ptr data_ht = ht_get(ht_data(), stk);
        stx_data_ptr data = NULL;
        if (data_ht == NULL) {
            data = ts_load_stk(stk);
            if (data == NULL)
                continue;
            data_ht = ht_new_data(stk, (void*)data);
            ht_insert(ht_data(), data_ht);
        } else
            data = (stx_data_ptr) data_ht->val.data;
        ldr_ptr leader = ana_leader(data, dt, exp, use_eod_spots,
                                    download_options);
        if (leader->is_ldr)
            fprintf(fp, "%s\t%s\t%d\t%d\t%d\t%d\n", exp, stk, leader->activity,
                    leader->avg_range, leader->opt_spread,
                    leader->atm_price);
        free(leader);
        if (ix % 100 == 0)
            LOGINFO("%s: analyzed %5d/%5d stocks\n", dt, ix, rows);
    }
    fclose(fp);
    LOGINFO("%s: analyzed %5d/%5d stocks\n", dt, rows, rows);
    PQclear(res);

    char* remove_tmp_ldrs = "DROP TABLE IF EXISTS tmp_leaders";
    char* create_tmp_ldrs = "CREATE TEMPORARY TABLE tmp_leaders( " \
        "expiry DATE NOT NULL,"                                    \
        "stk VARCHAR(16) NOT NULL, "                               \
        "activity INTEGER, "                                       \
        "range INTEGER, "                                          \
        "opt_spread INTEGER, "                                     \
        "atm_price INTEGER, "                                      \
        "PRIMARY KEY(expiry, stk))";
    char* copy_csv_ldrs = "COPY tmp_leaders("                       \
        "expiry, stk, activity, range, opt_spread, atm_price"       \
        ") FROM '/tmp/leaders.csv'";
    char* upsert_sql = "INSERT INTO leaders ("                      \
        "expiry, stk, activity, range, opt_spread, atm_price"        \
        ") SELECT * FROM tmp_leaders ON CONFLICT ON CONSTRAINT "     \
        "leaders_pkey DO UPDATE SET range=EXCLUDED.range";
    bool result = db_upsert_from_file(remove_tmp_ldrs, create_tmp_ldrs,
                                      copy_csv_ldrs, upsert_sql);
    LOGINFO("%s uploading %d leaders in the DB for expiry %s\n",
            result? "Success": "Failed", rows, exp);
    if (rows > 0) {
        memset(sql_cmd, 0, 256 * sizeof(char));
        sprintf(sql_cmd, "INSERT INTO analyses VALUES ('%s', 'leaders')", dt);
        db_transaction(sql_cmd);
    }
    LOGINFO("<end>ana_expiry_analysis(%s)\n", dt);
    return 0;
}

cJSON* ana_get_leaders(char* exp, int max_atm_price, int max_opt_spread,
                       int min_act, int max_range, int max_num_ldrs) {
    cJSON *leader_list = cJSON_CreateArray();
    if (leader_list == NULL) {
        LOGERROR("Failed to create leader_list cJSON Array.\n");
        return NULL;
    }
    char sql_0[64], sql_atm_px[64], sql_spread[64], sql_exclude[64],
        sql_activity[64], sql_range[64], sql_limit[64], sql_cmd[512];
    memset(sql_0, 0, 64);
    memset(sql_atm_px, 0, 64);
    memset(sql_spread, 0, 64);
    memset(sql_activity, 0, 64);
    memset(sql_range, 0, 64);
    memset(sql_exclude, 0, 64);
    memset(sql_limit, 0, 64);
    memset(sql_cmd, 0, 512);
    sprintf(sql_0, "select stk from leaders where expiry='%s'", exp);
    if (max_atm_price > 0)
        sprintf(sql_atm_px, " and atm_price <= %u and atm_price > 0",
                (unsigned short) max_atm_price);
    if (max_opt_spread > 0)
        sprintf(sql_spread, " and opt_spread <= %u and opt_spread > 0",
                (unsigned short) max_opt_spread);
    sprintf(sql_exclude, "and stk not in (select * from excludes)");
    if (min_act > 0)
        sprintf(sql_activity, " and activity >= %u", (unsigned short) min_act);
    if (max_range > 0)
        sprintf(sql_range, " and range <= %u", (unsigned short) max_range);
    if (max_num_ldrs > 0)
        sprintf(sql_limit, " order by opt_spread limit %u",
                (unsigned short) max_num_ldrs);
    sprintf(sql_cmd, "%s%s%s%s%s%s%s", sql_0, sql_atm_px, sql_spread,
            sql_exclude, sql_activity, sql_range, sql_limit);
    LOGINFO("ana_get_leaders():\n  sql_cmd %s\n", sql_cmd);
    PGresult *res = db_query(sql_cmd);
    int rows = PQntuples(res);
    LOGINFO("  returned %d leaders\n", rows);
    cJSON *ldr_name = NULL;
    char* stk = NULL;
    for (int ix = 0; ix < rows; ix++) {
        stk = PQgetvalue(res, ix, 0);
        ldr_name = cJSON_CreateString(stk);
        if (ldr_name == NULL) {
            LOGERROR("Failed to create cJSON string for %s\n", stk);
            continue;
        }
        cJSON_AddItemToArray(leader_list, ldr_name);
    }
    PQclear(res);
    LOGINFO("Generated JSON Array with leaders\n");
    return leader_list;
}

cJSON* ana_get_leaders_asof(char* dt, int max_atm_price, int max_opt_spread,
                            int min_act, int max_range, int max_num_ldrs) {
    char *exp_date;
    int ana_ix = cal_ix(dt), exp_ix = cal_expiry(ana_ix, &exp_date);
    return ana_get_leaders(exp_date, max_atm_price, max_opt_spread, min_act,
                           max_range, max_num_ldrs);
}

/**
 *  Return the trend for a stock as of date dt, UT1 or UT2 if stk in
 *  an uptrend, DT1 or DT2 if stk in down trend, SIDEWAYS if stk not
 *  in trend. Stock is in UT2 if JL_200 was last in uptrend, in UT1 if
 *  JL_200 was last in natural rally, and JL_100 was in uptrend.
 *  Stock is in DT2 if JL_200 was last in downtrend, in DT1 if JL_200
 *  was last in natural reaction and JL_100 was in downtrend.
 */
int ana_trend(char* stk, char* dt) {
    jl_data_ptr jl_recs_200 = jl_get_jl(stk, dt, JL_200, JLF_200);
    jl_data_ptr jl_recs_100 = jl_get_jl(stk, dt, JL_100, JLF_100);
    if ((jl_recs_200 == NULL) || (jl_recs_100 == NULL)) {
        if (jl_recs_200 == NULL)
            LOGERROR("Failed to get JL(200) for %s as of %s", stk, dt);
        if (jl_recs_100 == NULL)
            LOGERROR("Failed to get JL(100) for %s as of %s", stk, dt);
        LOGERROR("Skipping ana_setups for %s as of %s", stk, dt);
        return SIDEWAYS;
    }
    if ((jl_recs_200->last->prim_state == UPTREND) &&
        (jl_recs_200->last->state == UPTREND))
        return UT2;
    if ((jl_recs_200->last->prim_state == RALLY) &&
        (jl_recs_100->last->prim_state == UPTREND))
        return UT1;
    if ((jl_recs_200->last->prim_state == DOWNTREND) &&
        (jl_recs_200->last->state == DOWNTREND))
        return DT2;
    if ((jl_recs_200->last->prim_state == REACTION) &&
        (jl_recs_100->last->prim_state == DOWNTREND))
        return DT1;
    return SIDEWAYS;
}

char* trend_to_string(int trend) {
    static char _retval[4];
    switch(trend) {
    case UT2:
        strcpy(_retval, "UT2");
        break;
    case UT1:
        strcpy(_retval, "UT1");
        break;
    case DT1:
        strcpy(_retval, "DT1");
        break;
    case DT2:
        strcpy(_retval, "DT2");
        break;
    default:
        strcpy(_retval, "NIL");
        break;
    }
    return _retval;
}

cJSON* trend_info(int trend) {
    if (trend == SIDEWAYS)
        return NULL;
    cJSON *info = cJSON_CreateObject();
    cJSON_AddStringToObject(info, "trend", trend_to_string(trend));
    return info;
}

void ana_triggered_setups(cJSON* setups, char* stk, char* dt, bool eod) {
    int trend = ana_trend(stk, dt);
    if (trend == SIDEWAYS)
        return;
    stx_data_ptr ts = ts_get_ts(stk, dt, 0);
    if ((ts == NULL) || (ts->pos == -1)) {
        LOGERROR("No data for %s as of %s, skipping\n", stk, dt);
        return;
    }
    daily_record_ptr dr = ts->data;
    int ix = ts->pos, trigrd = 1;
    bool res;
    if ((trend == UT1 || trend == UT2) && (dr[ix].high > dr[ix - 1].high)) {
        if (stp_jc_1234(dr, ix - 1, UP))
            stp_add_to_setups(setups, NULL, "JC_1234", 1, trend_info(trend),
                              true);
        if (stp_jc_5days(dr, ix - 1, UP))
            stp_add_to_setups(setups, NULL, "JC_5DAYS", 1, trend_info(trend),
                              true);
    }
    if ((trend == DT1 || trend == DT2) && (dr[ix].low < dr[ix - 1].low)) {
        if (stp_jc_1234(dr, ix - 1, DOWN))
            stp_add_to_setups(setups, NULL, "JC_1234", -1, trend_info(trend),
                              true);
        if (stp_jc_5days(dr, ix - 1, DOWN))
            stp_add_to_setups(setups, NULL, "JC_5DAYS", -1, trend_info(trend),
                              true);
    }
    char *setup_time = cal_setup_time(eod, false);
    stp_update_triggered_setups_in_database(setups, dt, stk, setup_time);
    cJSON_Delete(setups);
}

void ana_setups_tomorrow(cJSON* setups, char* stk, char* dt, char* next_dt) {
    int trend = ana_trend(stk, dt);
    if (trend == SIDEWAYS)
        return;
    stx_data_ptr ts = ts_get_ts(stk, dt, 0);
    if ((ts == NULL) || (ts->pos == -1)) {
        LOGERROR("No data for %s as of %s, skipping\n", stk, dt);
        return;
    }
    daily_record_ptr dr = ts->data;
    int ix = ts->pos, trigrd = 0;
    bool res;
    if (trend == UT1 || trend == UT2) {
        if (stp_jc_1234(dr, ix, UP))
            stp_add_to_setups(setups, NULL, "JC_1234", 1, trend_info(trend),
                              false);
        if (stp_jc_5days(dr, ix, UP))
            stp_add_to_setups(setups, NULL, "JC_5DAYS", 1, trend_info(trend),
                              false);
    }
    if (trend == DT1 || trend == DT2) {
        if (stp_jc_1234(dr, ix, DOWN))
            stp_add_to_setups(setups, NULL, "JC_1234", -1, trend_info(trend),
                              false);
        if (stp_jc_5days(dr, ix, DOWN))
            stp_add_to_setups(setups, NULL, "JC_5DAYS", -1, trend_info(trend),
                              false);
    }
    char *setup_time = cal_setup_time(true, true);
    stp_insert_setups_in_database(setups, next_dt, stk, setup_time);
    cJSON_Delete(setups);
}

int ana_clip(int value, int lb, int ub) {
    int res = value;
    if (res < lb)
        res = lb;
    if (res > ub)
        res = ub;
    return res;
}

/**
 *  This method calculates for a given stock, as of a certain date:
 *  - JL setups (breakouts, pullbacks, support/resistance),
 *  - Candlestick setups
 *  - Daily setups (strong closes, gaps, and reversal days)
 */
int ana_jl_setups(cJSON *setups, char* stk, char* dt, bool eod) {
    int res = 0;
    /**
     * Get, or calculate if not already there, JL records for 4
     * factors.
     */
    jl_data_ptr jl_050 = jl_get_jl(stk, dt, JL_050, JLF_050);
    jl_data_ptr jl_100 = jl_get_jl(stk, dt, JL_100, JLF_100);
    jl_data_ptr jl_150 = jl_get_jl(stk, dt, JL_150, JLF_150);
    jl_data_ptr jl_200 = jl_get_jl(stk, dt, JL_200, JLF_200);
    if ((jl_050 == NULL) || (jl_100 == NULL) || (jl_150 == NULL) ||
        (jl_200 == NULL))
        return -1;
    /**
     *  Check for breaks, for all the factors (0.5, 1.0, 1.5, 2.0)
     */
    stp_jl_breaks(setups, jl_050);
    stp_jl_breaks(setups, jl_100);
    stp_jl_breaks(setups, jl_150);
    stp_jl_breaks(setups, jl_200);
    /**
     *  Check for support resistance
     */
    stp_jl_support_resistance(setups, jl_050);
    stp_jl_support_resistance(setups, jl_100);
    /**
     *  Check for pullbacks bouncing from a longer-term channel
     */
    stp_jl_pullbacks(setups, jl_050, jl_100, jl_150, jl_200);
    /* if (eod) { */
    /*     stp_candlesticks(setups, jl_050); */
    /*     stp_daily_setups(setups, jl_050); */
    /* } */
    /**
     *  Insert in the database all the calculated setups
     */
    char *setup_time = cal_setup_time(eod, false);
    stp_insert_setups_in_database(setups, dt, stk, setup_time);
    cJSON_Delete(setups);
    return 0;
}

/**
 *  Get ohlc quotes for spot leaders, and option quotes for option
 *  leaders.  If opt_leaders is NULL, this will only retrieve OHLC
 *  quotes for ohlc_leaders.
*/
void get_quotes(cJSON *ohlc_leaders, cJSON *opt_leaders, char *dt,
                char *exp_date, char *exp_date2, bool eod) {
    /**
     *  First get the OHLC quotes
     */
    char *ohlc_filename = "/tmp/ohlc.csv";
    cJSON *ldr;
    int num = 0, total = cJSON_GetArraySize(ohlc_leaders);
    FILE *fp = NULL;
    if ((fp = fopen(ohlc_filename, "w")) == NULL) {
        LOGERROR("Failed to open file %s for writing\n", ohlc_filename);
        return;
    }
    curl_global_init(CURL_GLOBAL_ALL);
    cJSON_ArrayForEach(ldr, ohlc_leaders) {
        if (cJSON_IsString(ldr) && (ldr->valuestring != NULL))
            net_get_eod_data(fp, ldr->valuestring, dt);
        num++;
        if (num % 100 == 0)
            LOGINFO("%s: got OHLC quote for %4d / %4d leaders\n", dt, num,
                    total);
    }
    LOGINFO("%s: got OHLC quote for %4d / %4d leaders\n", dt, num, total);
    fclose(fp);
    fp = NULL;
    char sql_cmd[256];
    sprintf(sql_cmd, "DELETE FROM eods WHERE dt='%s' AND oi=1 AND stk NOT IN "
            "('^GSPC', '^IXIC', '^DJI')", dt);
    db_transaction(sql_cmd);
    db_upload_file("eods", ohlc_filename);
    /**
     *  Get the index OHLC quotes.  Mark them as final if this is an
     *  EOD run.
     */
    LOGINFO("%s: Getting the quotes for the indexes: \n", dt);
    net_get_eod_data(NULL, "^GSPC", dt);
    LOGINFO("S&P500 \n");
    net_get_eod_data(NULL, "^IXIC", dt);
    LOGINFO("Nasdaq \n");
    net_get_eod_data(NULL, "^DJI", dt);
    LOGINFO("Dow Jones\n");
    LOGINFO("%s: Got the quotes for the indexes\n", dt);
    if (eod == true) {
        sprintf(sql_cmd, "UPDATE eods SET oi=0 WHERE stk IN "
                "('^GSPC', '^IXIC', '^DJI') AND dt='%s'", dt);
        db_transaction(sql_cmd);
    }
    /**
     *  If opt_leaders is null, not retrieving option quotes.  Return
     */
    if (opt_leaders == NULL)
        return;
    /**
     *  Get the options quotes for opt_leaders for the next two
     *  expiries
     */
    char *opt_filename = "/tmp/options.csv";
    num = 0, total = cJSON_GetArraySize(opt_leaders);
    cJSON_ArrayForEach(ldr, opt_leaders) {
        if (cJSON_IsString(ldr) && (ldr->valuestring != NULL)) {
            FILE *opt_fp = fopen(opt_filename, "w");
            if (opt_fp == NULL)
                LOGERROR("Failed to open %s file", opt_filename);
            else {
                net_get_option_data(NULL, opt_fp, ldr->valuestring, dt, 
                                    exp_date, cal_long_expiry(exp_date));
                fclose(opt_fp);
                db_upload_file("options", opt_filename);
            }
            opt_fp = fopen(opt_filename, "w");
            if (opt_fp == NULL)
                LOGERROR("Failed to open %s file", opt_filename);
            else {
                net_get_option_data(NULL, opt_fp, ldr->valuestring, dt, 
                                    exp_date2, cal_long_expiry(exp_date2));
                fclose(opt_fp);
                db_upload_file("options", opt_filename);
            }
        }
        num++;
        if (num % 100 == 0)
            LOGINFO("%s: got option quote for %4d / %4d leaders\n", dt, num,
                    total);
    }
    LOGINFO("%s: got option quote for %4d / %4d leaders\n", dt, num, total);

    curl_global_cleanup();
}

/**
 *  Get intraday data for a stock for a custom time interval, starting
 *  at startts and until now
 */
void ana_stk_intraday_data(FILE *id_fp, char *stk, unsigned long startts,
                           char *interval) {
    time_t endts = time(NULL);
    int num_recs;
#ifdef DEBUG_ID_QUOTE
    char parsed_date[20];
    struct tm *ts;
    ts = localtime(&startts);
    strftime(parsed_date, 20, "%Y-%m-%d %H:%M", ts);
    printf("startts = %s\n", parsed_date);
    ts = localtime(&endts);
    strftime(parsed_date, 20, "%Y-%m-%d %H:%M", ts);
    printf("endts = %s\n", parsed_date);
#endif
    id_ptr id_data = net_get_intraday_data(stk, startts, endts, interval,
                                           &num_recs);
    if (id_data != NULL) {
        char id_date[20];
        struct tm *ts;
        for (int ix = 0; ix < num_recs; ix++) {
            if (id_data[ix].volume == 0)
                continue;
            ts = localtime(&(id_data[ix].timestamp));
            strftime(id_date, 20, "%Y-%m-%d %H:%M", ts);
            /* fprintf(stderr, "%ld %d %d %d %d %d\n", id_data[ix].timestamp, */
            if (id_fp != NULL)
                fprintf(id_fp, "%s\t%s\t%d\t%d\t%d\t%d\t%d\t1\n", stk, id_date,
                        id_data[ix].open, id_data[ix].high, id_data[ix].low,
                        id_data[ix].close, id_data[ix].volume);
            else
                fprintf(stderr, "%s %d %d %d %d %d\n", id_date, id_data[ix].open,
                        id_data[ix].high, id_data[ix].low, id_data[ix].close,
                        id_data[ix].volume);
        }
        free(id_data);
        id_data = NULL;
    } else {
        LOGERROR("Failed to get %s intraday data for %s\n", interval, stk);
    }
}

/**
 *  Method to get intraday data in real-time.  For now, just print
 *  data on screen.
 */
void ana_intraday_data(char* stk_list) {
    char sql_cmd[1024];
    sprintf(sql_cmd, "SELECT stk, MAX(dt) FROM intraday WHERE stk IN "
            "(%s) group by stk", stk_list);
    PGresult *res = db_query(sql_cmd);
/* #ifdef DEBUG */
    /* LOGDEBUG("Found %d intraday last dates for %s\n", PQntuples(res), */
    /*          stk_list); */
/* #endif */
    FILE *fp = NULL;
    char *id_filename = "/tmp/id.csv";
    if ((fp = fopen(id_filename, "w")) == NULL) {
        LOGERROR("Failed to open file %s for writing\n", id_filename);
        return;
    }
    hashtable_ptr lastdate_ht = ht_strings(res);
    char* stk = strtok(stk_list, ",");
    while (stk != NULL) {
        *stk++ = '\0';
        *(stk + strlen(stk) - 1) = '\0';
        ht_item_ptr last_date = ht_get(lastdate_ht, stk);
        if (last_date == NULL) {
            LOGINFO("Could not find last date for %s\n", stk);
        } else {
            LOGINFO("Last date for %s is %s\n", stk, last_date->val.str);
            unsigned long startts = cal_tsfromdt(last_date->val.str);
            ana_stk_intraday_data(fp, stk, startts, "5m");
        }
        stk = strtok(NULL, ",");
    }
    PQclear(res);
    ht_free(lastdate_ht);
    fclose(fp);
    fp = NULL;

    char* remove_tmp_intraday = "DROP TABLE IF EXISTS tmp_intraday";
    char* create_tmp_intraday = "CREATE TEMPORARY TABLE tmp_intraday( " \
        "stk VARCHAR(16) NOT NULL, "                                    \
        "dt TIMESTAMP NOT NULL, "                                       \
        "o INTEGER NOT NULL, "                                          \
        "hi INTEGER NOT NULL, "                                         \
        "lo INTEGER NOT NULL, "                                         \
        "c INTEGER NOT NULL, "                                          \
        "v INTEGER, "                                                   \
        "oi INTEGER, "                                                  \
        "PRIMARY KEY(stk, dt))";
    char* copy_csv_intraday = "COPY tmp_intraday("                  \
        "stk, dt, o, hi, lo, c, v, oi"                              \
        ") FROM '/tmp/id.csv'";
    char* upsert_sql = "INSERT INTO intraday (stk, dt, o, hi, lo, c, v, oi) " \
        "SELECT stk, dt, o, hi, lo, c, v, oi "                          \
        "FROM tmp_intraday ON CONFLICT (stk, dt) DO "                   \
        "UPDATE SET o = EXCLUDED.o, hi = EXCLUDED.hi, "                 \
        "lo = EXCLUDED.lo, c = EXCLUDED.c, v = EXCLUDED.v, "            \
        "oi = EXCLUDED.oi";
    bool result = db_upsert_from_file(remove_tmp_intraday, create_tmp_intraday,
                                      copy_csv_intraday, upsert_sql);
    LOGINFO("%s uploading intraday data in the DB\n",
            result? "Success": "Failed");
}

/**
 *  Find out the business day from which to start setup analysis for a
 *  given stock.
 */
char* ana_get_setup_date(char *stk, char *ana_date) {
    char *setup_date = NULL, sql_cmd[256];
    sprintf(sql_cmd, "SELECT dt FROM setup_dates WHERE stk='%s'", stk);
    PGresult* res = db_query(sql_cmd);
    int rows = PQntuples(res);
    if (rows == 0) {
        /**
         *  If there is no last setup analysis date in the DB, find
         *  first date (d1) when EOD is available for the stock.  Then
         *  move back a year from the analysis date (d2).  Start
         *  analysis from the most recent date among d1 and d2.
         */
        PQclear(res);
        sprintf(sql_cmd, "SELECT min(dt) FROM eods WHERE stk='%s'", stk);
        res = db_query(sql_cmd);
        rows = PQntuples(res);
        if (rows == 0) {
            PQclear(res);
            LOGERROR("Could not find EOD data for stock %s, exit\n", stk);
            return NULL;
        }
        setup_date = PQgetvalue(res, 0, 0);
        cal_move_bdays(setup_date, 45, &setup_date);
        char *setup_date_1 = NULL;
        cal_move_bdays(ana_date, -252, &setup_date_1);
        if (strcmp(setup_date, setup_date_1) < 0)
            setup_date = setup_date_1;
    } else {
        /**
         *  Found last setup analysis date in DB. Start analysis from
         *  the next business day.
         */
        setup_date = PQgetvalue(res, 0, 0);
        cal_next_bday(cal_ix(setup_date), &setup_date);
    }
    PQclear(res);
    return setup_date;
}

/**
 * Calculate setups for a single stock (stk) up to ana_date. If
 * ana_date is NULL, setups (and their scores) will be calculated up
 * to the current business date.
 */
void ana_setups(char* stk, char* setup_date, char* next_date, bool eod) {
    char sql_cmd[256];
    /**
     *  JSON array for all non-triggerable setups.
     */
    cJSON *setups = cJSON_CreateArray();
    /**
     *  JSON array for all setups that might be triggered tomorrow.
     *  Need separate array for these because they need to be inserted
     *  under next business day.
     */
    cJSON *tomorrow_setups = cJSON_CreateArray();
    /**
     *  JSON array for all setups that were triggered today.  Need
     *  separate array for these because, when a setup is triggered,
     *  need to update the triggered flag AND the setup time.
     */
    cJSON *triggered_setups = cJSON_CreateArray();
    /**
     *  Run the setup analysis for setup_date 
     */
    ana_triggered_setups(triggered_setups, stk, setup_date, eod);
    if (eod == true)
        ana_setups_tomorrow(tomorrow_setups, stk, setup_date, next_date);
    ana_jl_setups(setups, stk, setup_date, eod);
    /**
     *  Update the setup_dates table with the last date when the
     *  analysis was run for the given stock
     */
    memset(sql_cmd, 0, 256 * sizeof(char));
    sprintf(sql_cmd, "INSERT INTO setup_dates VALUES ('%s', '%s') ON CONFLICT"
            " (stk) DO UPDATE SET dt='%s'", stk, setup_date, setup_date);
    db_transaction(sql_cmd);
}

/**
 * Separate implementation of daily analysis for the case when it is
 * running in intraday-expiry mode, when all it needs to do is
 * download the option prices
 */
void ana_intraday_expiry(char *ana_date, int max_atm_price,
                         int max_opt_spread) {
    char *exp_date, *exp_date2;
    int ana_ix = cal_ix(ana_date), exp_ix = cal_expiry(ana_ix, &exp_date);
    cal_expiry(exp_ix + 1, &exp_date2);
    LOGINFO("ana_intraday_expiry() upcoming expiries: %s, %s\n",
            exp_date, exp_date2);
    if (strcmp(ana_date, exp_date) != 0) {
        LOGINFO("%s not an expiry.  Skip option download\n", ana_date);
        return;
    }
    LOGINFO("%s is an expiry. Downloading options for expiries %s and %s\n",
            ana_date, exp_date, exp_date2);
    bool download_options = true;
    cJSON *ldrs = ana_get_leaders(exp_date, max_atm_price, max_opt_spread,
                                  -1, -1, 0);
    get_quotes(ldrs, ldrs, ana_date, exp_date, exp_date2, download_options);
    LOGINFO("Downloaded options for expiries %s, %s\n", exp_date, exp_date2);
}

void ana_indicators(cJSON *leaders, char *ana_date) {
    indicators_relative_strength(leaders, ana_date, 4);
    indicators_relative_strength(leaders, ana_date, 10);
    indicators_relative_strength(leaders, ana_date, 45);
    indicators_relative_strength(leaders, ana_date, 252);
    indicators_on_balance_volume(leaders, ana_date, 10);
    indicators_on_balance_volume(leaders, ana_date, 20);
    indicators_on_balance_volume(leaders, ana_date, 45);
    indicators_candle_strength(leaders, ana_date, 10);
    indicators_candle_strength(leaders, ana_date, 20);
    indicators_candle_strength(leaders, ana_date, 45);
}

cJSON* ana_get_id_leaders(cJSON *stp_leaders, char *ind_date, char *ind_name,
                          int short_ind_bound, int long_ind_bound) {
    cJSON *leader_list = cJSON_CreateArray();
    if (leader_list == NULL) {
        LOGERROR("Failed to create leader_list cJSON Array.\n");
        return NULL;
    }
    char sql[256];
    memset(sql, 0, 256);
    sprintf(sql, "SELECT ticker FROM indicators_1 WHERE dt='%s' AND name='%s'"
            " AND (bucket_rank <= %d OR bucket_rank >= %d) AND "
            "ticker NOT IN (SELECT * FROM excludes)",
            ind_date, ind_name, short_ind_bound, long_ind_bound);
    /* LOGINFO("ana_get_leaders():\n  sql_cmd %s\n", sql_cmd); */
    /* PGresult *res = db_query(sql_cmd); */
    /* int rows = PQntuples(res); */
    /* LOGINFO("  returned %d leaders\n", rows); */
    /* cJSON *ldr_name = NULL; */
    /* char* stk = NULL; */
    /* for (int ix = 0; ix < rows; ix++) { */
    /*     stk = PQgetvalue(res, ix, 0); */
    /*     ldr_name = cJSON_CreateString(stk); */
    /*     if (ldr_name == NULL) { */
    /*         LOGERROR("Failed to create cJSON string for %s\n", stk); */
    /*         continue; */
    /*     } */
    /*     cJSON_AddItemToArray(leader_list, ldr_name); */
    /* } */
    /* PQclear(res); */
    LOGINFO("Generated JSON Array with leaders\n");
    return leader_list;
}


/**
 * Main daily analysis method
 */
void ana_stx_analysis(char *ana_date, cJSON *stx, int max_atm_price,
                      int max_opt_spread, int min_ind_activity,
                      int min_stp_activity,  int max_stp_range,
                      bool download_spots, bool download_options, bool eod) {
    /**
     *  Get the next two expiries
     */
    char *exp_date, *exp_date2;
    int ana_ix = cal_ix(ana_date);
    int exp_ix = cal_expiry(ana_ix + (eod? 1: 0), &exp_date);
    cal_expiry(exp_ix + 1, &exp_date2);
    /**
     *  Get the list of stocks that will be analyzed.  Three lists of
     *  stocks: options (will retrieve option data at EOD), indicators
     *  (for indicator analysis at EOD), and setups (get spots
     *  intraday).  Setups is a subset of indicators.
     */
    cJSON *ldr = NULL, *stp_leaders = stx, *ind_leaders = stx,
        *opt_leaders = stx, *id_leaders = stx;
    if (ind_leaders == NULL)
        ind_leaders = ana_get_leaders(exp_date, -1, -1, min_ind_activity,
                                      -1, 0);
    if (stp_leaders == NULL)
        stp_leaders = ana_get_leaders(exp_date, -1, -1, min_stp_activity,
                                      max_stp_range, 0);
    if (eod && opt_leaders == NULL)
        opt_leaders = ana_get_leaders(exp_date, max_atm_price, max_opt_spread,
                                      -1, -1, 0);
    int num = 0, ind_total = cJSON_GetArraySize(ind_leaders),
        stp_total = cJSON_GetArraySize(stp_leaders),
        opt_total = (opt_leaders == NULL)? 0: cJSON_GetArraySize(opt_leaders);
    LOGINFO("ana_stx_analysis() will analyze %d indicator leaders, "
            "%d setup leaders, %d option leaders as of %s\n",
            ind_total, stp_total, opt_total, ana_date);
    LOGINFO("Upcoming expiries: %s and %s\n", exp_date, exp_date2);
    char sql_cmd[256];
    /**
     *  For real-time runs, update setup table.  Gaps and strong
     *  closes are only calculated at the end of the day.
     */
    if (download_spots) {
        LOGINFO("For real-time runs, update setup table.\n");
        LOGINFO("Downloading spots%s quotes\n",
                download_options? " and options": "");
        if (download_options)
            get_quotes(ind_leaders, opt_leaders, ana_date, exp_date,
                       exp_date2, download_options);
        else
            get_quotes(stp_leaders, NULL, ana_date, exp_date, exp_date2,
                       download_options);
    }
    LOGINFO("Running %s analysis for %s\n", eod? "eod": "intraday", ana_date);
    LOGINFO("Calculating setups for %d stocks\n", stp_total);
    /**
     *  Get the next business date for triggerable setups - these are
     *  setups that are only triggered next day if either the next
     *  high is greater than today high, of next low is less than
     *  today low
     */
    char* next_date = NULL;
    if (eod == true)
        cal_next_bday(cal_ix(ana_date), &next_date);
    cJSON_ArrayForEach(ldr, stp_leaders) {
        if (cJSON_IsString(ldr) && (ldr->valuestring != NULL))
            ana_setups(ldr->valuestring, ana_date, next_date, eod);
        num++;
        if (num % 100 == 0)
            LOGINFO("%s: analyzed %4d / %4d stocks\n", ana_date, num,
                    stp_total);
    }
    LOGINFO("%s: analyzed %4d / %4d stocks\n", ana_date, num, stp_total);
    if (eod == true) {
        LOGINFO("Calculating the indicators for %d stocks as of %s\n",
                ind_total, ana_date);
        ana_indicators(ind_leaders, ana_date);
    }
    char *ind_name = "CS_45", *ind_date = NULL;
    int short_ind_bound = 10, long_ind_bound = 90, dt_ix = cal_ix(ana_date);
    if (eod)
        ind_date = ana_date;
    else
        cal_prev_bday(dt_ix, &ind_date);
    if (id_leaders == NULL)
        id_leaders = ana_get_id_leaders(exp_date, ind_date, ind_name,
                                        short_ind_bound, long_ind_bound,
                                        min_stp_activity, max_stp_range);
    LOGINFO("Freeing the memory\n");
    if (stx == NULL) {
       cJSON_Delete(ind_leaders);
       cJSON_Delete(stp_leaders);
       cJSON_Delete(opt_leaders);
       cJSON_Delete(id_leaders);
    }
    LOGINFO("ana_stx_analysis(): done\n");
}

#endif
