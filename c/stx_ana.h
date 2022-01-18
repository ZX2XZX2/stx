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
#define MIN_RCR 15
#define MAX_OPT_SPREAD 33
#define MAX_ATM_PRICE 500
#define UP 'U'
#define DOWN 'D'
#define JL_FACTOR 2.00
#define JLF_050 0.50
#define JLF_100 1.0
#define JLF_150 1.50
#define JLF_200 2.00
#define JL_050 "050"
#define JL_100 "100"
#define JL_150 "150"
#define JL_200 "200"

typedef struct ldr_t {
    int activity;
    int range_ratio;
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
                   bool realtime_analysis, bool download_options) {
    /**
     *  A stock is a leader at a given date if:
     *  1. Its average activity is above a threshold.
     *  2. Its average range is above a threshold.
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
        avg_rg += (1000 * ts_true_range(data, ix) / data->data[ix].close);
    }
    avg_act /= AVG_DAYS;
    avg_rg /= AVG_DAYS;
    leader->activity = avg_act;
    leader->range_ratio = avg_rg;
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
    if (realtime_analysis) 
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

int ana_expiry_analysis(char* dt, bool realtime_analysis, bool download_spots,
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
        ldr_ptr leader = ana_leader(data, dt, exp, realtime_analysis,
                                    download_options);
        if (leader->is_ldr)
            fprintf(fp, "%s\t%s\t%d\t%d\t%d\t%d\n", exp, stk, leader->activity,
                    leader->range_ratio, leader->opt_spread, 
                    leader->atm_price);
        free(leader);
        if (ix % 100 == 0)
            LOGINFO("%s: analyzed %5d/%5d stocks\n", dt, ix, rows);
    }
    fclose(fp);
    LOGINFO("%s: analyzed %5d/%5d stocks\n", dt, rows, rows);
    PQclear(res);
    db_upload_file("leaders", filename);
    LOGINFO("%s: uploaded leaders in the database as of date %s\n", exp, dt);
    if (rows > 0) {
        memset(sql_cmd, 0, 256 * sizeof(char));
        sprintf(sql_cmd, "INSERT INTO analyses VALUES ('%s', 'leaders')", dt);
        db_transaction(sql_cmd);
    }
    LOGINFO("<end>ana_expiry_analysis(%s)\n", dt);
    return 0;
}

cJSON* ana_get_leaders(char* exp, int max_atm_price, int max_opt_spread,
                       int max_num_ldrs) {
    cJSON *leader_list = cJSON_CreateArray();
    if (leader_list == NULL) {
        LOGERROR("Failed to create leader_list cJSON Array.\n");
        return NULL;
    }
    char sql_0[64], sql_atm_px[64], sql_spread[64], sql_exclude[64],
        sql_limit[64], sql_cmd[512];
    memset(sql_0, 0, 64);
    memset(sql_atm_px, 0, 64);
    memset(sql_spread, 0, 64);
    memset(sql_exclude, 0, 64);
    memset(sql_limit, 0, 64);
    memset(sql_cmd, 0, 512);
    sprintf(sql_0, "select stk from leaders where expiry='%s'", exp);
    if (max_atm_price > 0)
        sprintf(sql_atm_px, "and atm_price <= %u",
                (unsigned short) max_atm_price);
    if (max_opt_spread > 0)
        sprintf(sql_spread, "and opt_spread <= %u",
                (unsigned short) max_opt_spread);
    sprintf(sql_exclude, "and stk not in (select * from excludes)");
    if (max_num_ldrs > 0)
        sprintf(sql_limit, "order by opt_spread limit %u",
                (unsigned short) max_num_ldrs);
    sprintf(sql_cmd, "%s%s%s%s%s", sql_0, sql_atm_px, sql_spread, sql_exclude,
            sql_limit);
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
                            int max_num_ldrs) {
    char *exp_date;
    int ana_ix = cal_ix(dt), exp_ix = cal_expiry(ana_ix, &exp_date);
    return ana_get_leaders(exp_date, max_atm_price, max_opt_spread,
                           max_num_ldrs);
}

void ana_pullbacks(FILE* fp, char* stk, char* dt, jl_data_ptr jl_recs) {
    daily_record_ptr dr = jl_recs->data->data;
    int ix = jl_recs->data->pos, trigrd = 1;
    bool res;
    if ((jl_recs->last->prim_state == UPTREND) && 
        (jl_recs->last->state == UPTREND) && (dr[ix].high > dr[ix - 1].high)) {
        if (stp_jc_1234(dr, ix - 1, UP))
            fprintf(fp, "%s\t%s\tJC_1234\t%c\t%d\n", dt, stk, UP, trigrd);
        if (stp_jc_5days(dr, ix - 1, UP))
            fprintf(fp, "%s\t%s\tJC_5DAYS\t%c\t%d\n", dt, stk, UP, trigrd);
    } else if ((jl_recs->last->prim_state == DOWNTREND) && 
               (jl_recs->last->state == DOWNTREND) && 
               (dr[ix].low < dr[ix - 1].low)) {
        if (stp_jc_1234(dr, ix - 1, DOWN))
            fprintf(fp, "%s\t%s\tJC_1234\t%c\t%d\n", dt, stk, DOWN, trigrd);
        if (stp_jc_5days(dr, ix - 1, DOWN))
            fprintf(fp, "%s\t%s\tJC_5DAYS\t%c\t%d\n", dt, stk, DOWN, trigrd);
    }
}

void ana_setups_tomorrow(FILE* fp, char* stk, char* dt, char* next_dt,
                         jl_data_ptr jl_recs) {
    daily_record_ptr dr = jl_recs->data->data;
    int ix = jl_recs->data->pos, trigrd = 0;
    bool res;
    if ((jl_recs->last->prim_state == UPTREND) && 
        (jl_recs->last->state == UPTREND)) {
        if (stp_jc_1234(dr, ix, UP))
            fprintf(fp, "%s\t%s\tJC_1234\t%c\t0\n", next_dt, stk, UP);
        if (stp_jc_5days(dr, ix, UP))
            fprintf(fp, "%s\t%s\tJC_5DAYS\t%c\t0\n", next_dt, stk, UP);
    } else if ((jl_recs->last->prim_state == DOWNTREND) && 
               (jl_recs->last->state == DOWNTREND)) {
        if (stp_jc_1234(dr, ix, DOWN))
            fprintf(fp, "%s\t%s\tJC_1234\t%c\t0\n", next_dt, stk, DOWN);
        if (stp_jc_5days(dr, ix, DOWN))
            fprintf(fp, "%s\t%s\tJC_5DAYS\t%c\t0\n", next_dt, stk, DOWN);
    }
}

void ana_setups(FILE* fp, char* stk, char* dt, char* next_dt, bool eod) {
    ht_item_ptr jl_ht = ht_get(ht_jl(JL_200), stk);
    jl_data_ptr jl_recs = NULL;
    if (jl_ht == NULL) {
        stx_data_ptr data = ts_load_stk(stk);
        if (data == NULL) {
            LOGERROR("Could not load %s, skipping...\n", stk);
            return;
        }
        jl_recs = jl_jl(data, dt, JL_FACTOR);
        jl_ht = ht_new_data(stk, (void*)jl_recs);
        ht_insert(ht_jl(JL_200), jl_ht);
    } else {
        jl_recs = (jl_data_ptr) jl_ht->val.data;
        jl_advance(jl_recs, dt);
    }
    ana_pullbacks(fp, stk, dt, jl_recs);
    if (eod == true)
        ana_setups_tomorrow(fp, stk, dt, next_dt, jl_recs);
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
    if (eod) {
        stp_candlesticks(setups, jl_050);
        stp_daily_setups(setups, jl_050);
    }
    /**
     *  Insert in the database  all the calculated setups
     */
    stp_insert_setups_in_database(setups, dt, stk);
    cJSON_Delete(setups);
    return 0;
}

void get_quotes(cJSON *leaders, char *dt, char *exp_date, char *exp_date2,
                bool eod) {
    char *filename = "/tmp/intraday.csv", *opt_filename = "/tmp/options.csv";
    cJSON *ldr;
    int num = 0, total = cJSON_GetArraySize(leaders);
    FILE *fp = NULL;
    if ((fp = fopen(filename, "w")) == NULL) {
        LOGERROR("Failed to open file %s for writing\n", filename);
        return;
    }
    curl_global_init(CURL_GLOBAL_ALL);
    cJSON_ArrayForEach(ldr, leaders) {
        if (cJSON_IsString(ldr) && (ldr->valuestring != NULL)) {
            net_get_eod_data(fp, ldr->valuestring, dt);
            if (eod == true) {
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
        }
        num++;
        if (num % 100 == 0)
            LOGINFO("%s: got quote for %4d / %4d leaders\n", dt, num, total);
    }
    LOGINFO("%s: got quote for %4d / %4d leaders\n", dt, num, total);
    fclose(fp);
    fp = NULL;
    char sql_cmd[256];
    sprintf(sql_cmd, "DELETE FROM eods WHERE dt='%s' AND oi=1 AND stk NOT IN "
            "('^GSPC', '^IXIC', '^DJI')", dt);
    db_transaction(sql_cmd);
    db_upload_file("eods", filename);

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
    curl_global_cleanup();
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
void ana_scored_setups(char* stk, char* ana_date, char* next_dt, bool eod) {
    char sql_cmd[256];
    /**
     * setup_date is the date from which we start calculating setups
     */
    char *setup_date = ana_get_setup_date(stk, ana_date);
    /**
     *  Create the JSON array that will contain all the calculated
     *  setups
     */
    cJSON *setups = cJSON_CreateArray();
    /**
     * Run the setup analysis all the way to ana_date 
     */
    int ana_res = 0;
    while((ana_res == 0) && (strcmp(setup_date, ana_date) <= 0)) {
        ana_res = ana_jl_setups(setups, stk, setup_date, eod);
        if (ana_res == 0)
            cal_next_bday(cal_ix(setup_date), &setup_date);
    }
    /**
     *  Undo the last iteration of the while loop that moved
     *  setup_date one day too far ahead
     */
    cal_prev_bday(cal_ix(setup_date), &setup_date);
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
    cJSON *ldrs = ana_get_leaders(exp_date, max_atm_price, max_opt_spread, 0);
    get_quotes(ldrs, ana_date, exp_date, exp_date2, download_options);
    LOGINFO("Downloaded options for expiries %s, %s\n", exp_date, exp_date2);
}

void ana_indicators(cJSON *leaders, char *ana_date) {
    indicators_relative_strength(leaders, ana_date, 4);
    indicators_relative_strength(leaders, ana_date, 10);
    indicators_relative_strength(leaders, ana_date, 45);
    indicators_relative_strength(leaders, ana_date, 252);
    indicators_on_balance_volume(leaders, ana_date, 4);
    indicators_on_balance_volume(leaders, ana_date, 10);
    indicators_on_balance_volume(leaders, ana_date, 45);
    indicators_candle_strength(leaders, ana_date, 4);
    indicators_candle_strength(leaders, ana_date, 10);
    indicators_candle_strength(leaders, ana_date, 45);
}

/**
 * Main daily analysis method
 */
void ana_stx_analysis(char *ana_date, cJSON *stx, int max_atm_price,
                      int max_opt_spread, bool download_spots,
                      bool download_options, bool eod) {
    /**
     *  Get the next two expiries
     */
    char *exp_date, *exp_date2;
    int ana_ix = cal_ix(ana_date);
    int exp_ix = cal_expiry(ana_ix + (eod? 1: 0), &exp_date);
    cal_expiry(exp_ix + 1, &exp_date2);
    /**
     *  Get the list of stocks that will be analyzed
     */
    cJSON *ldr = NULL, *leaders = stx;
    if (leaders == NULL)
        leaders = ana_get_leaders(exp_date, max_atm_price, max_opt_spread, 0);
    int num = 0, total = cJSON_GetArraySize(leaders);
    LOGINFO("ana_stx_analysis() will analyze %d leaders as of %s\n", total,
            ana_date);
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
        get_quotes(leaders, ana_date, exp_date, exp_date2, download_options);
    }
    LOGINFO("Running %s analysis for %s\n", eod? "eod": "intraday", ana_date);
    LOGINFO("Calculating setups for %d stocks\n", total);
    char* next_dt = NULL;
    if (eod == true)
        cal_next_bday(cal_ix(ana_date), &next_dt);
    cJSON_ArrayForEach(ldr, leaders) {
        if (cJSON_IsString(ldr) && (ldr->valuestring != NULL)) {
            ana_scored_setups(ldr->valuestring, ana_date, next_dt, eod);
            /** TODO: fix this mess */
            /* ana_setups(ldr->valuestring, ana_date, next_dt, eod); */
            /* if (eod) */
            /*     ana_calc_rs(ldr->valuestring, ana_date, rs + num); */
        }
        num++;
        if (num % 100 == 0)
            LOGINFO("%s: analyzed %4d / %4d stocks\n", ana_date, num, total);
    }
    LOGINFO("%s: analyzed %4d / %4d stocks\n", ana_date, num, total);
    if (eod == true) {
        LOGINFO("Calculating the indicators for %d stocks as of %s\n",
                total, ana_date);
        ana_indicators(leaders, ana_date);
    }
    LOGINFO("Freeing the memory\n");
    if (stx == NULL)
       cJSON_Delete(leaders);
    LOGINFO("ana_stx_analysis(): done\n");
}

#endif
