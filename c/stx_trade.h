#ifndef __STX_TRADE_H__
#define __STX_TRADE_H__

#include <cjson/cJSON.h>
#include "stx_ana.h"
#include "stx_core.h"




cJSON* trd_get_id_leaders(char *exp_date, char *ind_date, char *ind_name,
                          int short_ind_bound, int long_ind_bound,
                          int min_stp_activity, int max_stp_range) {
    cJSON *leader_list = cJSON_CreateArray();
    if (leader_list == NULL) {
        LOGERROR("Failed to create leader_list cJSON Array.\n");
        return NULL;
    }
    char sql_cmd[512];
    memset(sql_cmd, 0, 512);
    sprintf(sql_cmd, "SELECT ticker FROM indicators_1 WHERE "
            "dt='%s' AND name='%s' "
            "AND (bucket_rank <= %d OR bucket_rank >= %d) AND "
            "ticker NOT IN (SELECT * FROM excludes) AND "
            "ticker IN (SELECT stk FROM leaders WHERE expiry='%s' AND "
            "activity>=%d and range<=%d)",
            ind_date, ind_name, short_ind_bound, long_ind_bound, exp_date,
            min_stp_activity, max_stp_range);
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

/**
 *  Get the watchlist for the next day.  Fixed length (watch_len) long
 *  and short watchlists selected; stocks exhibiting certain setups
 *  are sorted based on their indicator rank (usually for CS_45).
 *  TODO: use watchlist to keep stocks from the previous days in the
 *  intraday watchlist.
 */
void trd_eod_intraday_watchlist(char *eod_date, char *indicator_name,
                                char *setups, int watch_len, cJSON *watchlist) {
    /** Get next business day when setups might be triggered */
    char* setup_date = NULL;
    cal_next_bday(cal_ix(eod_date), &setup_date);
    char sql_cmd_up[512], sql_cmd_down[512];
    memset(sql_cmd_up, 0, 512);
    memset(sql_cmd_down, 0, 512);
    sprintf(sql_cmd_up, "SELECT DISTINCT ticker, bucket_rank, rank "
            "FROM indicators_1 i, time_setups s WHERE ticker=stk AND "
            "i.dt='%s' AND s.dt='%s' AND direction='U' AND name='%s' "
            "AND setup IN (%s) ORDER BY rank DESC LIMIT %d",
            eod_date, setup_date, indicator_name, setups, watch_len);
    sprintf(sql_cmd_down, "SELECT DISTINCT ticker, bucket_rank, rank "
            "FROM indicators_1 i, time_setups s WHERE ticker=stk AND "
            "i.dt='%s' AND s.dt='%s' AND direction='D' AND name='%s' "
            "AND setup IN (%s) ORDER BY rank LIMIT %d",
            eod_date, setup_date, indicator_name, setups, watch_len);
    char long_watchlist[512], short_watchlist[512];
    memset(long_watchlist, 0, 512);
    memset(short_watchlist, 0, 512);
    char* stk = NULL;
    int bucket_rank;
    PGresult *res = db_query(sql_cmd_up);
    int rows = PQntuples(res);
    LOGINFO("Long watchlist has %d stocks:\n", rows);
    for (int ix = 0; ix < rows; ix++) {
        stk = PQgetvalue(res, ix, 0);
        bucket_rank = atoi(PQgetvalue(res, ix, 1));
        if (ix > 0)
            strcat(long_watchlist, ",");
        strcat(long_watchlist, "'");
        strcat(long_watchlist, stk);
        strcat(long_watchlist, "'");
        printf(" %2d. %5s CS_45 = %2d\n", (ix + 1), stk, bucket_rank);
    }
    PQclear(res);
    res = db_query(sql_cmd_down);
    rows = PQntuples(res);
    LOGINFO("Short watchlist has %d stocks:\n", rows);
    for (int ix = 0; ix < rows; ix++) {
        stk = PQgetvalue(res, ix, 0);
        bucket_rank = atoi(PQgetvalue(res, ix, 1));
        if (ix > 0)
            strcat(short_watchlist, ",");
        strcat(short_watchlist, "'");
        strcat(short_watchlist, stk);
        strcat(short_watchlist, "'");
        printf(" %2d. %5s CS_45 = %2d\n", (ix + 1), stk, bucket_rank);
    }
    PQclear(res);
    /* printf("long_watchlist = %s\n", long_watchlist); */
    /* printf("short_watchlist = %s\n", short_watchlist); */
    ana_intraday_data(long_watchlist);
    ana_intraday_data(short_watchlist);
}

/**
 *  Day trading main analysis method
 */
void trd_daytrade(char *ana_date, char *ana_time, char *exp_date, cJSON *stx,
                  char *ind_name, char *ind_date, char *setups,
                  int max_long, int max_short, bool eod, bool realtime) {
    char sql_cmd_up[512],  sql_cmd_down[512];
    memset(sql_cmd_up, 0, 512);
    memset(sql_cmd_down, 0, 512);
    sprintf(sql_cmd_up, "SELECT ticker, bucket_rank, setup, direction "
            "FROM indicators_1 i, time_setups s WHERE ticker=stk AND "
            "i.dt='%s' AND s.dt='%s' AND direction='U' AND name='%s' "
            "AND setup in (%s) %s ORDER BY bucket_rank DESC LIMIT %d",
            ind_date, ana_date, ind_name, setups,
            ((realtime && !eod)? "AND triggered='t'": ""), max_long);
    sprintf(sql_cmd_down, "SELECT ticker, bucket_rank, setup, direction "
            "FROM indicators_1 i, time_setups s WHERE ticker=stk AND "
            "i.dt='%s' AND s.dt='%s' AND direction='D' AND name='%s' "
            "AND setup in (%s) %s ORDER BY bucket_rank LIMIT %d",
            ind_date, ana_date, ind_name, setups,
            ((realtime && !eod)? "AND triggered='t'": ""), max_short);
    
    PGresult *res = db_query(sql_cmd_up);
    int rows = PQntuples(res);

    /* cJSON *id_leaders = stx; */
    /* char *ind_date = NULL; */



    /* if (id_leaders == NULL) */
    /*     id_leaders = ana_get_id_leaders(exp_date, ind_date, ind_name, */
    /*                                     short_ind_bound, long_ind_bound, */
    /*                                     min_stp_activity, max_stp_range); */

    /* LOGINFO("Freeing the memory\n"); */
    /* if (stx == NULL) { */
    /*    cJSON_Delete(id_leaders); */
    /* } */
    LOGINFO("Done\n");

}

int trd_load_market(char *mkt_name, char *start_date, bool realtime) {
    if (!realtime) {
        if (!strcmp(mkt_name, "") && start_date == NULL) {
            fprintf(stderr, "To launch a realtime run use the '-r' or "
                    "'--realtime' flags\n");
            fprintf(stderr, "To launch a simulation either: \n"
                    " - load an existing simulation, using the '-m' or "
                    "the '--market-name' flag, or \n"
                    " - start a new simulation, using the '-s' or "
                    "'--start-date' flag to specify the start date \n");
            exit(1);
        }
    }
    return 0;
}

int trd_save_market(char *mkt_name, bool realtime) {
    return 0;
}

#endif
