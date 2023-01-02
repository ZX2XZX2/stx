#ifndef __STX_MKT_H__
#define __STX_MKT_H__

#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <time.h>
#include "stx_core.h"

typedef struct market_t {
    char mkt_name[32];
    char mkt_date[20];
    char mkt_update_dt[20];
    cJSON *mkt;
    bool realtime;
} market, *market_ptr;

static market_ptr mkt = NULL;

void mkt_load_data(char *mkt_name) {
    char sql_cmd[1024];
    sprintf(sql_cmd, "SELECT mkt_name, mkt_date, mkt_update_dt, mkt_cache,"
            " realtime FROM market_caches WHERE mkt_name='%s'", mkt_name);
    PGresult *res = db_query(sql_cmd);
    int rows = PQntuples(res);
    if (rows == 1) {
        LOGINFO("Loading market %s from database\n", mkt_name);
        mkt = (market_ptr) calloc((size_t) 1, sizeof(market));
        strcpy(mkt->mkt_name, PQgetvalue(res, 0, 0));
        strcpy(mkt->mkt_date, PQgetvalue(res, 0, 1));
        strcpy(mkt->mkt_update_dt, PQgetvalue(res, 0, 2));
        char *mkt_cache_db = PQgetvalue(res, 0, 3);
        mkt->mkt = cJSON_Parse(mkt_cache_db);
        mkt->realtime = PQgetvalue(res, 0, 4);
        PQclear(res);
    }
}

/**
 *  Store the market cache in the database.  In case of market name
 *  conflict, overwrite the contents previously stored.
 **/
void mkt_save(char *mkt_name) {
    if (mkt == NULL) {
        LOGWARN("Nothing to save, mkt is empty\n");
        return;
    }
    char *mkt_string = cJSON_Print(mkt->mkt);
    if (mkt_string == NULL) {
        LOGERROR("Could not print the market\n");
        return;
    }
    char *sql_cmd = (char *) calloc(strlen(mkt_string) + 512, sizeof(char));
    sprintf(sql_cmd, "INSERT INTO market_caches VALUES "
            "('%s', '%s', '%s', '%s', '%s') "
            "ON CONFLICT ON CONSTRAINT market_caches_pkey DO "
            "UPDATE SET mkt_date=EXCLUDED.mkt_date,"
            "mkt_update_dt=EXCLUDED.mkt_update_dt,"
            "mkt_cache=EXCLUDED.mkt_cache,"
            "realtime=EXCLUDED.realtime", mkt->mkt_name, mkt->mkt_date,
            mkt->mkt_update_dt, mkt_string, mkt->realtime? "TRUE": "FALSE"); 
    db_transaction(sql_cmd);
    LOGINFO("Saved market %s to database\n", mkt_name);
    free(mkt_string);
    free(sql_cmd);
    sql_cmd = NULL;
    mkt_string = NULL;
}

/**
 *  Exit market - save internal state to database and free the memory
 *  allocated to the mkt json pointer
 */
void mkt_exit(char *mkt_name) {
    if (mkt == NULL) {
        LOGWARN("Market %s already exited, mkt is NULL\n", mkt_name);
        return;
    }
    mkt_save(mkt_name);
    cJSON_Delete(mkt->mkt);
    free(mkt);
    mkt = NULL;
    LOGINFO("Exited market %s\n", mkt_name);
}

/**
 *  Check the existence (in the markets table) of a market cache with
 *  the name 'mkt_name'.  If the cache exists, load it in the mkt
 *  cJSON pointer.  If the cache does not exist, mkt will remain NULL.
 *  If another market is already loaded, exit it and load the new
 *  market.
 */
void mkt_load(char *mkt_name) {
    if (mkt == NULL)
        mkt_load_data(mkt_name);
    else {
        if (strcmp(mkt->mkt_name, mkt_name)) {
            /** Another market loaded. Exit, and load new market */
            mkt_exit(mkt->mkt_name);
            mkt_load_data(mkt_name);
        }
    }
}

/**
 *  Create a new market from scratch, following the schema in
 *  market.json.
*/
void mkt_create(char *mkt_name, char *in_mkt_date, bool realtime) {
    
    char *mkt_date = cal_current_trading_date(), mkt_update_dt[20];
    memset(mkt_update_dt, 0, 20 * sizeof(char));
    char *last_update_dt = cal_move_to_bday("1985-01-31", true);
    sprintf(mkt_update_dt, "%s 16:00:00", last_update_dt);
    if (!realtime && in_mkt_date != NULL)
        mkt_date = in_mkt_date;
    LOGINFO("Creating new %s market %s, as of %s\n",
            realtime? "realtime": "simulation", mkt_name, mkt_date);
    mkt = (market_ptr) calloc((size_t) 1, sizeof(market));
    strcpy(mkt->mkt_name, mkt_name);
    strcpy(mkt->mkt_date, mkt_date);
    strcpy(mkt->mkt_update_dt, mkt_update_dt);
    mkt->realtime = realtime;
    mkt->mkt = cJSON_CreateObject();
    cJSON *portfolio = cJSON_CreateArray();
    cJSON_AddItemToObject(mkt->mkt, "portfolio", portfolio);
    cJSON *watchlist = cJSON_CreateArray();
    cJSON_AddItemToObject(mkt->mkt, "watchlist", watchlist);
    cJSON *setups = cJSON_CreateArray();
    cJSON_AddItemToObject(mkt->mkt, "setups", setups);
    cJSON *stx = cJSON_CreateArray();
    cJSON_AddItemToObject(mkt->mkt, "stx", stx);
    cJSON *stats = cJSON_CreateObject();
    cJSON_AddItemToObject(mkt->mkt, "stats", stats);
    LOGINFO("%s market %s created\n", realtime? "Realtime": "Simulation",
             mkt_name);
}


/**
 *  Start using a market cache.  If the mkt was previously loaded, do
 *  nothing.  If market was already cached in DB, load it from there.
 *  If no such market exists, create a new one.
 */
void mkt_enter(char *mkt_name, char *mkt_date, bool realtime) {
    LOGINFO("Enter market %s\n", mkt_name);
    if (mkt == NULL)
        mkt_load(mkt_name);
    /** TODO: fix the parameters passed to this function */
    if (mkt == NULL)
        mkt_create(mkt_name, mkt_date, realtime);
}

/**
 *  Print the contents of a market.  The parameter mkt_path is a '/'
 *  separated path in the json market state.  For a list, a particular
 *  index is specified within square brackets.  For example, to
 *  display the first portfolio position, 'mkt_path=portfolio[0]'
 */
void mkt_print(char *mkt_name, char *mkt_path) {
    if (mkt == NULL) {
        LOGWARN("Nothing to print, mkt is empty\n");
        return;
    }
    char *string = cJSON_Print(mkt->mkt);
    if (string == NULL) {
        LOGERROR("Could not print the market\n");
        return;
    }
    LOGINFO("The market is:\n%s\n", string);
    free(string);
    string = NULL;
}

/**
 *  Generate a new market cache at the end of the day
 */
void gen_eod_market_cache(char *mkt_name, char *mkt_date, bool realtime) {
    mkt_enter(mkt_name, mkt_date, realtime);
    /** TODO: Update market here */
    mkt_exit(mkt_name);
}
#endif
