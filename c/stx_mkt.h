#ifndef __STX_MKT_H__
#define __STX_MKT_H__

#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "stx_core.h"

typedef struct market_t {
    char mkt_name[32];
    char mkt_date[20];
    char mkt_update_dt[20];
    cJSON *mkt;
    bool realtime;
} market, *market_ptr;

static market_ptr mkt = NULL;

struct stat st = {0};

void mkt_create_dirs(char *mkt_name) {
    char base_path[128], file_path[144];
    sprintf(base_path, "%s/stx/mkt/%s", getenv("HOME"), mkt_name);
    sprintf(file_path, "%s/eod", base_path);
    if (stat(file_path, &st) == -1)
        mkdir(file_path, 0700);
    sprintf(file_path, "%s/intraday", base_path);
    if (stat(file_path, &st) == -1)
        mkdir(file_path, 0700);
    sprintf(file_path, "%s/jl_eod", base_path);
    if (stat(file_path, &st) == -1)
        mkdir(file_path, 0700);
    sprintf(file_path, "%s/jl_intraday", base_path);
    if (stat(file_path, &st) == -1)
        mkdir(file_path, 0700);
}

void mkt_load_mkt(char *mkt_name) {
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
    mkt_create_dirs(mkt_name);
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
        mkt_load_mkt(mkt_name);
    else {
        if (strcmp(mkt->mkt_name, mkt_name)) {
            /** Another market loaded. Exit, and load new market */
            mkt_exit(mkt->mkt_name);
            mkt_load_mkt(mkt_name);
        }
    }
}

/**
 *  Create a new market from scratch, following the schema in
 *  market.json.
*/
void mkt_create(char *mkt_name, char *mkt_date, bool realtime) {    
    LOGINFO("Creating new %s market %s, as of %s\n",
            realtime? "realtime": "simulation", mkt_name, mkt_date);
    mkt = (market_ptr) calloc((size_t) 1, sizeof(market));
    strcpy(mkt->mkt_name, mkt_name);
    strcpy(mkt->mkt_date, mkt_date);
    sprintf(mkt->mkt_update_dt, "%s 16:00:00",
            cal_move_to_bday("1985-01-31", true));
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
    mkt_save(mkt_name);
    mkt_create_dirs(mkt_name);
    LOGINFO("%s market %s created\n", realtime? "Realtime": "Simulation",
             mkt_name);
}


/**
 *  Start using a market cache.  If the mkt was previously loaded, do
 *  nothing.  If market was already cached in DB, load it from there.
 *  If no such market exists, create a new one.  mkt_name and realtime
 *  parameters are required.  If realtime is true, mkt_date will be
 *  set automatically to the current business day, if during trading
 *  hours, or the next, upcoming trading day if outside of trading
 *  hours. If in_mkt_date is NULL and realtime is set to false,
 *  mkt_date will be set to last business day.
 */
void mkt_enter(char *mkt_name, char *in_mkt_date, bool realtime) {
    LOGINFO("Enter %s market %s\n", realtime? "realtime": "simulation",
            mkt_name);
    if (mkt == NULL)
        mkt_load(mkt_name);
    /** Create new market if market could not be loaded. */
    if (mkt == NULL) {
        char *mkt_date = NULL;
        if (realtime)
            mkt_date = cal_market_date(NULL);
        else {
            if (in_mkt_date != NULL)
                mkt_date = in_mkt_date;
            else
                mkt_date = cal_current_trading_date();
        }
        LOGINFO("Creating new %s market %s, as of %s\n",
                realtime? "realtime": "simulation", mkt_name, mkt_date);
        mkt_create(mkt_name, mkt_date, realtime);
    }
}

/**
 *  Print the contents of a market.  The parameter mkt_path is a '/'
 *  separated path in the json market state.  For a list, a particular
 *  index is specified within square brackets.  For example, to
 *  display the first portfolio position, 'mkt_path=portfolio[0]'.  If
 *  mkt_path is NULL, then display the entire market description.  If
 *  the parameter short_description is set to true, then display only
 *  the market name, market date, last update datetime and realtime.
 */
void mkt_print(char *mkt_name, char *mkt_path, bool short_description) {
    if (mkt == NULL) {
        LOGWARN("Nothing to print, mkt is empty\n");
        return;
    }
    LOGINFO("\nmkt_name = %s, mkt_date = %s, last_updated_dt = %s, "
            "realtime = %s\n", mkt->mkt_name, mkt->mkt_date, mkt->mkt_update_dt,
            mkt->realtime? "true": "false");
    if (short_description)
        return;
    char *mkt_string = cJSON_Print(mkt->mkt);
    if (mkt_string == NULL) {
        LOGERROR("Could not print the market\n");
        return;
    }
    LOGINFO("The market is:\n%s\n", mkt_string);
    free(mkt_string);
    mkt_string = NULL;
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
