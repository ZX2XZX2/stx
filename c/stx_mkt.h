#ifndef __STX_MKT_H__
#define __STX_MKT_H__

#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <time.h>
#include "stx_core.h"

static cJSON *mkt = NULL;


/**
 *  Check the existence (in the markets table) of a market cache with
 *  the name 'mkt_name'.  If the cache exists, load it in the mkt
 *  cJSON pointer.  If the cache does not exist, mkt will remain NULL
 */
void load_market(char *mkt_name) {
    if (mkt == NULL) {
        char sql_cmd[1024];
        sprintf(sql_cmd, "SELECT mkt_name, mkt_cache FROM market_caches "
                "WHERE mkt_name='%s'", mkt_name);
        PGresult *res = db_query(sql_cmd);
        int rows = PQntuples(res);
        if (rows > 0) {
            LOGINFO("Loading market %s from database\n", mkt_name);
            char *mkt_cache_db = PQgetvalue(res, 0, 1);
            mkt = cJSON_Parse(mkt_cache_db);
            PQclear(res);
        }
        
    }
}


/**
 *  Create a new market from scratch, following the schema in
 *  market.json.
*/
void create_market(char *mkt_name) {
    LOGINFO("Creating new market %s\n", mkt_name);
    mkt = cJSON_CreateObject();
    cJSON_AddStringToObject(mkt, "name", mkt_name);
    cJSON *portfolio = cJSON_CreateArray();
    cJSON_AddItemToObject(mkt, "portfolio", portfolio);
    cJSON *watchlist = cJSON_CreateArray();
    cJSON_AddItemToObject(mkt, "watchlist", watchlist);
    cJSON *setups = cJSON_CreateArray();
    cJSON_AddItemToObject(mkt, "setups", setups);
    cJSON *stx = cJSON_CreateArray();
    cJSON_AddItemToObject(mkt, "stx", stx);
    cJSON *stats = cJSON_CreateObject();
    cJSON_AddItemToObject(mkt, "stats", stats);
    LOGINFO("Market %s created\n", mkt_name);
}


/**
 *  Start using a market cache.  If the mkt was previously loaded, do
 *  nothing.  If market was already cached in DB, load it from there.
 *  If no such market exists, create a new one.
 */
void enter_market(char *mkt_name) {
    LOGINFO("Enter market %s\n", mkt_name);
    if (mkt == NULL)
        load_market(mkt_name);
    if (mkt == NULL)
        create_market(mkt_name);
}


/**
 *  Store the market cache in the database.  In case of market name
 *  conflict, overwrite the contents previously stored.
 **/
void save_market(char *mkt_name) {
    if (mkt == NULL) {
        LOGWARN("Nothing to save, mkt is empty\n");
        return;
    }
    char *string = cJSON_Print(mkt);
    if (string == NULL) {
        LOGERROR("Could not print the market\n");
        return;
    }
    char *sql_cmd = (char *) calloc(strlen(string) + 512, sizeof(char));
    sprintf(sql_cmd, "INSERT INTO market_caches VALUES ('%s', '%s') "
            "ON CONFLICT ON CONSTRAINT market_caches_pkey DO "
            "UPDATE SET mkt_cache=EXCLUDED.mkt_cache", mkt_name, string); 
    db_transaction(sql_cmd);
    LOGINFO("Saved market %s to database\n", mkt_name);
    free(string);
    free(sql_cmd);
    sql_cmd = NULL;
    string = NULL;
}


/**
 *  Print the contents of a market
 */
void print_market(char *mkt_name, char *mkt_subset) {
    if (mkt == NULL) {
        LOGWARN("Nothing to print, mkt is empty\n");
        return;
    }
    char *string = cJSON_Print(mkt);
    if (string == NULL) {
        LOGERROR("Could not print the market\n");
        return;
    }
    LOGINFO("The market is:\n%s\n", string);
    free(string);
    string = NULL;
}


void exit_market(char *mkt_name) {
    if (mkt == NULL) {
        LOGWARN("Market %s already exited, mkt is NULL\n", mkt_name);
        return;
    }
    save_market(mkt_name);
    cJSON_Delete(mkt);
    LOGINFO("Exited market %s\n", mkt_name);
}
#endif
