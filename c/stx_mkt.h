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
        /** TODO: retrieve the cache from DB here */
    }
}


/**
 *  Create a new market from scratch, following the schema in
 *  market.json.  TODO: check if market was already saved in DB.
*/
void create_market(char *mkt_name) {
    load_market(mkt_name);
    if (mkt == NULL) {
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
    }
}


void save_market(char *mkt_name) {

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

#endif
