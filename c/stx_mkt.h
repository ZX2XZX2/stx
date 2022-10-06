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
 *  Create a new market from scratch, following the schema in
 *  market.json.  TODO: check if market was already saved in DB.
*/
void create_market(char *mkt_name) {
    if (mkt == NULL) {
        mkt = cJSON_CreateObject();
        cJSON_AddStringToObject(mkt, "name", mkt_name);
        portfolio = cJSON_CreateArray();
        cJSON_AddItemToObject(mkt, "portfolio", portfolio);
        watchlist = cJSON_CreateArray();
        cJSON_AddItemToObject(mkt, "watchlist", watchlist);
        setups = cJSON_CreateArray();
        cJSON_AddItemToObject(mkt, "setups", setups);
        stx = cJSON_CreateArray();
        cJSON_AddItemToObject(mkt, "stx", stx);
        stats = cJSON_CreateObject();
        cJSON_AddItemToObject(mkt, "stats", stats);
    }
}


void save_market(char *mkt_name) {

}


void load_market(char *mkt_name) {

}


void print_market(char *mkt_name, char *mkt_subset) {

}

#endif
