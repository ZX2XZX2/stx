#define _XOPEN_SOURCE
#include <cjson/cJSON.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_jl.h"
#include "stx_ts.h"

int main(int argc, char** argv) {
    char stk[16], ed[20];
    strcpy(stk, "NFLX");
    strcpy(ed, cal_current_trading_datetime());
    float factor = 1.5;
    int num_pivots = 0, num_lines = 3;
    bool print_pivots_only = false, print_nils = false, intraday = false;

    for (int ix = 1; ix < argc; ix++) {
        if (!strcmp(argv[ix], "-s") && (++ix < argc))
            strcpy(stk, argv[ix]);
        else if (!strcmp(argv[ix], "-e") && (++ix < argc))
            strcpy(ed, argv[ix]);
        else if (!strcmp(argv[ix], "-f") && (++ix < argc))
            factor = atof(argv[ix]);
        else if (!strcmp(argv[ix], "-n") && (++ix < argc))
            num_pivots = atoi(argv[ix]);
        else if (!strcmp(argv[ix], "-l") && (++ix < argc))
            num_lines = atoi(argv[ix]);
        else if (!strcmp(argv[ix], "-i") || !strcmp(argv[ix], "--intraday") )
            intraday = true;
        else if (!strcmp(argv[ix], "-ppo"))
            print_pivots_only = true;
        else if (!strcmp(argv[ix], "-pnils"))
            print_nils = true;
    }
    if (intraday) {
        if (strlen(ed) == 10)
            strcat(ed, " 15:55:00");
    } else {
        char *hhmm = strchr(ed, ' ');
        if (hhmm != NULL)
            *hhmm = '\0';
    }
    LOGINFO("ed = %s\n", ed);
    stx_data_ptr data = ts_load_stk(stk, ed, 0, intraday);
    LOGINFO("Loaded %d records for %s\n", data->num_recs, data->stk);
    jl_data_ptr jl = jl_jl(data, ed, factor);
    jl_print(jl, print_pivots_only, print_nils);
    for(int ix = jl->pos - num_lines; ix <= jl->pos; ix++)
        ts_print_record(&(jl->data->data[ix]));
    int num, ixx;
    jl_print_pivots(jl, num_pivots, &num);

    jl_piv_ptr p = jl_get_pivots(jl, 6);
    for(ixx = 0; ixx < p->num - 1; ixx++)
        jl_print_rec(p->pivots[ixx].date, p->pivots[ixx].state,
                     p->pivots[ixx].price, true, p->pivots[ixx].rg,
                     p->pivots[ixx].obv);
    jl_print_rec(p->pivots[ixx].date, p->pivots[ixx].state,
                 p->pivots[ixx].price, false, p->pivots[ixx].rg,
                 p->pivots[ixx].obv);
    free(p);
    p = NULL;

    /* cJSON *json_jl = jl_pivots_json(jl, 4); */
    /* char *string = cJSON_Print(json_jl); */
    /* if (string == NULL) */
    /*     fprintf(stderr, "Failed to print jl.\n"); */
    /* else */
    /*     fprintf(stderr, "%s\n", string); */
    /* cJSON_Delete(json_jl); */
}
