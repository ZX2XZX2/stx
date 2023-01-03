#define _XOPEN_SOURCE
#include <assert.h>
#include <stdio.h>
#include <locale.h>
#include <time.h>
#include "stx_core.h"
#include "stx_ts.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int main(void) {
    char dt[20];
 
    fprintf(stderr, "*** Testing cal_move_5mins ...");
    strcpy(dt, "2022-11-25 15:50:00");
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-25 15:55:00"));
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-28 09:30:00"));
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-28 09:35:00"));
    cal_move_5mins(dt, -1);
    assert(!strcmp(dt, "2022-11-28 09:30:00"));
    cal_move_5mins(dt, -1);
    assert(!strcmp(dt, "2022-11-25 15:55:00"));
    cal_move_5mins(dt, -1);
    assert(!strcmp(dt, "2022-11-25 15:50:00"));
    strcpy(dt, "2022-11-27 15:50:00");
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-28 09:30:00"));
    strcpy(dt, "2022-11-27 15:50:00");
    cal_move_5mins(dt, -1);
    assert(!strcmp(dt, "2022-11-25 15:55:00"));
    strcpy(dt, "2022-11-25 05:50:00");
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-25 09:30:00"));
    strcpy(dt, "2022-11-25 05:50:00");
    cal_move_5mins(dt, -1);
    assert(!strcmp(dt, "2022-11-23 15:55:00"));
    strcpy(dt, "2022-11-25 17:50:00");
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-28 09:30:00"));
    strcpy(dt, "2022-11-25 17:50:00");
    cal_move_5mins(dt, -1);
    assert(!strcmp(dt, "2022-11-25 15:55:00"));
    fprintf(stderr, " passed\n");
 
    fprintf(stderr, "*** Testing cal_get_date_from_dt ...");
    char *dt_date = cal_get_date_from_dt(dt);
    assert(!strcmp(dt_date, "2022-11-25"));
    assert(!strcmp(dt, "2022-11-25 15:55:00"));
    fprintf(stderr, " passed\n");
 
    fprintf(stderr, "*** Testing cal_5min_ticks ...");
    int num_ticks = cal_5min_ticks("2022-11-25 09:30:00", "2022-11-25 15:55:00");
    assert(num_ticks == 78);
    fprintf(stderr, " passed\n");

    fprintf(stderr, "*** Testing cal_move_5mins DST ...");
    strcpy(dt, "2022-11-02 09:30:00");
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-02 09:35:00"));
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-02 09:40:00"));
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-02 09:45:00"));
    strcpy(dt, "2022-11-09 09:30:00");
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-09 09:35:00"));
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-09 09:40:00"));
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-11-09 09:45:00"));
    strcpy(dt, "2022-10-24 09:30:00");
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-10-24 09:35:00"));
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-10-24 09:40:00"));
    cal_move_5mins(dt, 1);
    assert(!strcmp(dt, "2022-10-24 09:45:00"));
    fprintf(stderr, " passed\n");

    fprintf(stderr, "*** Testing cal_market_date ...");
    char *mkt_date = cal_market_date(NULL);
    assert(mkt_date != NULL);
    mkt_date = cal_market_date("2023-01-03 09:00:00");
    assert(!strcmp(mkt_date, "2023-01-03"));
    mkt_date = cal_market_date("2023-01-03 09:30:00");
    assert(!strcmp(mkt_date, "2023-01-03"));
    mkt_date = cal_market_date("2023-01-03 17:00:00");
    assert(!strcmp(mkt_date, "2023-01-04"));
    fprintf(stderr, " passed\n");

    cal_free();
    return 0;
}