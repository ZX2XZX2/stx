#define _XOPEN_SOURCE
#include <stdio.h>
#include <locale.h>
#include <time.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "stx_trade.h"

static volatile bool keep_running = true;

void sig_handler(int dummy) {
    keep_running = false;
}


/**
 *  This is supposed to run in two modes: realtime and simulation.
 *  Realtime runs during trading hours, get the data and analyzes it.
 *  Cannot control the speed; 'sleep_interval' can be used however, to
 *  specify how often to refresh the data and analyze it intraday.
 *  Simulation runs using historical data.  When in simulation mode,
 *  can control how fast it runs, can eliminate sleep altogether and
 *  replace it with command to move from input; also we can skip
 *  certain times if no events happened.
 */
int main(int argc, char** argv) {
    char mkt_name[32], dir_name[128], *start_date = NULL;
    int sleep_interval = 300;
    bool realtime = false;
    memset(mkt_name, 0, 32);
    memset(dir_name, 0, 128);
    for (int ix = 1; ix < argc; ix++) {
        if ((!strcmp(argv[ix], "-m") || !strcmp(argv[ix], "--market-name")) &&
             (ix++ < argc - 1))
            strcpy(mkt_name, argv[ix]);
        else if (!strcmp(argv[ix], "-r") || !strcmp(argv[ix], "--realtime")) {
            strcpy(mkt_name, "realtime");
            realtime = true;
        } else if ((!strcmp(argv[ix], "-i") ||
                    !strcmp(argv[ix], "--interval")) && (ix++ < argc - 1))
            sleep_interval = atoi(argv[ix]);
        else if ((!strcmp(argv[ix], "-s") ||
                  !strcmp(argv[ix], "--start-date")) && (ix++ < argc - 1))
            start_date = cal_move_to_bday(argv[ix], true);
    }
    signal(SIGINT, sig_handler);
    /**
     *  If running in realtime, need to check, during initialization,
     *  that we are prepared to run intraday analysis - the stock list
     *  is defined, all the data is stored in cache, etc.
     *
     *  Same thing for simulation, but two cases, either resuming a
     *  previous simulation, or starting a new one.
     */
    if (trd_load_market(mkt_name, start_date, realtime) != 0) {
        LOGERROR("Failed to load market, exiting ...\n" );
        exit(1);
    }
    while (keep_running) {
        LOGINFO("Still running ...\n");
        sleep(sleep_interval);
    }
    LOGINFO("Saving the market before exiting\n");
    trd_save_market(mkt_name, realtime);
}
