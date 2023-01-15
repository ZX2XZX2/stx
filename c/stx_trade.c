#define _XOPEN_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <locale.h>
#include <time.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "stx_mkt.h"

int get_lock(void) {
    int fdlock;
    struct flock fl;
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 1;
    if((fdlock = open("oneproc.lock", O_WRONLY|O_CREAT, 0666)) == -1)
        return 0;
    if(fcntl(fdlock, F_SETLK, &fl) == -1)
        return 0;
    return 1;
}

static volatile bool keep_running = true;

void sig_handler(int dummy) {
    keep_running = false;
}


/**
 *  This is supposed to run in two modes: realtime and simulation.
 *  Realtime runs during trading hours, get the data and analyzes it.
 *  Cannot control the speed; 'sleep_interval' can be used however, to
 *  specify how often to refresh the data and analyze it intraday.
 *  'sleep_interval' can only have the following values: 300
 *  (5-minute, default), 600 (10-minute), 900 (15-minute), 1800
 *  (30-minute).  Simulation runs using historical data.  When in
 *  simulation mode, analysis takes place every 5 minutes, and we can
 *  control how fast it runs, can eliminate sleep altogether and
 *  replace it with command to move from input; also we can skip
 *  certain times if no events happened.
 */
int main(int argc, char** argv) {
    if(!get_lock()) {
        fputs("Process already running!\n", stderr);
        return 1;
    }
    char mkt_name[64], *start_date = NULL;
    int interval = 300, num_runs = 78, run_times[80];
    bool realtime = false, keep_going_if_no_events = false;
    srand(time(NULL));
    memset(mkt_name, 0, 64 * sizeof(char));
    for (int ix = 1; ix < argc; ix++) {
        if ((!strcmp(argv[ix], "-m") || !strcmp(argv[ix], "--market-name")) &&
             (ix++ < argc - 1))
            strcpy(mkt_name, argv[ix]);
        else if (!strcmp(argv[ix], "-r") || !strcmp(argv[ix], "--realtime")) {
            strcpy(mkt_name, "realtime");
            realtime = true;
        } else if ((!strcmp(argv[ix], "-i") ||
                    !strcmp(argv[ix], "--interval")) && (ix++ < argc - 1))
            interval = atoi(argv[ix]);
        else if ((!strcmp(argv[ix], "-s") ||
                  !strcmp(argv[ix], "--start-date")) && (ix++ < argc - 1))
            start_date = cal_move_to_bday(argv[ix], true);
        else if (!strcmp(argv[ix], "-k") || !strcmp(argv[ix], "--skip"))
            keep_going_if_no_events = true;
    }
    signal(SIGINT, sig_handler);
    /**
     *  Validate the input interval parameter.  If realtime, interval
     *  can be 300, 600, 900, or 1800 seconds.  If simulation,
     *  interval can be any number from 0 to 300.
     */
    if (realtime) {
        if ((interval != 300) && (interval != 600) && (interval != 900) &&
            (interval != 1800)) {
            LOGERROR("Invalid interval (%d) specified for realtime market.  "
                     "Interval must be one of 300, 600, 900, or 1800\n",
                     interval);
            exit(1);
        }
    } else {
        if ((interval < 0) || (interval > 300)) {
            LOGERROR("Invalid interval (%d) specified for simulation market.  "
                     "Interval must be either 0 (requires keyboard input to "
                     "move to next tick, or any number between 1 and 300\n",
                     interval);
            exit(1);
        }
    }
    /**
     *  Setup the start date and the market name if they were not
     *  specified in the input parameters
     */
    if (start_date == NULL) {
        start_date = cal_last_intraday_date();
        LOGINFO("start_date = %s\n", start_date);
    }
    if (!strcmp(mkt_name, "")) {
        sprintf(mkt_name, "sim-%s", start_date);
        LOGINFO("No market name, assigning default %s\n", mkt_name);
    }
    /**
     *  If running in realtime, need to check, during initialization,
     *  that we are prepared to run intraday analysis - the stock list
     *  is defined, all the data is stored in cache, etc.
     *
     *  Same thing for simulation, but two cases, either resuming a
     *  previous simulation, or starting a new one.
     */
    mkt_enter(mkt_name, start_date, realtime);
    while (keep_running) {
        if (realtime) {
            LOGWARN("Not implemented yet, exiting main loop\n");
            keep_running = false;
        } else {
            LOGINFO("Still running ...\n");
            int res = mkt_analyze();
            LOGINFO("mkt_analyze returned %d\n", res);
            if ((res > 0) || !keep_going_if_no_events)
                sleep(interval);
        }
    }
    LOGINFO("Saving the market before exiting\n");
    /* trd_save_market(mkt_name, realtime); */
}
