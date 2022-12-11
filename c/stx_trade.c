#define _XOPEN_SOURCE
#include <stdio.h>
#include <locale.h>
#include <time.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "stx_core.h"

static volatile bool keep_running = true;

void sig_handler(int dummy) {
    keep_running = false;
}

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
    while (keep_running) {
        fprintf(stderr, "Still running ...\n");
        sleep(sleep_interval);
    }
    fprintf(stderr, "Saving the market before exiting\n");
}
