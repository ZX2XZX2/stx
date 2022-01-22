#ifndef __STX_INDICATORS_H__
#define __STX_INDICATORS_H__

#include <libpq-fe.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_jl.h"
#include "stx_ts.h"
#include <time.h>

#define ASCENDING_ORDER 0
#define DESCENDING_ORDER 1
/** Code to sort the stocks.  Initially used to calculate relative
 *  strength, IBD style.
 */

typedef struct eq_value_t {
    char ticker[16];
    int value;
    int rank;
    int bucket_rank;
} eq_value, *eq_value_ptr;

/** Use shell sort to sort num records, in (in/de)creasing order, as
 *  specified by the order param, using their value field. The rank
 *  and bucket_rank fields should not have any meaningful values at
 *  this point, and they are ignored.
 */
void indicators_shell_sort(eq_value_ptr recs, int num, int order) {
    int ix, ixx, ixxx, ixxxx;
    eq_value temp;
    for(ixxx = num / 2; ixxx > 0; ixxx /= 2) {
        for(ixx = ixxx; ixx < num; ixx++) {
            for(ix = ixx - ixxx; ix >= 0; ix -= ixxx) {
                if ((order == DESCENDING_ORDER &&
                     recs[ix].value < recs[ix + ixxx].value) ||
                    (order == ASCENDING_ORDER &&
                     recs[ix].value > recs[ix + ixxx].value)) {
                    memset(&temp, 0, sizeof(eq_value));
                    strcpy(temp.ticker, recs[ix].ticker);
                    temp.value= recs[ix].value;
                    strcpy(recs[ix].ticker, recs[ix + ixxx].ticker);
                    recs[ix].value = recs[ix + ixxx].value;
                    strcpy(recs[ix + ixxx].ticker, temp.ticker);
                    recs[ix + ixxx].value = temp.value;
                }
            }
        }
    }
}

/** Takes as input num_records records the date, the indicator name,
 *  and the number of rank buckets.  Uses indicators_shell_sort to
 *  sort the records, using their value field. Uses the sorted array
 *  to populate the rank and the bucket rank fields in the eq_value
 *  structures.  Inserts the results in the indicators_1 database
 *  table.
 */
void indicators_rank(eq_value_ptr records, char* dt, char* indicator_name,
                     int num_records, int num_buckets, int order) {
    indicators_shell_sort(records, num_records, order);
    int bucket_size = num_records / num_buckets,
        unbucketed = num_records % num_buckets,
        current_bucket_size = bucket_size,
        total = 0,
        processed = 0;

    for(int ix = 0; ix < num_buckets; ix++) {
        printf("%d: (", ix);
        current_bucket_size = (ix < unbucketed)? bucket_size + 1: bucket_size;
        for(int ixx = 0; ixx < current_bucket_size; ixx++) {
            eq_value_ptr rec = &(records[ixx + processed]);
            printf("%s ", rec->ticker);
            rec->rank = ixx + processed;
            rec->bucket_rank = ix;
            char sql_cmd[1024];
            sprintf(sql_cmd, "INSERT INTO indicators_1 VALUES "
                    "('%s', '%s', '%s', %d, %d, %d)"
                    " ON CONFLICT ON CONSTRAINT indicators_1_pkey DO "
                    "UPDATE SET value=%d, rank=%d, bucket_rank=%d",
                    rec->ticker, dt, indicator_name,
                    rec->value, rec->rank, rec->bucket_rank,
                    rec->value, rec->rank, rec->bucket_rank);
            db_transaction(sql_cmd);
        }
        processed += current_bucket_size;
        printf(")\n");
    }
}

int stock_relative_strength(stx_data_ptr data, int rs_days) {
    int rsd1 = rs_days / 4, rsd2 = rs_days / 2, ix = data->pos;
    float rs_1, rs_2, rs_3, res;
    float cc = (float)data->data[ix].close;
    float cc_0 = (float)data->data[0].close;
    float cc_1 = (rsd1 > 1)? (float)data->data[ix + 1 - rsd1].close:
        (float) data->data[ix].open;
    float cc_2 = (rsd2 > 1)? (float) data->data[ix + 1 - rsd2].close:
        (float) data->data[ix].open;
    float cc_3 = (rs_days > 1)? (float)data->data[ix + 1 - rs_days].close:
        (float) data->data[ix].open;
    if (ix >= rs_days - 1) {
        rs_1 = (cc_1 == 0)? 0: cc / cc_1 - 1;
        rs_2 = (cc_2 == 0)? 0: cc / cc_2 - 1;
        rs_3 = (cc_3 == 0)? 0: cc / cc_3 - 1;
    } else if (ix >= rsd2 - 1) {
        rs_1 = (cc_1 == 0)? 0: cc / cc_1 - 1;
        rs_2 = (cc_2 == 0)? 0: cc / cc_2 - 1;
        rs_3 = (cc_0 == 0)? 0: cc / cc_0 - 1;
    } else if (ix >= rsd1 - 1) {
        rs_1 = (cc_1 == 0)? 0: cc / cc_1 - 1;
        rs_2 = (cc_0 == 0)? 0: cc / cc_0 - 1;
        rs_3 = rs_2;
    } else {
        rs_1 = (cc_0 == 0)? 0: cc / cc_0 - 1;
        rs_2 = rs_1;
        rs_3 = rs_1;
    }
    res = 4000 * rs_1 + 3000 * rs_2 + 3000 * rs_3;
    return (int) res;
}

/**
 *  This is actually a variation on the A/D formula; the calculation
 *  of the daily indicators is done in the jl_set_obv function. The
 *  value is divided every day by the (20-day) average volume for that
 *  day and multiplied by 100.  Which means that OBV for 1 day of 100
 *  is when the OBV is equal to the average daily volume for that day
 */
int stock_on_balance_volume(stx_data_ptr data, int num_days) {
    int obv = 0, end = data->pos;
    int start = (end >= num_days - 1)? (end - num_days + 1): 0;
    jl_data_ptr jld = jl_get_jl(data->stk, data->data[data->pos].date, JL_050,
                                JLF_050);
    jl_record_ptr jls = &(jld->recs[start]), jle = &(jld->recs[end]);
    if (jls->volume == 0)
        return 0;
    for (int ix = start; ix <= end; ix++) {
        int daily_obv = 0;
        for (int ixx = 0; ixx < 3; ixx++)
            daily_obv += jld->recs[ix].obv[ixx];
        /**
         *  Use average_volume to avoid division by 0
         */
        int average_volume = (jld->recs[ix].volume > 0)?
            jld->recs[ix].volume: 1000000;
#ifdef DEBUG_OBV
        fprintf(stderr, "%s, %s- %s: daily_obv = %d, average_volume = %d\n",
                data->stk, data->data[data->pos].date, data->data[ix].date,
                daily_obv, average_volume);
#endif
        daily_obv = 100 * daily_obv / average_volume;
        obv += daily_obv;
#ifdef DEBUG_OBV
        fprintf(stderr, "%s, %s- %s: daily_obv = %d, average_volume = %d\n",
                data->stk, data->data[data->pos].date, data->data[ix].date,
                daily_obv, average_volume);
#endif
    }
#ifdef DEBUG_OBV
        fprintf(stderr, "%s, %s: obv = %d\n", data->stk,
                data->data[data->pos].date, obv);
#endif
    return obv;
}

int stock_candle_strength(stx_data_ptr data, int num_days) {
    return 0;
}

void indicators(char* indicator_type, cJSON *tickers, char* asof_date,
                int num_days) {
    cJSON *ticker = NULL;
    int num = 0, total = cJSON_GetArraySize(tickers);
    char indicator_name[8];
    memset(indicator_name, 0, 8);
    sprintf(indicator_name, "%s_%d", indicator_type, num_days);
    LOGINFO("%s for %d tickers as of %s\n", indicator_name, total, asof_date);
    eq_value_ptr ev = (eq_value_ptr) calloc((size_t)total, sizeof(eq_value));
    memset(ev, 0, total * sizeof(eq_value));
    cJSON_ArrayForEach(ticker, tickers) {
        if (cJSON_IsString(ticker) && (ticker->valuestring != NULL)) {
            eq_value_ptr crt_ev = ev + num;
            strcpy(crt_ev->ticker, ticker->valuestring);
            stx_data_ptr data = ts_get_ts(crt_ev->ticker, asof_date, 0);
            if (data == NULL || data->pos == -1) {
                LOGERROR("No data for %s. Wont calc %s\n", crt_ev->ticker,
                         indicator_name);
            } else {
                if (!strcmp(indicator_type, "RS")) {
                    crt_ev->value = stock_relative_strength(data, num_days);
                    num++;
                } else if (!strcmp(indicator_type, "OBV")) {
                    crt_ev->value = stock_on_balance_volume(data, num_days);
                    num++;
                } else if (!strcmp(indicator_type, "CS")) {
                    /* crt_ev->value = stock_candle_strength(data, num_days); */
                    /* num++; */
                }
            }
        }
        if (num % 100 == 0)
            LOGINFO("  %s for %5d / %5d tickers as of %s\n", indicator_name,
                    num, total, asof_date);
    }
    LOGINFO(" %s for %5d / %5d tickers as of %s\n", indicator_name, num,
            total, asof_date);
    indicators_rank(ev, asof_date, indicator_name, num, 100, ASCENDING_ORDER);
    LOGINFO("%s: inserted in DB ranked records\n", indicator_name);
    if (ev != NULL) {
        free(ev);
        ev = NULL;
    }
    LOGINFO("%s: freed the data\n", indicator_name);
}

void indicators_relative_strength(cJSON *tickers, char* asof_date,
                                  int num_days) {
    indicators("RS", tickers, asof_date, num_days);
}

void indicators_candle_strength(cJSON *tickers, char* asof_date, int num_days) {
    indicators("CS", tickers, asof_date, num_days);
}

void indicators_on_balance_volume(cJSON *tickers, char* asof_date,
                                  int num_days) {
    indicators("OBV", tickers, asof_date, num_days);
}
#endif
