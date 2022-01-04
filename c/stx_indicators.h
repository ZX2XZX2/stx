#ifndef __STX_INDICATORS_H__
#define __STX_INDICATORS_H__

#include <libpq-fe.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
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
                if ((order == ASCENDING_ORDER &&
                     recs[ix].value < recs[ix + ixxx].value) ||
                    (order == DESCENDING_ORDER &&
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
    res = 40 * rs_1 + 30 * rs_2 + 30 * rs_3;
    return (int) res;
}

void indicators_stock_relative_strength(cJSON *tickers, char* asof_date,
                                        int num_days) {
    cJSON *ticker = NULL;
    int num = 0, total = cJSON_GetArraySize(tickers);
    char rs_name[8];
    memset(rs_name, 0, 8);
    sprintf(rs_name, "RS_%d", num_days);
    LOGINFO("%s for %d tickers as of %s\n", rs_name, total, asof_date);
    eq_value_ptr rs = (eq_value_ptr) calloc((size_t)total, sizeof(eq_value));
    memset(rs, 0, total * sizeof(eq_value));
    cJSON_ArrayForEach(ticker, tickers) {
        if (cJSON_IsString(ticker) && (ticker->valuestring != NULL)) {
            eq_value_ptr crt_rs = rs + num;
            strcpy(crt_rs->ticker, ticker->valuestring);
            stx_data_ptr data = ts_get_ts(crt_rs->ticker, asof_date, 0);
            if (data == NULL || data->pos == -1) {
                LOGERROR("No data for %s. Wont calc RS\n", crt_rs->ticker);
            } else {
                crt_rs->value = stock_relative_strength(data, num_days);
                num++;
            }
        }
        if (num % 100 == 0)
            LOGINFO("%s for %4d / %4d tickers as of %s\n", rs_name, num, total,
                    asof_date);
    }
    LOGINFO("%s for %4d / %4d tickers as of %s\n", rs_name, num, total,
            asof_date);
    indicators_rank(rs, asof_date, rs_name, num, 100, DESCENDING_ORDER);
    LOGINFO("%s: sorted tickers in RS descending order\n", rs_name);
    LOGINFO("%s: ranked tickers and inserted in DB\n", rs_name);
    if (rs != NULL) {
        free(rs);
        rs = NULL;
    }
    LOGINFO("%s: freed the data\n", rs_name);
}
#endif
