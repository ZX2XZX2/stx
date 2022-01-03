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

#define INCREASING_ORDER 0
#define DECREASING_ORDER 1
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
                if ((order == INCREASING_ORDER &&
                     recs[ix].value < recs[ix + ixxx].value) ||
                    (order == DECREASING_ORDER &&
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
#endif
