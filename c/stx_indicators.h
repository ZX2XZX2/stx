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
        /* printf("%d: (", ix); */
        current_bucket_size = (ix < unbucketed)? bucket_size + 1: bucket_size;
        for(int ixx = 0; ixx < current_bucket_size; ixx++) {
            eq_value_ptr rec = &(records[ixx + processed]);
            /* printf("%s ", rec->ticker); */
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
        /* printf(")\n"); */
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
    jl_data_ptr jld = jl_get_jl(data->stk, data->data[data->pos].date, JL_100,
                                JLF_100);
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

/**
 *  Return a score associated with a strong close day.  The score is
 *  calculated based on three factors: the range ratio, the volume
 *  ratio, and the body ratio.
 */
int strong_close_score(jl_data_ptr jld, int ix) {
    /**
     *  Do not calculate scores for the first 20 days, because the
     *  first 20 JL records are not fully populated with average
     *  ranges and volumes
     */
    if (ix < 20)
        return 0;
    /**
     *  Pointers to current daily record, and previous JL record
     */
    jl_record_ptr jlr = &(jld->recs[ix - 1]);
    ohlcv_record_ptr dr = &(jld->data->data[ix]);
#ifdef DEBUG_CANDLE_STRENGTH
    fprintf(stderr, "SC %s %s: ", jld->data->stk, dr->date);
#endif
    /**
     *  Avoid division by zero (for average volume or range)
     */
    int jlr_volume = (jlr->volume == 0)? 1: jlr->volume;
    int jlr_rg = (jlr->rg == 0)? 1: jlr->rg;
    /**
     *  Calculate the range of the day, and compare it with the
     *  average range.  Do not use true range, as that will be taken
     *  into account during the gap score calculation.
     */
    int hl = dr->high - dr->low, range_ratio = 100 * hl / jlr_rg;
    /**
     *  Compare the volume of the day with the average volume.
     */
    int volume_ratio = 100 * dr->volume / jlr_volume;
    /**
     *  Determine if the day was a marubozu or not.  Strongest
     *  patterns are wide range, high volume days days where the stock
     *  opens near the low(high) and closes near the high (low).
     */
    int body = dr->close - dr->open, body_ratio = 100 * body / hl;
    /**
     *  Cap the range and volume ratios at 200. The marubozu ratio is
     *  automatically capped at 100.  It also gives the sign of the
     *  score (negative for down, positive for up).  Divide by 10000,
     *  so that a strong close score is always between -400 and 400
     */
#ifdef DEBUG_CANDLE_STRENGTH
    fprintf(stderr, "rr=%d vr=%d br=%d", range_ratio, volume_ratio, body_ratio);
#endif
    if (range_ratio > 200)
        range_ratio = 200;
    if (volume_ratio > 200)
        volume_ratio = 200;
#ifdef DEBUG_CANDLE_STRENGTH
    fprintf(stderr, " cap_rr=%d cap_vr=%d", range_ratio, volume_ratio);
#endif
    int daily_score = range_ratio * volume_ratio * body_ratio / 10000;
#ifdef DEBUG_CANDLE_STRENGTH
    fprintf(stderr, " cs=%d\n", daily_score);
#endif
    return daily_score;
}

/**
 *  Return a score for a gap.  The score is based on three factors:
 *  the range magnitude of the gap, the volume of the gap, and whether
 *  the gap was closed, or not.  ix represents the index at which the
 *  gap occurs.  end is the index of the current day.  If the gap was
 *  closed (close below/above the close before the gap) in the time
 *  interval between ix and end, then its score changes sign.
 */
int gap_score(jl_data_ptr jld, int gap_ix, int end) {
    /**
     *  Do not calculate scores for the first 20 days, because the
     *  first 20 JL records are not fully populated with average
     *  ranges and volumes
     */
    if (gap_ix < 20)
        return 0;
    /**
     *  Pointers to current, previous daily record, and previous JL record
     */
    jl_record_ptr jlr = &(jld->recs[gap_ix - 1]);
    ohlcv_record_ptr dr = &(jld->data->data[gap_ix]),
        dr_1 = &(jld->data->data[gap_ix - 1]);
#ifdef DEBUG_CANDLE_STRENGTH
    fprintf(stderr, "GAP %s %s: ", jld->data->stk, dr->date);
#endif
    /**
     *  If today's open equals yesterday's close, there is no gap, and
     *  the score is 0
     */
    int sod_gap = dr->open - dr_1->close;
    if (sod_gap == 0) {
#ifdef DEBUG_CANDLE_STRENGTH
        fprintf(stderr, "sod=%d, gap=0\n", sod_gap);
#endif
        return 0;
    }
    /**
     *  Avoid division by zero (for average volume or range)
     */
    int jlr_volume = (jlr->volume == 0)? 1: jlr->volume;
    int jlr_rg = (jlr->rg == 0)? 1: jlr->rg;
    /**
     *  Compare the volume of the gap day with the average volume.
     */
    int volume_ratio = 100 * dr->volume / jlr_volume;
    /**
     *  Iterate through every day. eod_gap is difference between the
     *  last close before the gap and the cursor close.  If eod_gap
     *  and sod_gap have opposite signs, the gap was closed.  In this
     *  case, change the score sign, and stop iterating.  Otherwise,
     *  the gap value will be the minimum of the sod_gap and eod_gap
     */
#ifdef DEBUG_CANDLE_STRENGTH
    fprintf(stderr, "sod=%d vr=%d\n", sod_gap, volume_ratio);
#endif
    int eod_gap = sod_gap, ix = gap_ix;
    for (ix = gap_ix; ix <= end; ++ix) {
        dr = &(jld->data->data[ix]);
        eod_gap = dr->close - dr_1->close;
#ifdef DEBUG_CANDLE_STRENGTH
        fprintf(stderr, "  %s: sod=%d eod=%d", dr->date, sod_gap, eod_gap);
#endif
        if (eod_gap * sod_gap < 0) {
#ifdef DEBUG_CANDLE_STRENGTH
            fprintf(stderr, "  CLOSED\n");
#endif
            break;
        }
#ifdef DEBUG_CANDLE_STRENGTH
            fprintf(stderr, "\n");
#endif
    }
    /**
     *  Calculate the ratio between the uncovered portion of the gap
     *  and the average daily range.  If the gap was covered, calc the
     *  ratio of the initial gap and the average daily range.  Take
     *  the opposite sign, as the gap was covered.
     */
    int range_ratio = 0;
    if (eod_gap * sod_gap < 0)
        range_ratio = -100 * sod_gap / jlr_rg;
    else {
        if (abs(eod_gap) < abs(sod_gap))
            range_ratio = 100 * eod_gap / jlr_rg;
        else
            range_ratio = 100 * sod_gap / jlr_rg;
    }
#ifdef DEBUG_CANDLE_STRENGTH
    fprintf(stderr, "GAP %s %s: rr=%d, vr=%d ", jld->data->stk,
            jld->data->data[jld->data->pos].date, range_ratio, volume_ratio);
#endif
    /**
     *  Cap the range and volume ratios at 200.  Divide by 100, so
     *  that a gap score is always between -400 and 400
     */
    if (range_ratio > 200)
        range_ratio = 200;
    if (range_ratio < -200)
        range_ratio = -200;
    if (volume_ratio > 200)
        volume_ratio = 200;
    int daily_score = range_ratio * volume_ratio / 100;
#ifdef DEBUG_CANDLE_STRENGTH
    fprintf(stderr, "cap_rr=%d, cap_vr=%d gap=%d\n", range_ratio, volume_ratio,
            daily_score);
#endif
    return daily_score;
}

/**
 *  Look for strong closes, wide range days (marubozus), and gaps in
 *  each direction.  Assign each one of these setups a score.  Cap the
 *  scores, to prevent one setup from playing a dominant role in the
 *  indicator calculation.
 */
int stock_candle_strength(stx_data_ptr data, int num_days) {
    int cs_score = 0, end = data->pos;
    int start = (end >= num_days - 1)? (end - num_days + 1): 0;
    jl_data_ptr jld = jl_get_jl(data->stk, data->data[data->pos].date, JL_100,
                                JLF_100);
    for (int ix = start; ix <= end; ++ix) {
        int daily_score = 0;
        int strong_close = ts_strong_close(&(data->data[ix]));
        if (strong_close != 0)
            cs_score += strong_close_score(jld, ix);
        cs_score += gap_score(jld, ix, end);
#ifdef DEBUG_CANDLE_STRENGTH
        fprintf(stderr, "CS: %s %s %s cs=%d\n", data->stk,
                data->data[data->pos].date, data->data[ix].date, cs_score);
#endif
    }
#ifdef DEBUG_CANDLE_STRENGTH
        fprintf(stderr, "CS: %s %s cs=%d\n", data->stk,
                data->data[data->pos].date, cs_score);
#endif
    return cs_score;
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
                    crt_ev->value = stock_candle_strength(data, num_days);
                    num++;
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
