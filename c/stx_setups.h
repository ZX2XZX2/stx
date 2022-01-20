#ifndef __STX_SETUP_H__
#define __STX_SETUP_H__

#include <cjson/cJSON.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_ts.h"

#define JC_1234      0x1
#define JC_5DAYS     0x2

#define JC_5D_DAYS 8
#define JC_5D_LB 30
#define JC_5D_UB 70

#define CANDLESTICK_MARUBOZU_RATIO 80
#define CANDLESTICK_LONG_DAY_AVG_RATIO 90
#define CANDLESTICK_SHORT_DAY_AVG_RATIO 40
#define CANDLESTICK_ENGULFING_RATIO 70
#define CANDLESTICK_HARAMI_RATIO 30
#define CANDLESTICK_HAMMER_BODY_LONG_SHADOW_RATIO 0.5
#define CANDLESTICK_HAMMER_SHORT_SHADOW_RANGE_RATIO 0.15
#define CANDLESTICK_DOJI_BODY_RANGE_RATIO 0.025
#define CANDLESTICK_EQUAL_VALUES_RANGE_RATIO 0.025

#define MAX(a, b) (a > b)? a: b
#define MIN(a, b) (a < b)? a: b

#define JL_MIN_CHANNEL_LEN 25

/**
 *  Utility function to add a setup to an array that will be later
 *  inserted in the jl_setups table
 */
void stp_add_to_setups(cJSON *setups, jl_data_ptr jl, char *setup_name,
                       int dir, cJSON *info, bool triggered) {
    if (setups == NULL)
        setups = cJSON_CreateArray();
    /**
     *  Check if the last setup in the array is JL_P, JL_SR, or JL_B.
     *  Remove it,because it is a lower strength setup for a smaller
     *  factor
     */
    if (!strcmp(setup_name, "JL_B") || !strcmp(setup_name, "JL_P")) {
        int num_setups = cJSON_GetArraySize(setups);
        if (num_setups > 0) {
            cJSON *last_setup = cJSON_GetArrayItem(setups, num_setups - 1);
            char *last_setup_name =
                cJSON_GetObjectItem(last_setup, "setup")->valuestring;
            if (!strcmp(setup_name, last_setup_name))
                cJSON_DeleteItemFromArray(setups, num_setups - 1);
        }
    }
    char *direction = (dir > 0)? "U": "D";
    int factor = (jl == NULL)? 0: (int) (100 * jl->factor);
    cJSON *res = cJSON_CreateObject();
    cJSON_AddStringToObject(res, "setup", setup_name);
    cJSON_AddNumberToObject(res, "factor", factor);
    cJSON_AddStringToObject(res, "direction", direction);
    cJSON_AddStringToObject(res, "triggered", triggered? "TRUE": "FALSE");
    cJSON_AddItemToObject(res, "info", info);
    cJSON_AddItemToArray(setups, res);
}

bool stp_jc_1234(daily_record_ptr data, int ix, char trend) {
    bool result = false;
    int inside_days = 0, ixx;
    if (trend == 'U') {
        for(ixx = ix; ixx > ix - 2; ixx--) {
            if (ixx < 0)
                break;
            if (data[ixx].low > data[ixx - 1].low) {
                if (data[ixx].high < data[ixx - 1].high)
                    inside_days++;
                else
                    ixx = -1;
            }
        }
        if ((ixx > 0) && (inside_days < 2))
            result = true;
    } else {
        for(ixx = ix; ixx > ix - 2; ixx--) {
            if (ixx < 0)
                break;
            if (data[ixx].high < data[ixx - 1].high) {
                if (data[ixx].low > data[ixx - 1].low)
                    inside_days++;
                else
                    ixx = -1;
            }
        }
        if ((ixx > 0) && (inside_days < 2))
            result = true;
    }
    return result;
}

bool stp_jc_5days(daily_record_ptr data, int ix, char trend) {
    float min = 1000000000, max = 0;
    if (ix < JC_5D_DAYS - 1)
        return false;
    for(int ixx = ix; ixx > ix - JC_5D_DAYS; ixx--) {
        if (max < data[ixx].high)
            max = data[ixx].high;
        if (min > data[ixx].low)
            min = data[ixx].low;
    }
    if (max == min)
        return false;
    float fs = 100 * (data[ix].close - min) / (max - min);
    if (((trend == 'U') && (fs < JC_5D_LB)) || 
        ((trend == 'D') && (fs > JC_5D_UB)))
        return true;
    return false;
}

/**
 *  Check whether a given day intersects a channel built by connecting
 *  two pivots.  This applies to both breakout detection, and trend
 *  channel break detection.
 */
void stp_jl_breaks(cJSON *setups, jl_data_ptr jl) {
    jl_channel channel;
    jl_get_channel(jl, &channel);
    /** return if the channel could not be built */
    if (channel.ub.px1 == 0 || channel.lb.px1 == 0)
        return;
    /* filter out cases when upper channel is below lower channel */
    if (channel.ub.ipx < channel.lb.ipx)
        return;
    int ix = jl->data->pos;
    /**
     *  find the extremes for today's price either the high/low for
     *  the day, or yesterday's close
     */
    daily_record_ptr r = &(jl->data->data[ix]), r_1 = &(jl->data->data[ix - 1]);
    int ub = (r->high > r_1->close)? r->high: r_1->close;
    int lb = (r->low < r_1->close)? r->low: r_1->close;
    /** get the average volume for the last 20 (JL window) days */
    int v_pos_2 = (jl->recs[jl->pos - 2].volume == 0)? 1:
        jl->recs[jl->pos - 2].volume;
    /**
     *  only add JL_B setup if the current record is a primary record
     *  for the factor and in the direction of the trend (e.g. RALLY
     *  or UPTREND for an up direction setup)
     */
#ifdef JL_CHANNEL_DEBUG
    LOGDEBUG("ix = %d, jl->recs[ix].lns = %d, jl->last->prim_state = %s\n",
             ix, jl->recs[ix].lns, jl_state_to_string(jl->last->prim_state));
#endif
    jl_channel_boundary_ptr cb = NULL;
    int dir = 0;
    if ((channel.ub.d1 >= JL_MIN_CHANNEL_LEN) && (channel.ub.slope <= 0) &&
        (channel.ub.ipx > lb) && (channel.ub.ipx < ub) &&
        (jl->recs[ix].lns == ix) && jl_up(jl->last->prim_state)) {
        cb = &(channel.ub);
        dir = 1;
    }
    if ((channel.lb.d1 >= JL_MIN_CHANNEL_LEN) && (channel.lb.slope >= 0) &&
        (channel.lb.ipx > lb) && (channel.lb.ipx < ub) &&
        (jl->recs[ix].lns == ix) && jl_down(jl->last->prim_state)) {
        if (dir == 1) { /** discard setups triggered in both directions */
            dir = 0;
            cb = NULL;
        } else {
            cb = &(channel.lb);
            dir = -1;
        }
    }
    if (cb != NULL) {
        cJSON *info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "ipx", cb->ipx);
        cJSON_AddNumberToObject(info, "len", cb->d1);
        cJSON_AddNumberToObject(info, "vr", 100 * r->volume / v_pos_2);
        cJSON_AddNumberToObject(info, "slope", cb->slope);
        cJSON_AddNumberToObject(info, "obv1", cb->obv1);
        cJSON_AddNumberToObject(info, "obv2", cb->obv2);
        stp_add_to_setups(setups, jl, "JL_B", dir, info, true);
    }
}

/**
 *  Check whether the action on a given day stops at a resistance /
 *  support point, or whether it pierces that resistance / support (on
 *  high volume), and whether it recovers after piercing or not.
 */
void stp_jl_support_resistance(cJSON *setups, jl_data_ptr jl) {
    /**
     *  Return if the current record is not a primary record, or it is
     *  not the first record in a new trend
     */
    if (!jl_first_new_trend(jl))
        return;
    jl_piv_ptr pivs = jl_get_pivots(jl, 10);
    /**
     *  Return if we do not have enough pivot points
     */
    if (pivs->num < 5)
        goto end;
    int i = jl->data->pos, num_pivots = pivs->num;
    jl_pivot_ptr pivots = pivs->pivots;
    jl_record_ptr jlr = &(jl->recs[i]);
    jl_pivot_ptr last_pivot = pivots + num_pivots - 2;
    int last_pivot_ix = ts_find_date_record(jl->data, last_pivot->date, 0);
    daily_record_ptr last_pivot_r = &(jl->data->data[last_pivot_ix]);
    jl_record_ptr jlr_pivot = &(jl->recs[last_pivot_ix]);
    int last_piv_volume = (last_pivot_r->volume == 0)? 1: last_pivot_r->volume;
    int last_piv_avg_volume = (jlr_pivot->volume == 0)? 1: jlr_pivot->volume;
    int num_sr_pivots = 0, sr_price = 0, dir = jl_up(last_pivot->state)? -1: 1;
    float sr_volume_ratio = 100 * last_piv_volume / last_piv_avg_volume;
    int sr_pivot_indices[10];
    memset(sr_pivot_indices, 0, 10 * sizeof(int));
    /**
     *  Add to the SR list any past pivots for which the price
     *  difference from the last pivot is within 1/5 of the average
     *  range
     */
    for(int ix = 0; ix < num_pivots - 3; ix++) {
        if (abs(last_pivot->price - pivots[ix].price) < jlr->rg / 5) {
            if (num_sr_pivots == 0)
                sr_price = pivots[ix].price;
            sr_pivot_indices[num_sr_pivots] = ix;
            num_sr_pivots++;
        }
    }
    /**
     *  If at least one pivot was found, add a JL_SR setup with the
     *  following additional info:
     *  - a list of SR pivots, including date, price, state and obv
     *    (obv calculated relative to last pivot)
     *  - the last SR pivot is the latest pivot.
     *  - the price at which support/resistance occurred
     *  - the ratio between latest pivot volume and the average volume
     *  - setup length - number of business days between earliest SR
          pivot and latest pivot
     */
    if (num_sr_pivots > 0) {
        cJSON *sr_pivots = cJSON_CreateArray();
        for (int ix = 0; ix < num_sr_pivots; ++ix) {
            jl_pivot_ptr sr_piv = pivots + sr_pivot_indices[ix];
            cJSON* sr_pivot = cJSON_CreateObject();
            cJSON_AddStringToObject(sr_pivot, "date", sr_piv->date);
            cJSON_AddStringToObject(sr_pivot, "state",
                                    jl_state_to_string(sr_piv->state));
            cJSON_AddNumberToObject(sr_pivot, "price", sr_piv->price);
            cJSON_AddNumberToObject(sr_pivot, "obv",
                (last_pivot->obv - sr_piv->obv));
            cJSON_AddItemToArray(sr_pivots, sr_pivot);
        }
        cJSON* sr_pivot = cJSON_CreateObject();
        cJSON_AddStringToObject(sr_pivot, "date", last_pivot->date);
        cJSON_AddStringToObject(sr_pivot, "state",
                                jl_state_to_string(last_pivot->state));
        cJSON_AddNumberToObject(sr_pivot, "price", last_pivot->price);
        cJSON_AddNumberToObject(sr_pivot, "obv", last_pivot->obv);
        cJSON_AddItemToArray(sr_pivots, sr_pivot);
        cJSON* info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "sr", sr_price);
        cJSON_AddNumberToObject(info, "vr", sr_volume_ratio);
        cJSON_AddNumberToObject(info, "num_sr", num_sr_pivots);
        jl_pivot_ptr srp0 = pivots + sr_pivot_indices[0];
        cJSON_AddNumberToObject(info, "length",
                                cal_num_busdays(srp0->date, last_pivot->date));
        cJSON_AddItemToObject(info, "sr_pivots", sr_pivots);
        stp_add_to_setups(setups, jl, "JL_SR", dir, info, true);
    }
 end:
    jl_free_pivots(pivs);
}


/**
This is the final objective of stp_add_jl_pullback_setup
{
    "cp": 2800,
    "length": 100,
    "state": "NRa",
    "channel": {
        "bound": "lower",
        "p1": {
            "dt": "2021-01-04",
            "st": "NRe",
            "px": 2800,
            "obv": 1.34
        },
        "p2": {
            "dt": "2021-01-04",
            "st": "NRe",
            "px": 2800,
            "obv": 1.34
        }
    },
    "pivot": {
        "dt": "2021-01-04",
        "st": "NRe",
        "px": 2800,
        "obv": 1.34
    }
}

*/

void stp_add_jl_pullback_setup(cJSON *setups, jl_data_ptr jl,
                               jl_channel_ptr channel, int direction,
                               jl_piv_ptr pivs_050) {
    jl_channel_boundary_ptr cb = 
        (abs(direction) > 1)? &(channel->ub): &(channel->lb);
    jl_pivot_ptr pivot = &(pivs_050->pivots[pivs_050->num - 2]);
    char *bound = (abs(direction) > 1)? "upper": "lower";
    cJSON *p1 = cJSON_CreateObject();
    cJSON_AddStringToObject(p1, "date", jl->data->data[jl->pos - cb->d1].date);
    cJSON_AddStringToObject(p1, "state", jl_state_to_string(cb->s1));
    cJSON_AddNumberToObject(p1, "price", cb->px1);
    cJSON_AddNumberToObject(p1, "obv", cb->obv1);
    cJSON *p2 = cJSON_CreateObject();
    cJSON_AddStringToObject(p2, "date", jl->data->data[jl->pos - cb->d2].date);
    cJSON_AddStringToObject(p2, "state", jl_state_to_string(cb->s2));
    cJSON_AddNumberToObject(p2, "price", cb->px2);
    cJSON_AddNumberToObject(p2, "obv", cb->obv2);
    cJSON* piv = cJSON_CreateObject();
    cJSON_AddStringToObject(piv, "date", pivot->date);
    cJSON_AddStringToObject(piv, "state", jl_state_to_string(pivot->state));
    cJSON_AddNumberToObject(piv, "price", pivot->price);
    cJSON_AddNumberToObject(piv, "obv", pivot->obv);    
    cJSON *chan = cJSON_CreateObject();
    cJSON_AddStringToObject(chan, "bound", bound);
    cJSON_AddItemToObject(chan, "p1", p1);
    cJSON_AddItemToObject(chan, "p2", p2);
    cJSON *info = cJSON_CreateObject();
    cJSON_AddItemToObject(info, "pivot", piv);
    cJSON_AddItemToObject(info, "channel", chan);
    stp_add_to_setups(setups, jl, "JL_P", direction / abs(direction),
                      info, true);
}

/**
 *  Check whether a 0.5 pivot bounces off a longer channel trend.
 *  given day creates a new pivot point, and determine
 * if that pivot point represents a change in trend.

 * TODO: only return this setup if it is formed of a
 * support/resistance structure: either bounces of the lower part of
 * an upward channel, or bounces of the upper part of a downward
 * channel.
 */
void stp_jl_pullbacks(cJSON *setups, jl_data_ptr jl_050, jl_data_ptr jl_100,
                      jl_data_ptr jl_150, jl_data_ptr jl_200) {
    /**
     *  Return if the current record is not a primary record, or it is
     *  not the first record in a new trend
     */
    if (!jl_first_new_trend(jl_050))
        return;
    jl_piv_ptr pivs_050 = NULL, pivs_100 = NULL, pivs_150 = NULL,
        pivs_200 = NULL;
    /**
     *  Get last 4 pivots and the last non-secondary record for
     *  factors 1.5 and 2.0. Exit if there are less than 4 pivots.
     */
    pivs_150 = jl_get_pivots(jl_150, 4);
    pivs_200 = jl_get_pivots(jl_200, 4);
    if ((pivs_150->num < 5) || (pivs_200->num < 5))
        goto end;
    /**
     *  Find out how many 0.5/1.0 pivots are after the last two pivots
     *  for factors 1.5 and 2.0.  If there are 4 pivots or more, use
     *  those pivots.  Otherwise, get the last 4 0.5/1.0 pivots.
     */
    char *lrdt_150 = pivs_150->pivots[pivs_150->num - 3].date,
        *lrdt_200 = pivs_200->pivots[pivs_200->num - 3].date;
    char *lrdt = (strcmp(lrdt_150, lrdt_200) >= 0)? lrdt_200: lrdt_150;
    pivs_100 = jl_get_pivots_date(jl_100, lrdt);
    if (pivs_100->num < 5) {
        jl_free_pivots(pivs_100);
        pivs_100 = jl_get_pivots(jl_100, 4);
    }
    pivs_050 = jl_get_pivots_date(jl_050, lrdt);
    if (pivs_050->num < 5) {
        jl_free_pivots(pivs_050);
        pivs_050 = jl_get_pivots(jl_050, 4);
    }

    /**
     *  TODO: 
     *  1. Implement jl_pivot_bounce_channel() function
     *  2. Fix ana_add_jl_pullback_setup() and rename it to stp_...
     *  3. Define hybrid channels that:
     *     a. start from the last long term pivot
     *     b. get all short term pivots after last long term pivot
     *     c. if less than 6 short term pivots, no hybrid channel
     *     d. connect long term pivot with -3, -5, ... short term pivots
     *     e. get the smallest slope, thats the hybrid channel
     *     f. ...
     *  4. run jl_pivot_bounce_channel() for hybrid channels
     */
    jl_channel channel_100, channel_150, channel_200;
    jl_get_channel(jl_100, &channel_100);
    jl_get_channel(jl_150, &channel_150);
    jl_get_channel(jl_200, &channel_200);

    jl_pivot_ptr last_piv_050 = &(pivs_050->pivots[pivs_050->num - 2]);
    int direction = jl_pivot_bounce_channel(last_piv_050, &channel_100);
    if (direction != 0)
        stp_add_jl_pullback_setup(setups, jl_100, &channel_100, direction,
                                  pivs_050);
    direction = jl_pivot_bounce_channel(last_piv_050, &channel_150);
    if (direction != 0)
        stp_add_jl_pullback_setup(setups, jl_150, &channel_150, direction,
                                  pivs_050);
    direction = jl_pivot_bounce_channel(last_piv_050, &channel_200);
    if (direction != 0)
        stp_add_jl_pullback_setup(setups, jl_200, &channel_200, direction,
                                  pivs_050);
 end:
    jl_free_pivots(pivs_050);
    jl_free_pivots(pivs_100);
    jl_free_pivots(pivs_150);
    jl_free_pivots(pivs_200);
}


/**
 *  The following candlestick patterns are implemented:
 *  - hammer
 *  - engulfing
 *  - piercing/dark cloud cover/kicking
 *  - three white soldiers / black crows
 *   -star
 *  - CBS
 *  - engulfing harami
 */
void stp_candlesticks(cJSON *setups, jl_data_ptr jl) {
    daily_record_ptr r[6];
    cJSON *info = cJSON_CreateObject();
    int ix_0 = jl->data->pos - 1;
    for(int ix = 0; ix < 6; ix++)
        r[ix] = &(jl->data->data[ix_0 - ix]);
    int body[6], max_oc[6], min_oc[6];
    for(int ix = 0; ix < 6; ix++) {
        body[ix] = r[ix]->close - r[ix]->open;
        max_oc[ix] = MAX(r[ix]->open, r[ix]->close);
        min_oc[ix] = MIN(r[ix]->open, r[ix]->close);
    }
    int marubozu[6], engulfing[2], harami[5], piercing = 0, star = 0, cbs = 0;
    int three = 0, three_in = 0, three_out = 0, kicking = 0, eng_harami = 0;
    /**
     *  Calculate marubozu and harami patterns for the last 6 (5)
     *  days
     */
    for(int ix = 0; ix < 6; ix++) {
        /** Handle the case when open == high == low == close */
        int h_l = r[ix]->high - r[ix]->low;
        if (h_l == 0)
            h_l = 1;
        int ratio = 100 * body[ix] / h_l;
        marubozu[ix] = (abs(ratio) < CANDLESTICK_MARUBOZU_RATIO)? 0: ratio;
        if (ix >= 5)
            continue;
        if ((100 * abs(body[ix + 1]) > jl->recs[ix_0 - ix - 2].rg *
             CANDLESTICK_LONG_DAY_AVG_RATIO) &&
            (100 * body[ix] <= CANDLESTICK_HARAMI_RATIO * body[ix + 1]) &&
            (max_oc[ix] < max_oc[ix + 1]) && (min_oc[ix] > min_oc[ix + 1]))
            harami[ix] = 1;
        else
            harami[ix] = 0;
    }
    /**
     *  Calculate engulfing pattern for the last two days 
     */
    for(int ix = 0; ix < 2; ix++) {
        if ((body[ix] * body[ix + 1] < 0) &&
            (abs(body[ix]) > abs(body[ix + 1])) &&
            (max_oc[ix] >= max_oc[ix + 1]) &&
            (min_oc[ix] <= min_oc[ix + 1]))
            engulfing[ix] = (body[ix] > 0)? 1: -1;
        else
            engulfing[ix] = 0;
    }
    /**
     *  Calculate piercing pattern
     */
    if ((body[0] * body[1] < 0) &&
        (100 * abs(body[1]) > jl->recs[ix_0 - 2].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO) &&
        (100 * abs(body[0]) > jl->recs[ix_0 - 1].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO)) {
        if ((body[0] > 0) && (r[0]->open < r[1]->low) &&
            (2 * r[0]->close > (r[1]->low + r[1]->high)))
            piercing = 1;
        if ((body[0] < 0) && (r[0]->open > r[1]->high) &&
            (2 * r[0]->close < (r[1]->low + r[1]->high)))
            piercing = -1;
    }
    /**
     *  Check for star patterns. Rules of Recognition
     *  1. First day always the color established by ensuing trend
     *  2. Second day always gapped from body of first day. Color not
     *     important.
     *  3. Third day always opposite color of first day.
     *  4. First day, maybe the third day, are long days.
     *
     *  If third day closes deeply (more than halfway) into first day
     *  body, a much stronger move should ensue, especially if heavy
     *  volume occurs on third day.
     */
    if ((100 * abs(body[2]) > jl->recs[ix_0 - 3].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO) && (body[0] * body[2] < 0) &&
        (100 * abs(body[1]) < jl->recs[ix_0 - 2].rg *
         CANDLESTICK_SHORT_DAY_AVG_RATIO)) {
        int max_r1oc = MAX(r[1]->open, r[1]->close);
        int min_r1oc = MIN(r[1]->open, r[1]->close);
        if ((body[2] < 0) && (max_r1oc < r[2]->close) &&
            (max_r1oc < r[0]->open) &&
            (2 * r[0]->close > r[2]->open + r[2]->close))
            star = 1;
        if ((body[2] > 0) && (min_r1oc > r[2]->close) &&
            (min_r1oc > r[0]->open) &&
            (2 * r[0]->close < r[2]->open + r[2]->close))
            star = -1;
    }
    /**
     *  Three (white soldiers / black crows).  
     *  1. Three consecutive long white days occur, each with a higher
     *     close.  
     *  2. Each day opens within the body of the previous day.
     *  3. Each day closes at or near its high.
     *
     *  1. Three consecutive long black days occur, each with a lower
     *     close.  
     *  2. Each day opens within the body of the previous day.
     *  3. Each day closes at or near its lows.
     */
    if (((body[2] > 0 && body[1] > 0 && body[0] > 0) ||
         (body[2] < 0 && body[1] < 0 && body[0] < 0)) &&
        (100 * abs(body[2]) > jl->recs[ix_0 - 3].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO) &&
        (100 * abs(body[1]) > jl->recs[ix_0 - 2].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO) &&
        (100 * abs(body[0]) > jl->recs[ix_0 - 1].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO)) {
        if ((r[1]->close > r[0]->close) && (r[2]->close > r[1]->close) &&
            (r[1]->open <= r[0]->close) && (r[2]->open <= r[1]->close) &&
            (4 * r[0]->close > 3 * r[0]->high + r[0]->low) &&
            (4 * r[1]->close > 3 * r[1]->high + r[1]->low) &&
            (4 * r[2]->close > 3 * r[2]->high + r[2]->low))
            three = 1;
        if ((r[1]->close < r[0]->close) && (r[2]->close < r[1]->close) &&
            (r[1]->open >= r[0]->close) && (r[2]->open >= r[1]->close) &&
            (4 * r[0]->close < 3 * r[0]->low + r[0]->high) &&
            (4 * r[1]->close < 3 * r[1]->low + r[1]->high) &&
            (4 * r[2]->close < 3 * r[2]->low + r[2]->high))
            three = -1;
    }
    /**
     *  Kicking:
     *  1. A Marubozu of one color is followed by another of opposite
     *     color
     *  2. A gap must occur between the two lines.
     */
    if (marubozu[1] * marubozu[0] < 0) {
        if ((marubozu[0] > 0) && (r[0]->open > r[1]->open))
            kicking = 1;
        if ((marubozu[0] < 0) && (r[0]->open < r[1]->open))
            kicking = -1;
    }
    /**
     *  CBS:
     *  1. First two days are Black Marubozu
     *  2. Third day is black, gaps down at open, pierces previous day
     *     body.
     *  3. Fourth black day completely engulfs third day, including
     *     shadow.
     */
    if ((marubozu[3] < 0) && (marubozu[2] < 0) && (body[1] < 0) &&
        (r[1]->open < r[2]->close) && (body[0] < 0) &&
        (r[0]->open > r[1]->high) && (r[0]->close < r[1]->low))
        cbs = 1;
    /**
     *  3out: engulfing pattern, followed by a close in the direction
     *  of the trend
     */
    if ((engulfing[1] > 0) && (body[0] > 0) && (r[0]->close > r[1]->close))
        three_out = 1;
    if ((engulfing[1] < 0) && (body[0] < 0) && (r[0]->close < r[1]->close))
        three_out = -1;
    /**
     *  The bullish/bearish engulfing harami pattern consists of two
     *  combination patterns. The first is a harami pattern and the
     *  second is an engulfing pattern. The end result is a pattern
     *  whose two sides are white/black marubozu candlesticks.
     */
    int min_40o = MIN(r[4]->open, r[0]->open);
    int min_40c = MIN(r[4]->close, r[0]->close);
    int max_40o = MAX(r[4]->open, r[0]->open);
    int max_40c = MAX(r[4]->close, r[0]->close);
    if ((harami[2] == 1) && (body[3] > 0) && engulfing[0] == 1)
        eng_harami = 1;
    if ((harami[3] == 1) && (body[4] > 0) && (engulfing[0] == 1) &&
        (max_oc[2] < max_40c) && (min_oc[2] > min_40o))
        eng_harami = 1;
    if ((harami[2] == 1) && (body[3] < 0) && engulfing[0] == -1)
        eng_harami = -1;
    if ((harami[3] == 1) && (body[4] < 0) && (engulfing[0] == -1) &&
        (max_oc[2] < max_40o) && (min_oc[2] > min_40c))
        eng_harami = -1;
    /**
     *  Add all the computed candlestick setups to the setups array
     */
    if (engulfing[1] != 0)
        stp_add_to_setups(setups, NULL, "Engulfing", engulfing[0], NULL, true);
    if (piercing != 0)
        stp_add_to_setups(setups, NULL, "Piercing", piercing, NULL, true);
    if (star != 0)
        stp_add_to_setups(setups, NULL, "Star", star, NULL, true);
    if (cbs != 0)
        stp_add_to_setups(setups, NULL, "Cbs", cbs, NULL, true);
    if (three != 0)
        stp_add_to_setups(setups, NULL, "3", three, NULL, true);
    if (three_in != 0)
        stp_add_to_setups(setups, NULL, "3in", three_in, NULL, true);
    if (three_out != 0)
        stp_add_to_setups(setups, NULL, "3out", three_out, NULL, true);
    if (kicking != 0)
        stp_add_to_setups(setups, NULL, "Kicking", kicking, NULL, true);
    if (eng_harami != 0)
        stp_add_to_setups(setups, NULL, "EngHarami", eng_harami, NULL, true);
}

void stp_daily_setups(cJSON *setups, jl_data_ptr jl) {
    daily_record_ptr r[2];
    jl_record_ptr jlr[2];
    int ix_0 = jl->data->pos;
    for(int ix = 0; ix < 2; ix++) {
        r[ix] = &(jl->data->data[ix_0 - ix]);
        jlr[ix] = &(jl->recs[ix_0 - ix]);
    }
    int jlr_1_volume = (jlr[1]->volume == 0)? 1: jlr[1]->volume;
    int jlr_1_rg = (jlr[1]->rg == 0)? 1: jlr[1]->rg;
    char *stk = jl->data->stk, *dt = r[0]->date;
    LOGINFO("ana_daily_setups(): stk = %s, dt = %s\n", stk, dt);
    /**
     *  Find strong closes up or down; rr/vr capture range/volume
     *  significance
     *  TODO: add marubozu info
     */
    int sc_dir = ts_strong_close(r[0]);
    if (sc_dir != 0) {
        int rr = 100 * ts_true_range(jl->data, ix_0) / jlr_1_rg;
        if (((sc_dir == -1) &&
             (r[0]->close > ts_weighted_price(jl->data, ix_0 - 1))) ||
            ((sc_dir == 1) &&
             (r[0]->close < ts_weighted_price(jl->data, ix_0 - 1))))
            rr = 0;
        cJSON *info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "vr",
                                100 * r[0]->volume / jlr_1_volume);
        cJSON_AddNumberToObject(info, "rr", rr);
        stp_add_to_setups(setups, NULL, "SC", sc_dir, info, true);
    }
    /**
     *  Find gaps.  TODO: Add gaps for which the open is higher than
     *  the close.  Add in the info whether the gap was closed during
     *  the day.
     */
    int gap_dir = 0;
    if (r[0]->open > r[1]->high)
        gap_dir = 1;
    if (r[0]->open < r[1]->low)
        gap_dir = -1;
    if (gap_dir != 0) {
        int drawdown = 0;
        if (gap_dir == 1)
            drawdown = (r[0]->close - r[0]->high);
        else
            drawdown = (r[0]->close - r[0]->low);
        cJSON *info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "vr",
                                100 * r[0]->volume / jlr_1_volume);
        cJSON_AddNumberToObject(info, "eod_gain",
                                100 * (r[0]->close - r[1]->close) /
                                jlr_1_rg);
        cJSON_AddNumberToObject(info, "drawdown", 100 * drawdown / jlr_1_rg);
        stp_add_to_setups(setups, NULL, "Gap", gap_dir, info, true);
    }
    /**
     *  Find reversal days: TODO - document the definition of a
     *  reversal day
     */
    int rd_dir = 0, min_oc = MIN(r[0]->open, r[1]->close);
    int max_oc = MAX(r[0]->open, r[1]->close);
    if ((r[0]->low < r[1]->low) && (r[0]->low < min_oc - jlr[1]->rg) &&
        (r[0]->close > r[0]->open) && (sc_dir == 1))
        rd_dir = 1;
    if ((r[0]->high > r[1]->high) && (r[0]->high > max_oc + jlr[1]->rg) &&
        (r[0]->close < r[0]->open) && (sc_dir == -1))
        rd_dir = -1;
    if (rd_dir != 0) {
        cJSON *info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "vr",
                                100 * r[0]->volume / jlr_1_volume);
        int rd_drawdown = (rd_dir == 1)? (min_oc - r[0]->low):
            (r[0]->high - max_oc);
        cJSON_AddNumberToObject(info, "rd_drawdown",
                                100 * rd_drawdown / jlr_1_rg);
        cJSON_AddNumberToObject(info, "rd_gain",
                                100 * (r[0]->close - r[0]->open) / jlr_1_rg);
        stp_add_to_setups(setups, NULL, "RDay", rd_dir, info, true);
    }
}

void stp_insert_setups_in_database(cJSON *setups, char *dt, char *stk) {
    int num_setups = cJSON_GetArraySize(setups);
    if (num_setups > 0) {
        LOGINFO("Inserting %d setups for %s on %s\n", num_setups, stk, dt);
        cJSON* setup;
        cJSON_ArrayForEach(setup, setups) {
            cJSON *info = cJSON_GetObjectItem(setup, "info");
            char *info_string = (info != NULL)? cJSON_Print(info): "{}";
            char sql_cmd[2048];
            sprintf(sql_cmd, "insert into jl_setups values ('%s','%s','%s',%d,"
                    "'%s',%s,%d,'%s')", dt, stk,
                    cJSON_GetObjectItem(setup, "setup")->valuestring,
                    cJSON_GetObjectItem(setup, "factor")->valueint,
                    cJSON_GetObjectItem(setup, "direction")->valuestring,
                    cJSON_GetObjectItem(setup, "triggered")->valuestring,
                    cJSON_GetObjectItem(setup, "score")->valueint,
                    info_string);
            db_transaction(sql_cmd);
        }
    }
}

#endif
