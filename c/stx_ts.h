#ifndef __STX_TS_H__
#define __STX_TS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <time.h>
#include "stx_core.h"

/** This defines a strong close, if (1+SC)*c>=SC*h+l, or (1+SC)*c<=h+SC*l*/
#define SC 4

/** BEGIN: macros */
#define module( x) (( x>  0)? x: -x)
#define sign( x) (( x> 0)? 1: -1)
/** END: macros */

typedef struct chart_record_t {
    char date[20];
    int open;
    int high;
    int low;
    int close;
    int volume;
    int sma_50;
    int sma_200;
} chart_record, *chart_record_ptr;

int ts_true_range(stx_data_ptr data, int ix) {
    int res = data->data[ix].high - data->data[ix].low;
    if(ix == 0) 
        return res;
    if(res < data->data[ix].high - data->data[ix - 1].close)
        res = data->data[ix].high - data->data[ix - 1].close;
    if(res < data->data[ix - 1].close - data->data[ix].low)
        res = data->data[ix - 1].close - data->data[ix].low;
    return res;
}

int ts_strong_close(ohlcv_record_ptr dr) {
    int sc_dir = 0;
    if (dr->high == dr->low)
        return sc_dir;
    if ((SC + 1) * dr->close >= SC * dr->high + dr->low)
        sc_dir = 1;
    if ((SC + 1) * dr->close <= dr->high + SC * dr->low)
        sc_dir = -1;
    return sc_dir;
}

int ts_gap(stx_data_ptr data, int ix) {
    if (ix < 1)
        return 0;
    ohlcv_record_ptr r = data->data + ix, r_1 = data->data + ix - 1;
    if (r->open == r_1->close)
        return 0;
    if (r->open > r_1->high)
        return 2;
    if (r->open > r_1->close)
        return 1;
    if (r->open < r_1->low)
        return -2;
    if (r->open < r_1->close)
        return -1;
    return 0;
}

int ts_weighted_price(stx_data_ptr data, int ix) {
    return (data->data[ix].high + data->data[ix].low + data->data[ix].close)
        / 3;
}

void ts_load_splits(stx_data_ptr data) {
    char sql_cmd[256], start_date[20], end_date[20];
    strcpy(start_date, data->data[0].date);
    strcpy(end_date, data->data[data->num_recs - 1].date);
    if (data->intraday) {
        *(strchr(start_date, ' ')) = '\0';
        *(strchr(end_date, ' ')) = '\0';
    }
    sprintf(sql_cmd, "SELECT ratio, dt FROM dividends WHERE stk='%s' AND "
            "dt BETWEEN '%s' AND '%s' ORDER BY dt", data->stk, start_date,
            end_date);
    PGresult *res = db_query(sql_cmd);
#ifdef DEBUG
    LOGDEBUG("Found %d splits for %s between %s and %s\n",
             PQntuples(res), data->stk, start_date, end_date);
#endif
    data->splits = ht_divis(res);
#ifdef DEBUG
    LOGDEBUG("Loaded splits for %s between %s and %s\n", data->stk, start_date,
             end_date);
#endif
    PQclear(res);
}

stx_data_ptr ts_load_eod_stk(char *stk, char *end_dt, int num_days) {
#ifdef DEBUG
    LOGDEBUG("Loading EOD data for %s\n", stk);
#endif
    stx_data_ptr data = (stx_data_ptr) calloc((size_t)1, sizeof(stx_data));
    data->data = NULL;
    data->num_recs = 0;
    data->pos = 0;
    data->last_adj = -1;
    data->intraday = 0;
    char sql_cmd[128], start_dt_sql[32];
    memset(sql_cmd, 0, 128 * sizeof(char));
    memset(start_dt_sql, 0, 32 * sizeof(char));
    sprintf(sql_cmd, "SELECT o, hi, lo, c, v, dt FROM eods WHERE stk='%s'",
            stk);
    if (end_dt != NULL) {
        char end_dt_sql[32];
        memset(end_dt_sql, 0, 32 * sizeof(char));
        sprintf(end_dt_sql, " AND dt<='%s'", end_dt);
        strcat(sql_cmd, end_dt_sql);
    }
    if (num_days > 0) {
        char *start_dt = NULL;
        if (end_dt == NULL)
            end_dt = cal_current_trading_date();
        cal_move_bdays(end_dt, -num_days, &start_dt);
        sprintf(start_dt_sql, " AND dt>='%s'", start_dt);
    } else
        sprintf(start_dt_sql, " AND dt>='%s'", "1985-01-01");
    strcat(sql_cmd, start_dt_sql);
    strcat(sql_cmd, " ORDER BY dt");
    PGresult *res = db_query(sql_cmd);
    if((data->num_recs = PQntuples(res)) <= 0) 
        return data;
    int num = data->num_recs;
    char sd[16], ed[16];
    strcpy(sd, PQgetvalue(res, 0, 5));
    strcpy(ed, PQgetvalue(res, num - 1, 5));
    int b_days = cal_num_busdays(sd, ed);
    data->num_recs = b_days;
#ifdef DEBUG
    LOGDEBUG("Found %d records for %s\n", num, stk);
#endif
    data->data = (ohlcv_record_ptr) calloc(b_days, sizeof(ohlcv_record));
    int ts_idx = 0;
    int calix = cal_ix(sd);
    char* dt;
    calix = cal_prev_bday(calix, &dt);
    for(int ix = 0; ix < num; ix++) {
        calix = cal_next_bday(calix, &dt);
        char* db_date = PQgetvalue(res, ix, 5);
        if (strcmp(dt, db_date) > 0) {
            LOGERROR("%s: Something is very wrong: dt = %s, db_date = %s\n",
                     stk, dt, db_date);
            return NULL;
        }
        while (strcmp(dt, db_date) < 0) {
#ifdef DEBUG
            LOGDEBUG("Adding data for %s, not found in %s data\n", dt, stk);
#endif
            data->data[ts_idx].open = data->data[ts_idx - 1].close;
            data->data[ts_idx].high = data->data[ts_idx - 1].close;
            data->data[ts_idx].low = data->data[ts_idx - 1].close;
            data->data[ts_idx].close = data->data[ts_idx - 1].close;
            data->data[ts_idx].volume = 0;
            strcpy(data->data[ts_idx].date, dt); 
            calix = cal_next_bday(calix, &dt);
            ts_idx++;
        }   
        data->data[ts_idx].open = atoi(PQgetvalue(res, ix, 0));
        data->data[ts_idx].high = atoi(PQgetvalue(res, ix, 1));
        data->data[ts_idx].low = atoi(PQgetvalue(res, ix, 2));
        data->data[ts_idx].close = atoi(PQgetvalue(res, ix, 3));
        data->data[ts_idx].volume = atoi(PQgetvalue(res, ix, 4));
        strcpy(data->data[ts_idx].date, PQgetvalue(res, ix, 5)); 
        ts_idx++;
    }
    data->pos = 0;
    // data->pos = b_days - 1;
    PQclear(res);
#ifdef DEBUG
    LOGDEBUG("Loading the splits for %s\n", stk);
#endif
    strcpy(data->stk, stk);
    ts_load_splits(data);
#ifdef DEBUG
    LOGDEBUG("Done loading %s\n", stk);
#endif
    return data;
}

/**
 *  Get the intraday data for a stock.  If end_dt is NULL, get data
 *  until the current trading time.  If num_days is zero or negative,
 *  use a default value of 20.  Start date is num_days business days
 *  before end date.  TODO: maintain a hashtable with intraday records
 *  for stocks.  If the stock is not found in hash table, invoke this
 *  function.  Otherwise, call another function that gets intraday
 *  data, updates database and the intraday data structure.  The
 *  intraday hash table should be cleared at the beginning of each
 *  day.  Then, for each intraday stock, allocate 78 * (num business
 *  days between start and end dates).  The last 78 records will get
 *  populated during the trading day.
 */
stx_data_ptr ts_load_id_stk(char *stk, char *end_dt, int num_days) {
#ifdef DEBUG
    LOGDEBUG("Loading intraday data for %s\n", stk);
#endif
    bool realtime = false;
    char *current_trading_date = cal_current_trading_date();
    stx_data_ptr data = (stx_data_ptr) calloc((size_t)1, sizeof(stx_data));
    data->data = NULL;
    data->num_recs = 0;
    data->pos = 0;
    data->last_adj = -1;
    data->intraday = 1;
    char sql_cmd[128], start_dt_sql[32];
    memset(sql_cmd, 0, 128 * sizeof(char));
    memset(start_dt_sql, 0, 32 * sizeof(char));
    sprintf(sql_cmd, "SELECT o, hi, lo, c, v, dt FROM intraday WHERE stk='%s'",
            stk);
    if (end_dt != NULL) {
        char end_dt_sql[32];
        memset(end_dt_sql, 0, 32 * sizeof(char));
        sprintf(end_dt_sql, " AND DATE(dt)<='%s'", end_dt);
        strcat(sql_cmd, end_dt_sql);
    }
    if (num_days <= 0)
        num_days = 20;
    char *start_dt = NULL;
    if ((end_dt == NULL) ||
        ((end_dt != NULL) && (strcmp(end_dt, current_trading_date) > 0)))
        end_dt = current_trading_date;
    cal_move_bdays(end_dt, -num_days, &start_dt);
    sprintf(start_dt_sql, " AND DATE(dt)>='%s'", start_dt);
    strcat(sql_cmd, start_dt_sql);
    strcat(sql_cmd, " ORDER BY dt");
    PGresult *res = db_query(sql_cmd);
    int num_db_recs = PQntuples(res);
#ifdef DEBUG
    LOGDEBUG("Found %d records for %s\n", num_db_recs, stk);
#endif
    if(num_db_recs <= 0) 
        return data;
    char sd[20], ed[20], db_sd[20], db_ed[20];
    memset(sd, 0, 20 * sizeof(char));
    memset(ed, 0, 20 * sizeof(char));
    memset(db_sd, 0, 20 * sizeof(char));
    memset(db_ed, 0, 20 * sizeof(char));
    strcpy(sd, start_dt);
    strcpy(db_sd, PQgetvalue(res, 0, 5));
    strcpy(ed, end_dt);
    strcpy(db_ed, PQgetvalue(res, num_db_recs - 1, 5));
    /* *(strchr(sd, ' ')) = '\0'; */
    /* *(strchr(ed, ' ')) = '\0'; */
    int num_recs = 78 * cal_num_busdays(sd, ed);
    data->num_recs = num_recs;
    data->data = (ohlcv_record_ptr) calloc(num_recs, sizeof(ohlcv_record));
    strcat(sd, " 09:30:00");
    strcat(ed, " 15:55:00");
    int ts_idx = 0;
    int o_db = atoi(PQgetvalue(res, 0, 0));
    while(strcmp(db_sd, sd) > 0) {
        data->data[ts_idx].open = o_db;
        data->data[ts_idx].high = o_db;
        data->data[ts_idx].low = o_db;
        data->data[ts_idx].close = o_db;
        data->data[ts_idx].volume = 0;
        strcpy(data->data[ts_idx].date, sd);
        cal_move_5mins(sd, 1);
        ts_idx++;
    }
    for (int db_ix = 0; db_ix < num_db_recs; db_ix++) {
        strcpy(db_sd, PQgetvalue(res, db_ix, 5));
        while(strcmp(db_sd, sd) > 0) {
            int prev_close = data->data[ts_idx - 1].close;
            data->data[ts_idx].open = prev_close;
            data->data[ts_idx].high = prev_close;
            data->data[ts_idx].low = prev_close;
            data->data[ts_idx].close = prev_close;
            data->data[ts_idx].volume = 0;
            strcpy(data->data[ts_idx].date, sd);
            cal_move_5mins(sd, 1);
            ts_idx++;
        }
        data->data[ts_idx].open = atoi(PQgetvalue(res, db_ix, 0));
        data->data[ts_idx].high = atoi(PQgetvalue(res, db_ix, 1));
        data->data[ts_idx].low = atoi(PQgetvalue(res, db_ix, 2));
        data->data[ts_idx].close = atoi(PQgetvalue(res, db_ix, 3));
        data->data[ts_idx].volume = atoi(PQgetvalue(res, db_ix, 4));
        strcpy(data->data[ts_idx].date, PQgetvalue(res, db_ix, 5));
        /**
         *  "Fix" the problem of massive volumes at 15:55:00
         */
        if (ts_idx % 78 == 77)
            data->data[ts_idx].volume = 0.6 * (data->data[ts_idx - 1].volume +
                data->data[ts_idx - 2].volume);
        cal_move_5mins(sd, 1);
        ts_idx++;
    }
    int last_close = atoi(PQgetvalue(res, num_db_recs - 1, 3));
    while(strcmp(sd, ed) <= 0) {
        data->data[ts_idx].open = last_close;
        data->data[ts_idx].high = last_close;
        data->data[ts_idx].low = last_close;
        data->data[ts_idx].close = last_close;
        data->data[ts_idx].volume = 0;
        strcpy(data->data[ts_idx].date, sd);
        cal_move_5mins(sd, 1);
        ts_idx++;
    }
    PQclear(res);
#ifdef DEBUG
    LOGDEBUG("Loading the splits for %s\n", stk);
#endif
    strcpy(data->stk, stk);
    ts_load_splits(data);
#ifdef DEBUG
    LOGDEBUG("Done loading %s\n", stk);
#endif
    return data;
}

/**
 *  Load the EOD ('intraday' is false) or intraday ('intraday' is
 *  true) data for a stock.  The 'dt' argument specifies the end
 *  datetime up to which data should be loaded.  If 'dt' is NULL, load
 *  the data until current date time.  The 'num_days' parameter
 *  specifies for how many days prior to 'dt' we should retrieve the
 *  data.  If 'num_days' is not positive, then load all the data
 *  available for that stock.  Calling this function with 'dt' set to
 *  NULL during the trading hours will trigger the realtime logic,
 *  where the code will attempt to download any missing data between
 *  the date time of the last upload and the current datetime.
 */
stx_data_ptr ts_load_stk(char *stk, char *dt, int num_days, bool intraday) {
    stx_data_ptr data = NULL;
    char load_date[20], *hhmm = NULL;
    strcpy(load_date, dt);
    hhmm = strchr(load_date, ' ');
    if (hhmm != NULL)
        *hhmm = '\0';
    if (!intraday)
        data = ts_load_eod_stk(stk, load_date, num_days);
    else
        data = ts_load_id_stk(stk, load_date, num_days);
    return data;
}

/**
 * Find the index corresponding to a datetime.  For non-intraday,
 * rel_pos is a parameter that can take the following values:
 *  0 - do an exact search
 *  1 - return date, or next business day, if date not found
 * -1 - return date, or previous business day, if date not found
 **/
int ts_find_date_record(stx_data_ptr data, char* dt, int rel_pos) {
    char* first_dt = data->data[0].date;
    if (data->intraday == 1) {
        int ix = cal_5min_ticks(first_dt, dt) - 1;
        return ix;
    }
    char crt_dt[20], *hhmm = NULL;
    memset(crt_dt, 0, 20 * sizeof(char));
    strcpy(crt_dt, dt);
    hhmm = strchr(crt_dt, ' ');
    if (hhmm != NULL)
        *hhmm++ = '\0';
    int n = cal_num_busdays(first_dt, crt_dt) - 1;
    if (n < 0) {
        if (rel_pos > 0)
            return 0;
    } else if (n >= data->num_recs) {
        if (rel_pos < 0)
            return data->num_recs - 1;
    } else {
        if (strcmp(data->data[n].date, crt_dt) == 0)
            return n;
        else {
            if (rel_pos < 0)
                return n - 1;
            if (rel_pos > 0)
                return n;
        }
    }
    return -1;
}

void ts_adjust_data(stx_data_ptr data, int split_ix) {
    if (split_ix < 0)
        return;
    for(int ix = data->last_adj + 1; ix <= split_ix; ix++) {
        char date[32];
        memset(date, 0, 32 * sizeof(char));
        if (data->intraday)
            sprintf(date, "%s 15:55:00", data->splits->list[ix].key);
        else
            strcpy(date, data->splits->list[ix].key);
        float ratio = data->splits->list[ix].val.ratio;
        /* find the index for the date in the data->data */
        /* adjust the data up to, and including that index */
        int end = ts_find_date_record(data, date, 0);
        for(int ixx = 0; ixx <= end; ++ixx) {
            data->data[ixx].open = (int)(ratio * data->data[ixx].open);
            data->data[ixx].high = (int)(ratio * data->data[ixx].high);
            data->data[ixx].low = (int)(ratio * data->data[ixx].low);
            data->data[ixx].close = (int)(ratio * data->data[ixx].close);
            data->data[ixx].volume = (int)(data->data[ixx].volume / ratio);
        }
    }
    data->last_adj = split_ix;
}

void ts_set_day(stx_data_ptr data, char* date, int rel_pos) {
    if (!strcmp(data->data[data->pos].date, date))
        return;
    data->pos = ts_find_date_record(data, date, rel_pos);
    if (data->pos == -1) {
        LOGERROR("Could not set date to %s for %s\n", date, data->stk);
        return;
    }
    char split_date[20];
    strcpy(split_date, date);
    if (data->intraday) {
        char *hhmm = strchr(split_date, ' ');
        if (hhmm != NULL)
            *hhmm = '\0';
    }
    int split_ix = ht_seq_index(data->splits, split_date);
    if (split_ix >= 0)
        ts_adjust_data(data, split_ix);
}

int ts_next(stx_data_ptr data) {
    if (data->pos >= data->num_recs - 1)
        return -1;
    data->pos++;
    ht_item_ptr split = ht_get(data->splits, data->data[data->pos].date);
    if (split != NULL)
        ts_adjust_data(data, ht_seq_index(data->splits, 
                                          data->data[data->pos].date));
    return 0;
}

int ts_advance(stx_data_ptr data, char* end_date) {
    int res = 0, num_days = 0;
    while ((strcmp(data->data[data->pos].date, end_date) <= 0) &&
           (res != -1)) {
        res = ts_next(data);
        num_days++;
    }
    return num_days;
}

void ts_free_data(stx_data_ptr data) {
    ht_free(data->splits);
    free(data->data);
    data->data = NULL;
    free(data);
    data = NULL;
}

void ts_print_record(ohlcv_record_ptr record) {
    fprintf(stderr, "%s %7d %7d %7d %7d %7d\n", record->date, record->open, 
            record->high, record->low, record->close, record->volume);
}

void ts_print(stx_data_ptr data, int num_recs) {
    LOGINFO("Data for stock %s\n", data->stk);
    fprintf(stderr, "%s %s has %d records, current record: %d, last adj: %d\n",
            data->stk, (data->intraday? "intraday": "eod"), data->num_recs,
            data->pos, data->last_adj);
    fprintf(stderr, "Splits: \n");
    ht_print(data->splits);
    fprintf(stderr, "First %d, last %d OHLCV records:\n", num_recs, num_recs);
    if (data->pos < 2 * num_recs) {
        for (int ix = 0; ix < data->pos; ix++)
            ts_print_record(data->data + ix);
    } else {
        for (int ix = 0; ix < num_recs; ix++)
            ts_print_record(data->data + ix);
        fprintf(stderr, ".  .  .\n");
        for (int ix = data->pos - num_recs + 1; ix <= data->pos; ix++)
            ts_print_record(data->data + ix);
    }
}

/** Return daily records for a stock.  Use static stx hashtable,
 *  accessed through the ht_data() method.
 */
stx_data_ptr ts_get_ts(char *stk, char* dt, int rel_pos) {
    ht_item_ptr data_ht = ht_get(ht_data(), stk);
    stx_data_ptr data = NULL;
    if (data_ht == NULL) {
        data = ts_load_stk(stk, NULL, 0, false);
        if (data == NULL)
            return data;
        ts_set_day(data, dt, rel_pos);
        data_ht = ht_new_data(stk, (void*)data);
        ht_insert(ht_data(), data_ht);
    } else {
        data = (stx_data_ptr) data_ht->val.data;
        ts_set_day(data, dt, rel_pos);
    }
    return data;
}

void ts_eod_intraday_update(stx_data_ptr eod_data, stx_data_ptr id_data) {
    int id_end = id_data->pos, crs = id_end - id_end % 78;
    ohlcv_record_ptr id = id_data->data + crs;
    ohlcv_record_ptr last_eod = eod_data->data + eod_data->pos;
    last_eod->open = id->open;
    last_eod->high = id->high;
    last_eod->low = id->low;
    last_eod->close = id->close;
    last_eod->volume = id->volume / 1000;
    crs++;
    while (crs <= id_end) {
        id = id_data->data + crs;
        if (id->high > last_eod->high)
            last_eod->high = id->high;
        if (id->low < last_eod->low)
            last_eod->low = id->low;
        last_eod->volume += id->volume / 1000;
        last_eod->close = id->close;
        crs++;
    }
}

void ts_serialize_to_file(stx_data_ptr data, char *mkt_name, bool realtime) {
    char file_path[64];
    sprintf(file_path, "%s/stx/mkt/%s/%s/%s.dat", getenv("HOME"), mkt_name,
            (data->intraday == 0? "eod": "intraday"), data->stk);
    FILE *fp = fopen(file_path, "wb");
    chart_record_ptr chart = (chart_record_ptr) calloc((size_t)(data->pos + 1),
                                                       sizeof(chart_record));
    int avg_50 = 0, avg_200 = 0;
    for (int ix = 0; ix <= data->pos; ix++) {
        strcpy(chart[ix].date, data->data[ix].date);
        chart[ix].open = data->data[ix].open;
        chart[ix].high = data->data[ix].high;
        chart[ix].low = data->data[ix].low;
        chart[ix].close = data->data[ix].close;
        chart[ix].volume = data->data[ix].volume * 1000;
        if (ix < 49) {
            chart[ix].sma_50 = -1;
            avg_50 += chart[ix].close;
        } else if (ix == 49) {
            chart[ix].sma_50 = avg_50 / 50;
        } else {
            avg_50 += (chart[ix].close - chart[ix - 50].close);
            chart[ix].sma_50 = avg_50 / 50;
        }
        if (ix < 199) {
            chart[ix].sma_200 = -1;
            avg_200 += chart[ix].close;
        } else if (ix == 199) {
            chart[ix].sma_200 = avg_200 / 200;
        } else {
            avg_200 += (chart[ix].close - chart[ix - 200].close);
            chart[ix].sma_200 = avg_200 / 200;
        }
    }
    if (fwrite(chart, sizeof(chart_record), data->pos + 1, fp) <= 0) {
        LOGERROR("Failed to write data");
    }
    fclose(fp);
    free(chart);
    chart = NULL;
}
#endif
