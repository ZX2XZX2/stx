#ifndef __STX_CORE_H__
#define __STX_CORE_H__

#define _XOPEN_SOURCE
#include <libpq-fe.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>


/** LOGGING Used this: https://stackoverflow.com/questions/7411301/ **/
/* Get current time in format YYYY-MM-DD HH:MM:SS.mms */
char* crt_timestamp() {
    time_t seconds = time(NULL);
    static char _retval[24];
    strftime(_retval, 24, "%Y-%m-%d %H:%M:%S", localtime(&seconds));
    return _retval;
}

/* Remove path from filename */
#define __SHORT_FILE__ (strrchr(__FILE__, '/') ? \
                        strrchr(__FILE__, '/') + 1 : __FILE__)

/* Main log macro */
#define __LOG__(format, loglevel, ...) \
    fprintf(stderr, "%s %-5s [%s] [%s:%d] " format , crt_timestamp(),   \
           loglevel, __func__, __SHORT_FILE__, __LINE__, ## __VA_ARGS__)

/* Specific log macros with  */
#define LOGDEBUG(format, ...) __LOG__(format, "DEBUG", ## __VA_ARGS__)
#define LOGWARN(format, ...) __LOG__(format, "WARN", ## __VA_ARGS__)
#define LOGERROR(format, ...) __LOG__(format, "ERROR", ## __VA_ARGS__)
#define LOGINFO(format, ...) __LOG__(format, "INFO", ## __VA_ARGS__)

/** DATABASE **/
static PGconn *conn = NULL;

void do_exit(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

void db_connect() {
    if(conn == NULL)
        conn = PQconnectdb(getenv("POSTGRES_CNX"));
    if (PQstatus(conn) == CONNECTION_BAD) {
        LOGERROR("Connection to database failed: %s\n", PQerrorMessage(conn));
        do_exit(conn);
    }
}

void db_disconnect() {
    PQfinish(conn);
}

PGresult* db_query(char* sql_cmd) {
#ifdef DEBUG
    LOGDEBUG("<db_query>: %s\n", sql_cmd);
#endif
    db_connect();
    PGresult *res = PQexec(conn, sql_cmd);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        LOGERROR("Failed to get data for command %s\n", sql_cmd);        
        PQclear(res);
        do_exit(conn);
    }
#ifdef DEBUG
    LOGDEBUG("</db_query>: %s\n", sql_cmd);
#endif
    return res;
}

bool db_upload_file(char* table_name, char* file_name) {
    db_connect();
    char sql_cmd[128];
    sprintf(sql_cmd, "COPY %s FROM '%s'", table_name, file_name);
    PGresult *res;
    bool success = true;
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("BEGIN failed: %s\n", PQresultErrorMessage(res));
        success = false;
    }
    PQclear(res);
    res = PQexec(conn, sql_cmd);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("%s failed: %s\n", sql_cmd, PQresultErrorMessage(res));
        success = false;
    }
    PQclear(res);
    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("COMMIT failed: %s\n", PQresultErrorMessage(res));
        success = false;
    }
    PQclear(res);
    return success;
}

bool db_transaction(char* sql_cmd) {
    db_connect();
    PGresult *res;
    bool success = true;
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("BEGIN failed: %s\n", PQresultErrorMessage(res));
        success = false;
    }
    PQclear(res);
    res = PQexec(conn, sql_cmd);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("%s failed: %s\n", sql_cmd, PQresultErrorMessage(res));
        success = false;
    }
    PQclear(res);
    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("COMMIT failed: %s\n", PQresultErrorMessage(res));
        success = false;
    }
    PQclear(res);
    return success;
}

bool db_upsert_from_file(char *sql_remove_tmp_table, char *sql_create_tmp_table,
                         char *copy_csv, char *sql_upsert) {
    bool success = true;
    db_connect();
    /**
     *  Delete temporary table; if one exists already
     */
    PGresult *res = PQexec(conn, sql_remove_tmp_table);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("%s failed: %s\n", sql_remove_tmp_table,
               PQresultErrorMessage(res));
    }
    PQclear(res);
    
    /**
     *  Create temporary table; load file data into temp table
     */
    res = PQexec(conn, sql_create_tmp_table);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("%s failed: %s\n", sql_create_tmp_table,
               PQresultErrorMessage(res));
        success = false;
    }
    PQclear(res);
    if (success) {
        res = PQexec(conn, copy_csv);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            printf("%s failed: %s\n", copy_csv,
                   PQresultErrorMessage(res));
            success = false;
        } else
            success = db_transaction(sql_upsert);
    }
    return success;
}

/** HASHTABLE 
This is based on : https://github.com/jamesroutley/write-a-hash-table/

Need this:
1. Number of elements is known at creation time.
2. Add an internal array which will contain only the keys.
3. I don't need resizing.
4. I don't need delete.
5. Need a function to insert all elements.
6. Need a function to return a list of indices between two dates.
7. Need a function to return the key values for the indices returned in 6.
**/

typedef struct cal_info_t {
    int day_number;
    int busday_number;
    bool is_busday;
} cal_info, *cal_info_ptr;

typedef struct ohlcv_record_t {
    int open;
    int high;
    int low;
    int close;
    int volume;
    char date[20];
} ohlcv_record, *ohlcv_record_ptr;

struct hashtable_t;
/* struct hashtable, *hashtable_ptr; */

typedef struct stx_data_t {
    ohlcv_record_ptr data;
    int num_recs;
    struct hashtable_t* splits;
    int pos;
    int ser_pos; /** last index that was serialized */
    int last_adj;
    char stk[16];
    int intraday;
} stx_data, *stx_data_ptr;

typedef enum { DIVI_HT, CAL_HT, DATA_HT, STR_HT } ht_type;

union item_value {
    float ratio;
    cal_info_ptr cal;
    void* data;
    char* str;
};

typedef struct ht_item_t {
    char key[16];
    ht_type item_type;
    union item_value val;
} ht_item, *ht_item_ptr;

typedef struct hashtable_t {
    int size;
    int count;
    ht_item_ptr* items;
    ht_item_ptr list;
} hashtable, *hashtable_ptr;

/** Global sentinel item; marks that a bucket contains a deleted item **/
static ht_item HT_DELETED_ITEM = {"", DIVI_HT, 0};

static int HT_PRIME_1 = 151;
static int HT_PRIME_2 = 163;

/*
 * Return whether x is prime or not
 *   1  - prime
 *   0  - not prime
 *   -1 - undefined (i.e. x < 2)
 */
int is_prime(const int x) {
    if (x < 2) { return -1; }
    if (x < 4) { return 1; }
    if ((x % 2) == 0) { return 0; }
    for (int i = 3; i <= floor(sqrt((double) x)); i += 2) {
        if ((x % i) == 0) {
            return 0;
        }
    }
    return 1;
}

/*
 * Return the next prime after x, or x if x is prime
 */
int next_prime(int x) {
    while (is_prime(x) != 1) {
        x++;
    }
    return x;
}

void ht_new_divi(ht_item_ptr i, const char* k, float v) {
    strcpy(i->key, k);
    i->item_type = DIVI_HT;
    i->val.ratio = v;
}

void ht_new_cal(ht_item_ptr i, const char* k, int dt_info) {
    strcpy(i->key, k);
    i->item_type = CAL_HT;
    i->val.cal = (cal_info_ptr) malloc(sizeof(cal_info));
    i->val.cal->day_number = abs(dt_info) & 0xffff;
    i->val.cal->busday_number = (abs(dt_info) >> 16) & 0x7fff;
    i->val.cal->is_busday = (dt_info >= 0);
}

ht_item_ptr ht_new_data(const char* k, void* data) {
    ht_item_ptr hi = (ht_item_ptr) calloc((size_t)1, sizeof(ht_item));
    strcpy(hi->key, k);
    hi->item_type = DATA_HT;
    hi->val.data = data;
    return hi;
}

ht_item_ptr ht_new_str(ht_item_ptr hi, const char* k, char* str) {
    strcpy(hi->key, k);
    hi->item_type = STR_HT;
    hi->val.str = str;
    return hi;
}

void ht_del_item(ht_item_ptr i) {
    /**
     *  Only used to delete stx data; then values are freed in ts_stx.
     *  Strings HT loads last date from DB and is freed when PGresult is freed
     */
    if (i->item_type == CAL_HT) {
        free(i->val.cal);
        i->val.cal = NULL;
    }
    free(i);
}

int ht_hash(const char* s, const int a, const int m) {
    long hash = 0;
    const int len_s = strlen(s);
    for (int i = 0; i < len_s; i++) {
        hash += (long)pow(a, len_s - (i+1)) * s[i];
        hash = hash % m;
    }
    return (int)hash;
}

int ht_get_hash(const char* s, const int num_buckets, const int attempt) {
    int hash_a = ht_hash(s, HT_PRIME_1, num_buckets);
    int hash_b = ht_hash(s, HT_PRIME_2, num_buckets);
#ifdef DDEBUGG
    if (attempt <= 999) 
        LOGINFO("hash_a = %d, hash_b = %d, num_buckets = %d, attempt = %d\n",
                hash_a, hash_b, num_buckets, attempt);
#endif
    if (hash_b % num_buckets == 0)
        hash_b = 1;
    return (hash_a + attempt * hash_b) % num_buckets;
}

void ht_insert(hashtable_ptr ht, ht_item_ptr item) {
    char* key = item->key;
    int index = ht_get_hash(key, ht->size, 0);
    ht_item_ptr crt_item = ht->items[index];
    int i = 1;
    while (crt_item != NULL) {
        if (crt_item != &HT_DELETED_ITEM) {
            if (!strcmp(crt_item->key, key)) {
                ht_del_item(crt_item);
                ht->items[index] = item;
                return;
            }
        }
        index = ht_get_hash(item->key, ht->size, i);
#ifdef DDEBUGG
        LOGDEBUG("i= %d, index = %d\n", i, index);
#endif
        crt_item = ht->items[index];
        i++;
    }
    ht->items[index] = item;
    ht->count++;
}

ht_item_ptr ht_get(hashtable_ptr ht, const char* key) {
    if (ht == NULL)
        return NULL;
    int index = ht_get_hash(key, ht->size, 0);
    ht_item_ptr item = ht->items[index];
    int i = 1;
    while (item != NULL) {
        if (item != &HT_DELETED_ITEM) {
            if (strcmp(item->key, key) == 0)
                return item;
        }
        index = ht_get_hash(key, ht->size, i);
        item = ht->items[index];
        i++;
    } 
    return NULL;
}

void ht_delete(hashtable_ptr ht, const char* key) {
    int ix = ht_get_hash(key, ht->size, 0);
    ht_item* item = ht->items[ix];
    int i = 1;
    while (item != NULL) {
        if (item != &HT_DELETED_ITEM) {
            if (strcmp(item->key, key) == 0) {
                ht_del_item(item);
                ht->items[ix] = &HT_DELETED_ITEM;
            }
        }
        ix = ht_get_hash(key, ht->size, i);
        item = ht->items[ix];
        i++;
    } 
    ht->count--;
}

hashtable_ptr ht_new(ht_item_ptr list, int num_elts) {
    if (num_elts <= 0)
        return NULL;
    hashtable_ptr ht = (hashtable_ptr)calloc((size_t)1, sizeof(hashtable));
    ht->count = 0;
    ht->list = list;
    ht->size = next_prime(2 * num_elts);
    ht->items = (ht_item_ptr*)calloc((size_t)ht->size, sizeof(ht_item_ptr));
    if (list != NULL) {
        for (int ix = 0; ix < num_elts; ++ix)
            ht_insert(ht, &list[ix]);
    }
    return ht;
}

hashtable_ptr ht_divis(PGresult* res) {
    int num = PQntuples(res);
#ifdef DDEBUGG
    LOGDEBUG("Found %d records\n", num);
#endif
    ht_item_ptr list = NULL;
    if (num > 0) {
        list = (ht_item_ptr) calloc((size_t)num, sizeof(ht_item));
        for(int ix = 0; ix < num; ix++) {
#ifdef DDEBUGG
            LOGDEBUG("ix = %d\n", ix);
#endif
            float ratio = atof(PQgetvalue(res, ix, 0));
            char* dt = PQgetvalue(res, ix, 1);
            ht_new_divi(list + ix, dt, ratio);
#ifdef DDEBUGG
            LOGDEBUG("value = %12.6f\n", ratio);
#endif
        }
    }
    return ht_new(list, num);
}

hashtable_ptr ht_strings(PGresult* res) {
    int num = PQntuples(res);
#ifdef DDEBUGG
    LOGDEBUG("Found %d records\n", num);
#endif
    ht_item_ptr list = NULL;
    if (num > 0) {
        list = (ht_item_ptr) calloc((size_t)num, sizeof(ht_item));
        for(int ix = 0; ix < num; ix++) {
#ifdef DDEBUGG
            LOGDEBUG("ix = %d\n", ix);
#endif
            char* key = PQgetvalue(res, ix, 0);
            char* value = PQgetvalue(res, ix, 1);
            ht_new_str(list + ix, key, value);
#ifdef DDEBUGG
            LOGDEBUG("key = %s, value = %s\n", key, value);
#endif
        }
    }
    return ht_new(list, num);
}

hashtable_ptr ht_calendar(PGresult* res) {
    int num = PQntuples(res);
#ifdef DDEBUGG
    LOGDEBUG("Calendar: found %d records\n", num);
#endif
    ht_item_ptr list = NULL;
    if (num > 0) {
        list = (ht_item_ptr) calloc((size_t)num, sizeof(ht_item));
        for(int ix = 0; ix < num; ix++) {
#ifdef DDEBUGG
            LOGDEBUG("ix = %d\n", ix);
#endif
            char* dt = PQgetvalue(res, ix, 0);
            int dt_info = atoi(PQgetvalue(res, ix, 1));
            ht_new_cal(list + ix, dt, dt_info);
#ifdef DDEBUGG
            LOGDEBUG("dt=%s, is_busday=%5s, num_day=%5d, num_busday=%5d\n",
                     dt, (dt_info > 0)? "true": "false", 
                     abs(dt_info) & 0xffff, (abs(dt_info) >> 16) & 0x7fff);
#endif
        }
    }
    return ht_new(list, num);
}


int ht_seq_index(hashtable_ptr ht, char* date) {
    if (ht == NULL)
        return -2;
    if (strcmp(date, ht->list[0].key) < 0)
        return -1;
    if (strcmp(date, ht->list[ht->count - 1].key) > 0)
        return ht->count - 1;
    int i = 0, j = ht->count - 1, mid = (i + j) / 2, cmp;
#ifdef DDEBUGG
    LOGDEBUG("i = %d, j = %d, mid = %d\n", i, j, mid);
#endif
    while(i <= j) {
        cmp = strcmp(date, ht->list[mid].key);
#ifdef DDEBUGG
        LOGDEBUG("cmp = %d, date = %s, ht->list[mid].key = %s\n", 
                 cmp, date, ht->list[mid].key);
#endif
        if(cmp > 0)
            i = mid + 1;
        else if(cmp < 0)
            j = mid - 1;
        else
            return mid;
        mid = (i + j) / 2;
#ifdef DDEBUGG
        LOGDEBUG("i = %d, j = %d, mid = %d\n", i, j, mid);
#endif
    }
    return mid;
}


void ht_print(hashtable_ptr ht) {
    /* LOGINFO("Hashtable: \n"); */
    if(ht == NULL || ht->size == 0)
        return;
    for(int ix = 0; ix < ht->count; ix++) {
        ht_item_ptr crs = ht->list + ix;
        if (crs->item_type == DIVI_HT)
            fprintf(stderr, "  %s, %12.6f\n", crs->key, crs->val.ratio);
        else if (crs->item_type == STR_HT)
            fprintf(stderr, "  %s, %s\n", crs->key, crs->val.str);
        else
            fprintf(stderr, "  %s, %5d %5d %d\n", crs->key,
                    crs->val.cal->day_number, crs->val.cal->busday_number,
                    crs->val.cal->is_busday);
    }
}


void ht_free(hashtable_ptr ht) {
    if (ht == NULL) 
        return;
    if (ht->list == NULL) {
        for(int ix = 0; ix < ht->size; ix++) {
            if (ht->items[ix] != NULL)
                free(ht->items[ix]);
        }
    } else {
        for(int ix = 0; ix < ht->count; ix++) {
            ht_item_ptr crs = ht->list + ix;
            if (crs->item_type == CAL_HT)
                free(crs->val.cal);
                crs->val.cal = NULL;
        }
        free(ht->list);
    }
    free(ht->items);
    free(ht);
    ht = NULL;
}


/** CALENDAR **/
/** LOGGING Used this: https://stackoverflow.com/questions/7411301/ **/
/* Get current time in format YYYY-MM-DD HH:MM:SS.mms */
static hashtable_ptr cal = NULL;

hashtable_ptr cal_get() {
    if (cal == NULL) {
        char sql_cmd[80];
        LOGINFO("getting calendar from database\n");
        strcpy(sql_cmd, "select * from calendar");    
        PGresult *sql_res = db_query(sql_cmd);
        LOGINFO("got calendar fron database\n");
        cal = ht_calendar(sql_res);
        PQclear(sql_res);
        LOGINFO("populated hashtable with calendar dates\n");
    }
    return cal;
}

void cal_free() {
    if (cal != NULL) {
        ht_free(cal);
        cal = NULL;
    }
}

int cal_num_busdays(char* start_date, char* end_date) {
    ht_item_ptr d1 = ht_get(cal_get(), start_date);
    ht_item_ptr d2 = ht_get(cal_get(), end_date);
    int num_days = d2->val.cal->busday_number - d1->val.cal->busday_number;
    int adj = 0;
    if (strcmp(start_date, end_date) <= 0) {
        if (d1->val.cal->is_busday)
            adj = 1;
    } else {
        if (d2->val.cal->is_busday)
            adj = -1;
    }
    return (num_days + adj);
}

/** returns day number for a given date */
int cal_ix(char* date) {
    ht_item_ptr d = ht_get(cal_get(), date);
    return d->val.cal->day_number;
}

/** returns business day number for a given date, -1 if a holiday */
int cal_bix(char* date) {
    ht_item_ptr d = ht_get(cal_get(), date);
    return d->val.cal->is_busday? d->val.cal->busday_number: -1;
}

int cal_next_bday(int crt_ix, char** next_date) {
    int next_ix = crt_ix + 1;
    for (ht_item_ptr crs = cal_get()->list + next_ix; !crs->val.cal->is_busday;
         crs++)
        next_ix++;
    *next_date = &(cal_get()->list[next_ix].key[0]);
    return next_ix;
}

int cal_prev_bday(int crt_ix, char** prev_date) {
    int prev_ix = crt_ix - 1;
    for (ht_item_ptr crs = cal_get()->list + prev_ix; !crs->val.cal->is_busday;
         crs--)
        prev_ix--;
    *prev_date = &(cal_get()->list[prev_ix].key[0]);
    return prev_ix;
}

/** Return the calendar pointer to 'dt', if 'dt' is a business day. If 'dt' is
 * a holiday, return the next business day if 'next_bday' is true, or the
 * previous business day, if 'next_bday' is false
 */
char* cal_move_to_bday(char* dt, bool next_bday) {
    int ix = cal_ix(dt), bix = cal_bix(dt);
    char *res = &(cal_get()->list[ix].key[0]);
    if (bix == -1) {
        if (next_bday)
            cal_next_bday(ix, &res);
        else
            cal_prev_bday(ix, &res);
    }
    return res;
}

/** Move 'num_days' business days away from the input date 'crt_date'
 * If 'num_days' is 0, return a pointer to the current date, if it is a
 * business day. Otherwise, return the previous business day.
*/
int cal_move_bdays(char* crt_date, int num_days, char** new_date) {
    int crt_ix = cal_ix(crt_date);
    int ix = 0;
    if (num_days == 0) {
        if (cal_bix(crt_date) == -1)
            crt_ix = cal_prev_bday(crt_ix, new_date);
        else
            *new_date = &(cal_get()->list[crt_ix].key[0]);
    } else if (num_days > 0) {
        while(ix < num_days) {
            crt_ix = cal_next_bday(crt_ix, new_date);
            ix++;
        }
    } else {
        while(ix > num_days) {
            crt_ix = cal_prev_bday(crt_ix, new_date);
            ix--;
        }
    }
    return crt_ix;
}

int cal_expiry(int ix, char** exp_date) {
    char *crt_date = &(cal_get()->list[ix].key[0]), tmp[16];
    int bix = cal_get()->list[ix].val.cal->is_busday? ix:
        cal_next_bday(ix, &crt_date);
    strncpy(tmp, crt_date, 4);
    tmp[4] = '\0';
    int year = atoi(tmp);
    strncpy(tmp, crt_date + 5, 2);
    tmp[2] = '\0';
    int month = atoi(tmp);
    strncpy(tmp, crt_date + 8, 2);
    tmp[2] = '\0';
    int day = atoi(tmp);
    int start_of_month_ix = bix - day + 1;
    int start_of_month_day_of_week = start_of_month_ix % 7;
    int third_friday = 15 + ((11 - start_of_month_day_of_week) % 7);
    int third_friday_ix = third_friday - 1 + start_of_month_ix;
    int exp_ix;
    if (third_friday < day) {
        month++;
        if (month > 12) {
            month = 1;
            year++;
        }
        sprintf(tmp, "%u-%02u-01", (unsigned short)year,
                (unsigned short) month);
        start_of_month_ix = cal_ix(tmp);
        start_of_month_day_of_week = start_of_month_ix % 7;
        third_friday = 15 + ((11 - start_of_month_day_of_week) % 7);
        third_friday_ix = third_friday - 1 + start_of_month_ix;
    }
    if (third_friday_ix <= 10974) {
        exp_ix = third_friday_ix + 1;
        *exp_date = &(cal_get()->list[exp_ix].key[0]);
    } else {
        *exp_date = &(cal_get()->list[third_friday_ix].key[0]);
        exp_ix = (cal_get()->list[third_friday_ix].val.cal->is_busday)?
            third_friday_ix: cal_prev_bday(third_friday_ix, exp_date);
    }
    return exp_ix;
}

int cal_expiry_next(int ix, char** exp_date) {
    ix = cal_expiry(ix, exp_date);
    ix = cal_next_bday(ix, exp_date);
    ix = cal_expiry(ix, exp_date);
    return ix;
}

int cal_prev_expiry(int ix, char** prev_exp_date) {
    int next_ix = cal_expiry(ix, prev_exp_date);
    return cal_expiry(next_ix - 40, prev_exp_date);
}

int cal_exp_bday(int exp_ix, char** exp_bdate) {
    int res = exp_ix;
    if(!cal_get()->list[exp_ix].val.cal->is_busday)
        res = cal_prev_bday(exp_ix, exp_bdate);
    else
        *exp_bdate = &(cal_get()->list[exp_ix].key[0]);
    return res;
}

/**
 *  Get the current trading date: current date if a business day and
 *  time is later than 9:30AM EST; previous business day otherwise
 */
char* cal_current_trading_date() {
    time_t seconds = time(NULL);
    char *res;
    struct tm *ts = localtime(&seconds);
    int hours = ts->tm_hour, minutes = ts->tm_min;
    char crt_date[12];
    strftime(crt_date, 12, "%Y-%m-%d", ts);
    int ix = cal_ix(crt_date);
    if ((hours < 9) || ((hours == 9) && (minutes < 30)))
        ix--;
    if(!cal_get()->list[ix].val.cal->is_busday)
        cal_prev_bday(ix, &res);
    else
        res = &(cal_get()->list[ix].key[0]);
    return res;
}

/**
 *  Get the market date: current date if dt is a business day and time
 *  is earlier than 5:00PM EST; next business day otherwise.  If dt is
 *  NULL, then use the current time.
 */
char* cal_market_date(char *dt) {
    char *res, crt_date[20];
    int hours, minutes;
    if (dt == NULL) {
        time_t seconds = time(NULL);
        struct tm *ts = localtime(&seconds);
        hours = ts->tm_hour;
        minutes = ts->tm_min;
        strftime(crt_date, 20, "%Y-%m-%d", ts);
        ts = NULL;
    } else {
        struct tm ts;
        memset(&ts, 0, sizeof(struct tm));
        if (strptime(dt, "%Y-%m-%d %H:%M:%S", &ts) == NULL) {
            LOGERROR("strptime failed for datetime %s\n", dt);
            return NULL;
        }
        hours = ts.tm_hour;
        minutes = ts.tm_min;
        strftime(crt_date, 20, "%Y-%m-%d", &ts);
    }
    int ix = cal_ix(crt_date);
    if(!cal_get()->list[ix].val.cal->is_busday || (hours >= 17))
        cal_next_bday(ix, &res);
    else
        res = &(cal_get()->list[ix].key[0]);
    return res;
}

char* cal_current_busdate(int hr) {
    time_t seconds = time(NULL);
    char *res;
    struct tm *ts = localtime(&seconds);
    int hours = ts->tm_hour;
    char crt_date[12];
    strftime(crt_date, 12, "%Y-%m-%d", ts);
    int ix = cal_ix(crt_date);
    if (hours < hr)
        ix--;
    if(!cal_get()->list[ix].val.cal->is_busday)
        cal_prev_bday(ix, &res);
    else
        res = &(cal_get()->list[ix].key[0]);
    return res;
}

/** This function returns true if today is a business day.  It is used
 * primarily to avoid running the cron jobs on holidays. If today is a
 * holiday, the cron job will */
bool cal_is_today_busday() {
    time_t seconds = time(NULL);
    char *res;
    struct tm *ts = localtime(&seconds);
    int hours = ts->tm_hour;
    char crt_date[12];
    strftime(crt_date, 12, "%Y-%m-%d", ts);
    int ix = cal_ix(crt_date);
    return cal_get()->list[ix].val.cal->is_busday;
}

/** This function returns true if dt is a business day.  It is a
 * convenience wrapper around the calendar internals
 */
bool cal_is_busday(char* dt) {
    int ix = cal_ix(dt);
    return cal_get()->list[ix].val.cal->is_busday;
}

long cal_long_expiry(char* exp_dt) {
    char *month = NULL, *day = NULL, year[16];
    struct tm result;
    time_t tt;
    strcpy(year, exp_dt);
    month = strchr(year, '-');
    *month++ = '\0';
    day = strchr(month, '-');
    *day++ = '\0';
    result.tm_mday = atoi(day);
    result.tm_mon = atoi(month) - 1;
    result.tm_year = atoi(year) - 1900;
    result.tm_hour = 0;
    result.tm_min = 0;
    result.tm_sec = 0;
    result.tm_isdst = 0;
    tt = mktime(&result) - timezone;
    return (long)tt;
}

/**
 *  This function returns the current time to be used when inserting
 *  setups in the database
 */
char* cal_setup_time(bool eod, bool tomorrow) {
    static char _setup_time_retval[8];
    if (eod) {
        if (tomorrow)
            strcpy(_setup_time_retval, "08:00");
        else
            strcpy(_setup_time_retval, "20:00");
    } else {
        time_t seconds = time(NULL);
        struct tm *ts = localtime(&seconds);
        strftime(_setup_time_retval, 8, "%H:%M", ts);
    }
    return _setup_time_retval;
}

/**
 *  Return the calendar pointer to the date portion of'dt', if 'dt' is
 *  a datetime in the format 'YYYY-mm-dd'.
 */
char* cal_get_date_from_dt(char* dt) {
    char copy_dt[20];
    memset(copy_dt, 0, 20);
    strcpy(copy_dt, dt);
    *(strchr(copy_dt, ' ')) = '\0';
    int ix = cal_ix(copy_dt);
    return &(cal_get()->list[ix].key[0]);
}

/**
 *  Get the number of 5 minute ticks between two datetimes.
 */
int cal_5min_ticks(char *start_dt, char *end_dt) {
    char *start_date = cal_get_date_from_dt(start_dt);
    char *end_date = cal_get_date_from_dt(end_dt);
    int b_days = cal_num_busdays(start_date, end_date) - 1, num_recs = 0;
    struct tm end_ts;
    memset(&end_ts, 0, sizeof(struct tm));
    strptime(end_dt, "%Y-%m-%d %H:%M:%S", &end_ts);
    int mins = (end_ts.tm_hour - 9) * 60 + end_ts.tm_min - 30;
    num_recs = 1 + 78 * b_days + mins / 5;
    return num_recs;
}

/**
 *  Get the current 5-minute trading timestamp, in the format
 *  YYYY-mm-dd HH:MM:ss.
 */
char* cal_current_trading_datetime() {
    static char _crt_trade_retval[20];
    time_t seconds = time(NULL);
    struct tm *ts = localtime(&seconds);
    char crt_date[12];
    strftime(crt_date, 12, "%Y-%m-%d", ts);
    char *current_trading_date = cal_current_trading_date();
    if (!strcmp(crt_date, current_trading_date) &&
        (ts->tm_hour < 16 &&
         ((ts->tm_hour > 9) ||
          (ts->tm_hour == 9 && ts->tm_min>= 30)))) {
        ts->tm_min = 5 * (ts->tm_min / 5);
        ts->tm_sec = 0;
        strftime(_crt_trade_retval, 20, "%Y-%m-%d %H:%M:%S", ts);
    } else
        sprintf(_crt_trade_retval, "%s 15:55:00", current_trading_date);
    return _crt_trade_retval;
}

/**
 * @brief transform the input date(time) in format YYYY-MM-DD HH:MM:SS
 * 
 * @param dt 
 * @return char* 
 */
char *cal_datetime(char *dt) {
    static char _retval_datetime[32];
    char end_date[20], *hhmm = NULL, *eod_hhmm = "15:55:00";
    /** Separate dt into date and time */
    strcpy(end_date, dt);
    hhmm = strchr(end_date, ' ');
    if (hhmm != NULL)
        *hhmm++ = '\0';
    else
        hhmm = eod_hhmm;
    /** If date not a business date, move it to previous business date */
    if (!cal_is_busday(end_date)) {
        char *prev_bdate = NULL;
        cal_prev_bday(cal_ix(end_date), &prev_bdate);
        strcpy(end_date, prev_bdate);
    }
    /** copy the date and the time in the datetime */
    memset(_retval_datetime, 0, 32 * sizeof(char));
    sprintf(_retval_datetime, "%s %s", end_date, hhmm);
    return _retval_datetime;
}

/**
 *  Generate timestamp from intraday date
 */
unsigned long cal_tsfromdt(char *dt) {
    struct tm result;
    time_t tt;
    if (strptime(dt, "%Y-%m-%d %H:%M:%S", &result) == NULL) {
        LOGERROR("strptime failed for datetime %s\n", dt);
        return 0;
    }
    time_t ts = mktime(&result);
    return ts;
}

/**
 *  Move 5 minutes ahead (if dir is 1) or behind (dir is -1) in time.
 *  dt contains both the intial datetime and the datetime after moving
 *  5 minutes.
 */
void cal_move_5mins(char *dt, int dir) {
    struct tm ts, *ts_ptr = NULL;
    memset(&ts, 0, sizeof(struct tm));
    if (strptime(dt, "%Y-%m-%d %H:%M:%S", &ts) == NULL) {
        LOGERROR("strptime failed for datetime %s\n", dt);
        return;
    }
    ts.tm_isdst = -1; /** handle DST */
    char tmp_dt[20], *hhmm = NULL, *move_date = NULL;
    memset(tmp_dt, 0, 20 * sizeof(char));
    strcpy(tmp_dt, dt);
    hhmm = strchr(tmp_dt, ' ');
    *hhmm = '\0';
    if (!cal_is_busday(tmp_dt)) {
        if (dir == 1) {
            cal_next_bday(cal_ix(tmp_dt), &move_date);
            sprintf(dt, "%s 09:30:00", move_date);
        } else {
            cal_prev_bday(cal_ix(tmp_dt), &move_date);
            sprintf(dt, "%s 15:55:00", move_date);
        }
    } else {
        if (dir == 1) {
            if (ts.tm_hour < 9 || (ts.tm_hour == 9 && ts.tm_min < 30))
                sprintf(dt, "%s 09:30:00", tmp_dt);
            else if (ts.tm_hour > 15 ||
                     (ts.tm_hour == 15 && ts.tm_min == 55)) {
                cal_next_bday(cal_ix(tmp_dt), &move_date);
                sprintf(dt, "%s 09:30:00", move_date);
            } else { /** advance 5 mins during the trading day */
                time_t tt = mktime(&ts);
                tt += 300;
                ts_ptr = localtime(&tt);
                strftime(dt, strlen(dt), "%Y-%m-%d %H:%M:%S", ts_ptr);
            }
        } else { /** dir == -1 */
            if (ts.tm_hour < 9 || (ts.tm_hour == 9 && ts.tm_min == 30)) {
                cal_prev_bday(cal_ix(tmp_dt), &move_date);
                sprintf(dt, "%s 15:55:00", move_date);
            } else if (ts.tm_hour > 15)
                sprintf(dt, "%s 15:55:00", tmp_dt);
            else { /** go back 5 mins during the trading day */
                time_t tt = mktime(&ts);
                tt -= 300;
                ts_ptr = localtime(&tt);
                strftime(dt, strlen(dt), "%Y-%m-%d %H:%M:%S", ts_ptr);
            }
        }
    }
}

/**
 *  This function returns the last date that has intraday data in the
 *  database
 */
char *cal_last_intraday_date() {
    char *sql_cmd = "SELECT MAX(DATE(dt)) FROM intraday";
    PGresult *sql_res = db_query(sql_cmd);
    char* last_intraday_date = PQgetvalue(sql_res, 0, 0);
    int calix = cal_ix(last_intraday_date);
    last_intraday_date = &(cal_get()->list[calix].key[0]);
    PQclear(sql_res);
    return last_intraday_date;
}

/**
 * @brief Format dt: YYYY-MM-dd for EOD, YYYY-MM-dd HH:MM:SS for intraday
 *
 * @param dt - datetime to format
 * @param intraday - true for intraday, false for EOD
 * @return char* - formatted datetime
 */
char* cal_sanitize_dt(char *dt, bool intraday) {
    static char _retval[20];
    strcpy(_retval, dt);
    char *crs = strchr(_retval, ' ');
    if (intraday) {
        /** Ensure that HH:MM:SS exists in intraday dates
         *  If not there, append 15:55:00 to the date
         */
        if (crs == NULL)
            sprintf(_retval, "%s 15:55:00", dt);
        /** TODO: Ensure that HH:MM:SS are between 09:30:00 and 15:55:00 */
    } else {
        /** Remove HH:MM:SS from datetimes used in EOD records */
        if (crs != NULL)
            *crs = '\0';
    }
    return _retval;
}


/** These two hashtables keep in memory data or JL records calculated
 *  for various equities
 */
static hashtable_ptr stx = NULL;
static hashtable_ptr id_stx = NULL;
static hashtable_ptr jl = NULL;
static hashtable_ptr id_jl = NULL;

/** Return the hash table with EOD stock data. */
hashtable_ptr ht_data() {
    if (stx == NULL) 
        stx = ht_new(NULL, 2500);
    return stx;
}

/** Return the hash table with intraday stock data. */
hashtable_ptr ht_id_data() {
    if (id_stx == NULL) 
        id_stx = ht_new(NULL, 2500);
    return id_stx;
}

/** Return the hash table with JL EOD stock data for a given factor */
hashtable_ptr ht_jl(const char* factor) {
    if (jl == NULL) 
        jl = ht_new(NULL, 5);
    ht_item_ptr jlht = ht_get(jl, factor);
    hashtable_ptr jl_factor_ht = NULL;
    if (jlht == NULL) {
        jl_factor_ht = ht_new(NULL, 2500);
        jlht = ht_new_data(factor, (void *) jl_factor_ht);
        ht_insert(jl, jlht);
    } else
        jl_factor_ht = (hashtable_ptr) jlht->val.data;
    return jl_factor_ht;
}

/** Return hash table with JL intraday stock data for a factor */
hashtable_ptr ht_id_jl(const char* factor) {
    if (id_jl == NULL) 
        id_jl = ht_new(NULL, 5);
    ht_item_ptr jlht = ht_get(id_jl, factor);
    hashtable_ptr jl_factor_ht = NULL;
    if (jlht == NULL) {
        jl_factor_ht = ht_new(NULL, 2500);
        jlht = ht_new_data(factor, (void *) jl_factor_ht);
        ht_insert(id_jl, jlht);
    } else
        jl_factor_ht = (hashtable_ptr) jlht->val.data;
    return jl_factor_ht;
}

#endif
