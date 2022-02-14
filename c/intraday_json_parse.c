#include <cjson/cJSON.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_net.h"

typedef struct id_t {
    long timestamp;
    int open;
    int high;
    int low;
    int close;
    int volume;
} id, *id_ptr;

cJSON* get_sub_array(cJSON *parent, char* sub_array_name) {
    char err_msg[80];
    cJSON *sub_array = cJSON_GetObjectItemCaseSensitive(parent, sub_array_name);
    if (sub_array == NULL) {
        sprintf(err_msg, "No '%s' found in \n", sub_array_name);
        net_print_json_err(parent, err_msg);
        return NULL;
    }
    if (!cJSON_IsArray(sub_array)) {
        sprintf(err_msg, "'%s' not an array \n", sub_array_name);
        net_print_json_err(sub_array, err_msg);
        return NULL;
    }
    return sub_array;
}

int main(int argc, char** argv) {
    char *filename = "/tmp/dfs_55.json";
    id_ptr id_data = NULL;
    FILE *fp = NULL;
    if ((fp = fopen(filename, "rb")) == NULL) {
        fprintf(stderr, "Failed to open %s\n", filename);
        return -1;
    }
    char* buffer = NULL;
    size_t len;
    ssize_t bytes_read = getdelim( &buffer, &len, '\0', fp);
    if ( bytes_read != -1) {
        /* Success, now the entire file is in the buffer */
        fprintf(stderr, "Read %ld bytes\n", bytes_read);
    }
    fclose(fp);
    cJSON *json = net_parse_quote(buffer);
    if (buffer != NULL) {
        free(buffer);
        buffer = NULL;
    }
    cJSON *chart = cJSON_GetObjectItemCaseSensitive(json, "chart");
    if (chart == NULL) {
        net_print_json_err(json, "No 'chart' found in data");
        goto end;
    }
    cJSON* chart_result = get_sub_array(chart, "result");
    if (chart_result == NULL) {
        net_print_json_err(chart, "No 'result' array found in 'chart'");
        goto end;
    }
    cJSON *intraday_data = cJSON_GetArrayItem(chart_result, 0);
    cJSON *timestamps = get_sub_array(intraday_data, "timestamp");
    if (timestamps == NULL) {
        net_print_json_err(intraday_data,
                           "No 'timestamp' array found in 'intraday_data'");
        goto end;
    }
    cJSON *indicators = cJSON_GetObjectItemCaseSensitive(intraday_data,
                                                         "indicators");
    if (indicators == NULL) {
        net_print_json_err(json, "No 'indicators' found in 'chart/result[0]'");
        goto end;
    }
    cJSON *quote_arr = get_sub_array(indicators, "quote");
    if (quote_arr == NULL) {
        net_print_json_err(indicators, "No 'quote' array in 'indicators'");
        goto end;
    }
    cJSON *quote = cJSON_GetArrayItem(quote_arr, 0);
    /* cJSON *lows = get_sub_array(intraday_data, "meta"); */
    /* if (lows == NULL) */
    /*     goto end; */
    cJSON *lows = get_sub_array(quote, "low");
    cJSON *opens = get_sub_array(quote, "open");
    cJSON *volumes = get_sub_array(quote, "volume");
    cJSON *highs = get_sub_array(quote, "high");
    cJSON *closes = get_sub_array(quote, "close");
    if (lows == NULL || opens == NULL || volumes == NULL ||
        highs == NULL || closes == NULL) {
        net_print_json_err(quote, "Failed to parse quote");
        goto end;
    }
    int total = cJSON_GetArraySize(timestamps);
    id_data = (id_ptr) calloc((size_t) total, sizeof(id));
    cJSON *crs;
    int num = 0;
    cJSON_ArrayForEach(crs, timestamps) {
        id_data[num++].timestamp = (long)(crs->valuedouble);
    }
    num = 0;
    cJSON_ArrayForEach(crs, lows)
        id_data[num++].low = (int)(100 * crs->valuedouble);
    num = 0;
    cJSON_ArrayForEach(crs, opens) {
        id_data[num++].open = (int)(100 * crs->valuedouble);
    }
    num = 0;
    cJSON_ArrayForEach(crs, highs) {
        id_data[num++].high = (int)(100 * crs->valuedouble);
    }
    num = 0;
    cJSON_ArrayForEach(crs, volumes) {
        id_data[num++].volume = (int)(crs->valuedouble);
    }
    num = 0;
    cJSON_ArrayForEach(crs, closes) {
        id_data[num++].close = (int)(100 * crs->valuedouble);
    }
    FILE* fpw = fopen("/tmp/id_out_1.txt", "w");
    for (num = 0; num < total; num++)
        fprintf(fpw, "%ld %d %d %d %d %d\n", id_data[num].timestamp,
                id_data[num].open, id_data[num].high, id_data[num].low,
                id_data[num].close, id_data[num].volume);
    fclose(fpw);
 end:
    if (id_data != NULL) {
        free(id_data);
        id_data = NULL;
    }
    cJSON_Delete(json);
    return 0;
}
