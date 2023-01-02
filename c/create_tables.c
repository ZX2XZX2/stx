#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

void do_exit(PGconn *cnx, PGresult *res) {
    fprintf(stderr, "%s\n", PQerrorMessage(cnx));
    PQclear(res);
    PQfinish(cnx);    
    exit(1);
}

void create_table_if_missing(PGconn *cnx, char *tbl_name, char *create_sql) {
    char cmd_buffer[256];
    memset(cmd_buffer, 0, 256);
    sprintf(cmd_buffer, "select * from information_schema.tables where " \
            "table_schema='public' and table_name='%s'", tbl_name);
    PGresult *res = PQexec(cnx, cmd_buffer);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printf("No table data retrieved for %s\n", tbl_name);
        do_exit(cnx, res);
    }
    int rows = PQntuples(res);
    if(rows == 0) {
        fprintf(stderr, "Table %s does not exist, creating\n", tbl_name);
        PQclear(res);
        res = PQexec(cnx, create_sql);
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
            do_exit(cnx, res); 
    } else
        fprintf(stderr, "Table %s exists, skipping\n", tbl_name);
    PQclear(res);
}

void create_index_if_missing(PGconn *cnx, char *tbl_name, char *idx_name,
                             char *create_sql) {
    char cmd_buffer[256];
    memset(cmd_buffer, 0, 256);
    sprintf(cmd_buffer, "select * from pg_indexes where tablename='%s' " \
            "and indexname = '%s' ", tbl_name, idx_name);
    PGresult *res = PQexec(cnx, cmd_buffer);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printf("No index data retrieved for table %s index %s\n", tbl_name,
               idx_name);
        do_exit(cnx, res);
    }
    int rows = PQntuples(res);
    if(rows == 0) {
        fprintf(stderr, "Index %s for table %s does not exist, creating\n",
                idx_name, tbl_name);
        PQclear(res);
        res = PQexec(cnx, create_sql);
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
            do_exit(cnx, res); 
    } else
        fprintf(stderr, "Index %s for table %s exists, skipping\n",
                idx_name, tbl_name);
    PQclear(res);
    
}

int main() {
    PGconn *cnx = PQconnectdb(getenv("POSTGRES_CNX"));
    if (PQstatus(cnx) == CONNECTION_BAD) {
        fprintf(stderr, "Connection to database failed: %s\n",
                PQerrorMessage(cnx));
        PQfinish(cnx);
        exit(1);
    }
    
    char* create_eods = "CREATE TABLE eods( "    \
        "stk VARCHAR(16) NOT NULL, "             \
        "dt DATE NOT NULL, "                     \
        "o INTEGER NOT NULL, "                   \
        "hi INTEGER NOT NULL, "                  \
        "lo INTEGER NOT NULL, "                  \
        "c INTEGER NOT NULL, "                   \
        "v INTEGER, "                            \
        "oi INTEGER, "                           \
        "PRIMARY KEY(stk, dt))";
    create_table_if_missing(cnx, "eods", create_eods);
    char* create_eods_dt_idx = "CREATE INDEX eods_dt_idx ON eods(dt)";
    create_index_if_missing(cnx, "eods", "eods_dt_idx", create_eods_dt_idx);

    char* create_divis = "CREATE TABLE dividends( "      \
        "stk VARCHAR(16) NOT NULL, "                     \
        "dt DATE NOT NULL, "                             \
        "ratio DOUBLE PRECISION NOT NULL, "              \
        "divi_type INTEGER NOT NULL, "                   \
        "PRIMARY KEY(stk, dt))";
    create_table_if_missing(cnx, "dividends", create_divis);
    char* create_divis_dt_idx = "CREATE INDEX dividends_dt_idx ON " \
        "dividends(dt)";
    create_index_if_missing(cnx, "dividends", "dividends_dt_idx",
                            create_divis_dt_idx);

    char* create_calendar = "CREATE TABLE calendar( "    \
        "dt DATE NOT NULL, "                             \
        "idx INTEGER NOT NULL, "                         \
        "PRIMARY KEY(dt))";
    create_table_if_missing(cnx, "calendar", create_calendar);

    char* create_spots = "CREATE TABLE opt_spots( "      \
        "stk VARCHAR(16) NOT NULL, "                     \
        "dt DATE NOT NULL, "                             \
        "spot INTEGER NOT NULL, "                        \
        "PRIMARY KEY(stk, dt))";
    create_table_if_missing(cnx, "opt_spots", create_spots);
    char* create_spots_dt_idx = "CREATE INDEX opt_spots_dt_idx ON " \
        "opt_spots(dt)";
    create_index_if_missing(cnx, "opt_spots", "opt_spots_dt_idx",
                            create_spots_dt_idx);

    char* create_opts = "CREATE TABLE options( "         \
        "expiry DATE NOT NULL, "                         \
        "und VARCHAR(16) NOT NULL, "                     \
        "cp VARCHAR(1) NOT NULL, "                       \
        "strike INTEGER NOT NULL, "                      \
        "dt DATE NOT NULL, "                             \
        "bid INTEGER, "                                  \
        "ask INTEGER, "                                  \
        "v INTEGER, "                                    \
        "oi INTEGER, "                                   \
        "PRIMARY KEY(expiry, und, cp, strike, dt))";
    create_table_if_missing(cnx, "options", create_opts);
    char* create_opts_und_dt_idx = "CREATE INDEX options_und_dt_idx ON " \
        "options(und, dt)";
    create_index_if_missing(cnx, "options", "options_und_dt_idx",
                            create_opts_und_dt_idx);

    char* create_dloaded_opts = "CREATE TABLE downloaded_options( "         \
        "expiry DATE NOT NULL, "                         \
        "und VARCHAR(16) NOT NULL, "                     \
        "cp VARCHAR(1) NOT NULL, "                       \
        "strike INTEGER NOT NULL, "                      \
        "dt DATE NOT NULL, "                             \
        "bid INTEGER, "                                  \
        "ask INTEGER, "                                  \
        "v INTEGER, "                                    \
        "oi INTEGER, "                                   \
        "PRIMARY KEY(expiry, und, cp, strike, dt))";
    create_table_if_missing(cnx, "downloaded_options", create_dloaded_opts);
    char* create_dloaded_opts_und_dt_idx = "CREATE INDEX "\
	"downloaded_options_und_dt_idx ON downloaded_options(und, dt)";
    create_index_if_missing(cnx, "downloaded_options",
			    "downloaded_options_und_dt_idx",
                            create_dloaded_opts_und_dt_idx);

    char* create_ldrs = "CREATE TABLE leaders( "         \
        "expiry DATE NOT NULL, "                         \
        "stk VARCHAR(16) NOT NULL, "                     \
        "activity INTEGER, "                             \
        "range INTEGER, "                                \
        "opt_spread INTEGER, "                           \
        "atm_price INTEGER, "                            \
        "PRIMARY KEY(expiry, stk))";
    create_table_if_missing(cnx, "leaders", create_ldrs);
    char* create_ldrs_stk_idx = "CREATE INDEX leaders_stk_idx ON " \
        "leaders(stk)";
    create_index_if_missing(cnx, "leaders", "leaders_stk_idx",
                            create_ldrs_stk_idx);

    char* create_setups = "CREATE TABLE setups( "        \
        "dt DATE NOT NULL, "                             \
        "stk VARCHAR(16) NOT NULL, "                     \
        "setup VARCHAR(16) NOT NULL, "                   \
        "direction CHAR(1) NOT NULL, "                   \
        "triggered BOOLEAN NOT NULL, "                   \
        "PRIMARY KEY(dt, stk, setup, direction))";
    create_table_if_missing(cnx, "setups", create_setups);
    char* create_setups_stk_idx = "CREATE INDEX setups_stk_idx ON " \
        "setups(stk)";
    create_index_if_missing(cnx, "setups", "setups_stk_idx",
                            create_setups_stk_idx);
    char* create_setups_dt_idx = "CREATE INDEX setups_dt_idx ON " \
        "setups(dt)";
    create_index_if_missing(cnx, "setups", "setups_dt_idx",
                            create_setups_dt_idx);
    char* create_setups_setup_idx = "CREATE INDEX setups_setup_idx ON " \
        "setups(setup)";
    create_index_if_missing(cnx, "setups", "setups_setup_idx",
                            create_setups_setup_idx);

    char* create_excludes = "CREATE TABLE excludes( "    \
        "stk VARCHAR(16) NOT NULL, "                     \
        "PRIMARY KEY(stk))";
    create_table_if_missing(cnx, "excludes", create_excludes);

    char* create_analyses = "CREATE TABLE analyses( "    \
        "dt DATE NOT NULL, "                             \
        "analysis VARCHAR(32) NOT NULL, "                \
        "PRIMARY KEY(dt, analysis))";
    create_table_if_missing(cnx, "analyses", create_analyses);

    char* create_trades = "CREATE TABLE trades("         \
        "tag VARCHAR(32) NOT NULL,"                              \
        "in_dt DATE NOT NULL,"                           \
        "out_dt DATE NOT NULL,"                          \
        "stk VARCHAR(16) NOT NULL,"                      \
        "setup VARCHAR(16) NOT NULL,"                    \
        "cp VARCHAR(1) NOT NULL,"                        \
        "exp_dt DATE NOT NULL,"                          \
        "strike INTEGER NOT NULL,"                       \
        "in_ask INTEGER,"                                \
        "out_bid INTEGER,"                               \
        "opt_pnl INTEGER NOT NULL,"                      \
        "opt_pct_pnl INTEGER NOT NULL,"                  \
        "moneyness INTEGER NOT NULL,"                    \
        "in_spot INTEGER NOT NULL,"                      \
        "in_range INTEGER NOT NULL,"                     \
        "out_spot INTEGER NOT NULL,"                     \
        "spot_pnl INTEGER NOT NULL,"                     \
        "num_contracts INTEGER NOT NULL,"                \
        "PRIMARY KEY(tag, in_dt, out_dt, stk, setup, cp, exp_dt, strike))";
    create_table_if_missing(cnx, "trades", create_trades);

    /* ALTER TABLE jl_setups ADD COLUMN tm TIME NOT NULL DEFAULT '20:00'; */
    char* create_jl_setups = "CREATE TABLE jl_setups( "  \
        "dt DATE NOT NULL, "                             \
        "stk VARCHAR(16) NOT NULL, "                     \
        "setup VARCHAR(16) NOT NULL, "                   \
        "factor INTEGER NOT NULL, "                      \
        "direction CHAR(1) NOT NULL, "                   \
        "triggered BOOLEAN NOT NULL, "                   \
        "score INTEGER NOT NULL, "                       \
        "info JSONB NOT NULL, "                          \
        "PRIMARY KEY(dt, stk, setup, direction, factor))";
    create_table_if_missing(cnx, "jl_setups", create_jl_setups);
    char* create_jl_setups_stk_idx = "CREATE INDEX jl_setups_stk_idx ON " \
        "jl_setups(stk)";
    create_index_if_missing(cnx, "jl_setups", "jl_setups_stk_idx",
                            create_jl_setups_stk_idx);
    char* create_jl_setups_dt_idx = "CREATE INDEX jl_setups_dt_idx ON " \
        "jl_setups(dt)";
    create_index_if_missing(cnx, "jl_setups", "jl_setups_dt_idx",
                            create_jl_setups_dt_idx);
    char* create_jl_setups_setup_idx = "CREATE INDEX jl_setups_setup_idx ON " \
        "jl_setups(setup)";
    create_index_if_missing(cnx, "jl_setups", "jl_setups_setup_idx",
                            create_jl_setups_setup_idx);

    /**
     *  This table stores the trend (cumulative) setup score,
     *  calculated daily (score decreases by 7/8 each day), and the
     *  trigger score which only applies for the day where the setups
     *  occur.
     */
    char* create_setup_scores = "CREATE TABLE setup_scores( "   \
        "dt DATE NOT NULL, "                                    \
        "stk VARCHAR(16) NOT NULL, "                            \
        "trigger_score INTEGER NOT NULL, "                      \
        "trend_score INTEGER NOT NULL, "                        \
        "PRIMARY KEY(dt, stk))";
    create_table_if_missing(cnx, "setup_scores", create_setup_scores);

    /**
     *  This table stores, for each stock the last date when setups
     *  and daily score were calculated for that stock.
    */
    char* create_setup_dates = "CREATE TABLE setup_dates( " \
        "stk VARCHAR(16) NOT NULL, "                        \
        "dt DATE NOT NULL, "                                \
        "PRIMARY KEY(stk))";
    create_table_if_missing(cnx, "setup_dates", create_setup_dates);

    /**
     *  This table stores additional analytics about the
     *  stocks. Analysis is stored in a JSONB field. To start, we have
     *  rs (relative strength) and rs_rank (getting each stock in a
     *  bucket from 0 to 99).
     */
    char* create_indicators = "CREATE TABLE indicators( "   \
        "stk VARCHAR(16) NOT NULL, "                        \
        "dt DATE NOT NULL, "                                \
        "indicators JSONB NOT NULL, "                       \
        "PRIMARY KEY(stk, dt))";
    create_table_if_missing(cnx, "indicators", create_indicators);

    /**
     *  Store additional analytics about the stocks and/or industry
     *  groups or sectors.  Ticker for the stock or industry/sector.
     *  For industry/sectors, ticker is taken from the industry_groups
     *  table.  Name identifies each analysis.  Value is the indicator
     *  value.  Rank is the absolute rank among all the stocks
     *  evaluated.  BucketRank is the rank fit into 100 buckets (from
     *  0 to 99).
     */
    char* create_indicators_1 = "CREATE TABLE indicators_1( "   \
        "ticker VARCHAR(16) NOT NULL, "                         \
        "dt DATE NOT NULL, "                                    \
        "name VARCHAR(16) NOT NULL, "                           \
        "value INTEGER NOT NULL, "                              \
        "rank INTEGER NOT NULL, "                               \
        "bucket_rank INTEGER NOT NULL, "                        \
        "info JSONB NOT NULL DEFAULT '{}', "                    \
        "PRIMARY KEY(ticker, dt, name))";
    create_table_if_missing(cnx, "indicators_1", create_indicators_1);

    /**
     *  This table stores ETFs: ticker, name, and category
     */
    char* create_etfs = "CREATE TABLE etfs( "   \
        "ticker VARCHAR(16) NOT NULL, "         \
        "name VARCHAR, "                        \
        "category VARCHAR, "                    \
        "PRIMARY KEY(ticker))";
    create_table_if_missing(cnx, "etfs", create_etfs);

    /**
     *  This table stores stock to ETFs mappings
     */
    char* create_stk_etfs = "CREATE TABLE stk_etfs( "   \
        "stk VARCHAR(16) NOT NULL, "                    \
        "etf VARCHAR(16) NOT NULL, "                    \
        "PRIMARY KEY(stk, etf))";
    create_table_if_missing(cnx, "stk_etfs", create_stk_etfs);

    /**
     *  This table stores stock to industry groups mappings
     */
    char* create_stk_ind_groups = "CREATE TABLE ind_groups( "   \
        "stk VARCHAR(16) NOT NULL, "                            \
        "dt DATE NOT NULL, "                                    \
        "source VARCHAR(16) NOT NULL, "                         \
        "industry VARCHAR NOT NULL, "                           \
        "sector VARCHAR NOT NULL, "                             \
        "profile VARCHAR NOT NULL, "                            \
        "PRIMARY KEY(stk, dt, source))";
    create_table_if_missing(cnx, "ind_groups", create_stk_ind_groups);

    /**
     *  This table stores the ids and mappings of sectors and industry
     *  groups
     */
    char* create_sectors_industries = "CREATE TABLE sectors_industries( "   \
        "dt DATE NOT NULL, "                                                \
        "group_type VARCHAR NOT NULL, "                                     \
        "group_name VARCHAR NOT NULL, "                                     \
        "group_id VARCHAR(16) NOT NULL,"                                    \
        "sector_id VARCHAR(16) NOT NULL, "                                  \
        "PRIMARY KEY(dt, group_type, group_name))";
    create_table_if_missing(cnx, "sectors_industries",
                            create_sectors_industries);

    /**
     *  New setup table, designed to store both intraday and eod
     *  setups.  Each setup has a time associated with it.  20:00 is
     *  the default time, and it means this is either an EOD setup, or
     *  we don't know the time at which it occured intraday.
     */
    char* create_time_setups = "CREATE TABLE time_setups( "  \
        "dt DATE NOT NULL, "                                 \
        "stk VARCHAR(16) NOT NULL, "                         \
        "setup VARCHAR(16) NOT NULL, "                       \
        "factor INTEGER NOT NULL, "                          \
        "direction CHAR(1) NOT NULL, "                       \
        "triggered BOOLEAN NOT NULL, "                       \
        "tm TIME NOT NULL DEFAULT '20:00',"                  \
        "info JSONB NOT NULL, "                              \
        "PRIMARY KEY(dt, stk, setup, direction, factor))";
    create_table_if_missing(cnx, "time_setups", create_time_setups);
    char* create_time_setups_stk_idx = "CREATE INDEX time_setups_stk_idx ON " \
        "time_setups(stk)";
    create_index_if_missing(cnx, "time_setups", "time_setups_stk_idx",
                            create_time_setups_stk_idx);
    char* create_time_setups_dt_idx = "CREATE INDEX time_setups_dt_idx ON " \
        "time_setups(dt)";
    create_index_if_missing(cnx, "time_setups", "time_setups_dt_idx",
                            create_time_setups_dt_idx);
    char* create_time_setups_setup_idx = "CREATE INDEX time_setups_setup_idx " \
        "ON time_setups(setup)";
    create_index_if_missing(cnx, "time_setups", "time_setups_setup_idx",
                            create_time_setups_setup_idx);

    /**
     *  This table stores intraday data
     */
    char* create_intraday = "CREATE TABLE intraday( "        \
        "stk VARCHAR(16) NOT NULL, "                         \
        "dt TIMESTAMP NOT NULL,"                             \
        "o INTEGER NOT NULL, "                               \
        "hi INTEGER NOT NULL, "                              \
        "lo INTEGER NOT NULL, "                              \
        "c INTEGER NOT NULL, "                               \
        "v INTEGER, "                                        \
        "oi INTEGER, "                                       \
        "PRIMARY KEY(stk, dt))";
    create_table_if_missing(cnx, "intraday", create_intraday);
    char* create_intraday_dt_idx = "CREATE INDEX intraday_dt_idx "  \
        "ON intraday(dt)";
    create_index_if_missing(cnx, "intraday", "intraday_dt_idx",
                            create_intraday_dt_idx);

    /**
     *  This table stores the market caches
     */
    char* create_market_caches = "CREATE TABLE market_caches( "      \
        "mkt_name VARCHAR(128) NOT NULL, "                           \
        "mkt_date DATE NOT NULL, "                                   \
        "mkt_update_dt TIMESTAMP NOT NULL, "                         \
        "mkt_cache JSONB NOT NULL, "                                 \
        "realtime BOOLEAN NOT NULL, "                                \
        "PRIMARY KEY(mkt_name))";
    create_table_if_missing(cnx, "market_caches", create_market_caches);

    PQfinish(cnx);
    return 0;
}
