CREATE TABLE intraday_tmp (LIKE intraday INCLUDING INDEXES);
INSERT INTO intraday_tmp (SELECT stk, dt + INTERVAL '60 min', o, hi, lo, c, v, oi FROM intraday WHERE DATE(dt) BETWEEN '2022-10-31' AND '2022-11-04');
DELETE FROM intraday WHERE DATE(dt) BETWEEN '2022-10-31' AND '2022-11-04';
INSERT INTO intraday (SELECT * FROM intraday_tmp);
DROP TABLE intraday_tmp;
