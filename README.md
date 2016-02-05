# LLAP For Da Win

Postgres is fast, it is stable and best of all [flexible](http://www.postgresql.org/docs/9.5/static/postgres-fdw.html).

With the help of some small libraries and Postgres, we can turn Postgres into a service view for LLAP.

![Small Demo](https://people.apache.org/~gopalv/llap_fdw_opt.gif)

## Install 

This needs Postgres 9.5 to run + a hive-2.0 JDBC server (LLAP ideal).

Most of the dep RPMs are in srpms/ directory. So, first get all those built and installed.

    make deps install

## Create FDW server

    CREATE SERVER llap foreign data wrapper multicorn OPTIONS (wrapper 'llap_fdw.LlapFdw', hostname '<host>', port '10000');
    CREATE SCHEMA llap0;
    import foreign schema <hive-db> from server llap INTO llap0;

  
## Meanwhile in Hive

Create the CUBE view

    0: jdbc:hive2://localhost:10000/> create view dependent_dist as select cd_gender, cd_marital_status, count(1) from customer_demographics where cd_dep_college_count > 0 group by cd_marital_status, cd_gender WITH CUBE;
    No rows affected (0.506 seconds)


## Put it all together

    psql (9.5.0)
    Type "help" for help.
    gopal=# import foreign schema tpcds_bin_partitioned_orc_200 from server llap INTO llap0;
    WARNING:  We are attemping to import tpcds_bin_partitioned_orc_200.date_dim
    WARNING:  We are attemping to import tpcds_bin_partitioned_orc_200.date_dim_dates
    WARNING:  We are attemping to import tpcds_bin_partitioned_orc_200.dependent_dist
    ...
    gopal=#\timing
    Timing is on.
    gopal=# select * from llap0.date_dim where d_date_sk = 2415022;
    WARNING:  [d_date_sk = 2415022]
    WARNING:  select `d_date`,`d_current_week`,`d_week_seq`,`d_current_day`,`d_first_dom`,`d_moy`,`d_holiday`,`d_month_seq`,`d_current_year`,`d_fy_quarter_seq`,`d_current_quarter`,`d_year`,`d_weekend`,`d_quarter_seq`,`d_date_id`,`d_following_holiday`,`d_fy_year`,`d_same_day_lq`,`d_qoy`,`d_current_month`,`d_same_day_ly`,`d_dom`,`d_date_sk`,`d_last_dom`,`d_fy_week_seq`,`d_day_name`,`d_quarter_name`,`d_dow` from `date_dim`  where (`d_date_sk` = %s)
    d_date_sk |    d_date_id     |   d_date   | d_month_seq | d_week_seq | d_quarter_seq | d_year | d_dow | d_moy | d_dom | d_qoy | d_fy_year | d_fy_quarter_seq | d_fy_week_seq | d_day_name | d_quarter_name | d_holiday | d_weekend | d_following_holiday | d_first_dom | d_last_dom | d_same_day_ly | d_same_day_lq | d_current_day | d_current_week | d_current_month | d_current_quarter | d_current_year 
    -----------+------------------+------------+-------------+------------+---------------+--------+-------+-------+-------+-------+-----------+------------------+---------------+------------+----------------+-----------+-----------+---------------------+-------------+------------+---------------+---------------+---------------+----------------+-----------------+-------------------+----------------
    2415022 | AAAAAAAAOKJNECAA | 1900-01-02 |           0 |          1 |             1 |   1900 |     1 |     1 |     2 |     1 |      1900 |                1 |             1 | Monday     | 1900Q1       | N         | N         | Y                   |     2415021 |    2415020 |       2414657 |       2414930 | N             | N              | N               | N                 | N
    (1 row)
    Time: 507.421 ms


## Query the CUBE view with filters

    gopal=# select * from llap0.dependent_dist where cd_gender = 'F';
    WARNING:  [cd_gender = F]
    WARNING:  select `_c2`,`cd_marital_status`,`cd_gender` from `dependent_dist`  where (`cd_gender` = %s)
     cd_gender | cd_marital_status |  _c2   
    -----------+-------------------+--------
     F         |                   | 823200
     F         | D                 | 164640
     F         | M                 | 164640
     F         | S                 | 164640
     F         | U                 | 164640
     F         | W                 | 164640
    (6 rows)
    Time: 2302.497 ms

## Materialize the view 

    gopal=# create materialized view llap0.dd as select * from llap0.dependent_dist WITH NO DATA;
    SELECT 0
    Time: 69.823 ms
    gopal=# REFRESH MATERIALIZED VIEW llap0.dd;
    WARNING:  []
    WARNING:  select `_c2`,`cd_marital_status`,`cd_gender` from `dependent_dist` 
    REFRESH MATERIALIZED VIEW
    Time: 2232.456 ms

## Run sub-millisecond queries from your web-apps

    gopal=# select * from llap0.dd where cd_gender='F';
     cd_gender | cd_marital_status |  _c2   
       -----------+-------------------+--------
        F         |                   | 823200
        F         | D                 | 164640
        F         | M                 | 164640
        F         | S                 | 164640
        F         | U                 | 164640
        F         | W                 | 164640
        (6 rows)
      Time: 0.580 ms

## Use the CUBE columns for concurrent/lazy refreshes

        gopal=# CREATE UNIQUE INDEX dd_u_idx ON llap0.dd (cd_gender, cd_marital_status);
        gopal=# REFRESH MATERIALIZED VIEW CONCURRENTLY llap0.dd;
        WARNING:  []
        WARNING:  select `_c2`,`cd_marital_status`,`cd_gender` from `dependent_dist` 
        REFRESH MATERIALIZED VIEW
        Time: 1627.088 ms
