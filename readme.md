# pg_vacuum

This python program determines whether a vacuum/analyze/freeze should be done and if so, which one.

(c) 2018-2022 SQLEXEC LLC
<br/>
GNU V3 and MIT licenses are conveyed accordingly.
<br/>
Bugs can be reported @ michaeldba@sqlexec.com


## History
The first version of this program was created in 2018.  
Program renamed from optimize_db.py to pg_vacuum.py (December 2020)

## Overview
This program is useful to identify and vacuum tables.  Most inputs are optional, and either an optional parameter is not used or a default value is used if not provided.  That means you can override internal parameters by specifying them on the command line.  The latest version of pg_vacuum incorporates parallel vacuuming if maintenance workers are available.  Here are the parameters:
<br/>
`-H --host`              host name
<br/>
`-d --dbname`            database name
<br/>
`-p --dbport`            database port
<br/>
`-U --dbuser`            database user
<br/>
`-s --maxsize`           max table size that will be considered (default, 400GB)
<br/>
`-y --analyzemaxdays`    Analyzes older than this will be considered
<br/>
`-x --vacuummaxdays`     Vacuums older than this will be considered
<br/>
`-t --mindeadtups`       minimum dead tups before considering a vacuum
<br/>
`-b --maxtables`         max number of tables to vacuum (default 9999)
<br/>
`-m --schema`            if provided, perform actions only on this schema
<br/>
`-z --pctfreeze`         specifies how close to wraparound before FREEZE is done.
<br/>
`-f --freeze`            perform freeze if necessary
<br/>
`-r --dryrun`            do a dry run for analysis before actually running it.
<br/>
`-q --inquiry`           show stats to validate run.  Best used with dryrun. Values: "all" | "found" | not specified
<br/>
`-i --ignoreparts`       ignore partitioned tables
<br/>
`-a --async`             run async jobs ignoring thresholds
<br/>
`-n --nullsonly`         Only consider tables with no vacuum or analyze history
<br/>
`-c --check`             Only Check stats on tables
<br/>
`-v --verbose`           Used primarily for debugging
<br/>

<br/>

## Requirements
1. python 2.7 or above
2. python packages: psycopg2
3. Works on Linux and Windows.
4. PostgreSQL versions 9.6 and up
<br/>

## Assumptions
1. Only when a table is within 25 million of reaching the wraparound threshold is it considered a FREEZE candidate. 
2. By default, catalog tables are ignored unless specified explicitly with the --schema option.
3. If passwords are required (authentication <> trust), then you must define credentials in the .pgpass (linux)/pgpass.conf (windows) files.
4. The less parameters you supply, the more wide-open the vacuum operation, i.e., more tables qualify
<br/>

## Examples
`Vacuum all tables that don't have any vacuums/analyzes. Only do tables less that 100MB in size. Bypass partitioned tables. Dryrun first.`<br/>
pg_vacuum.py -H localhost -d testing -p 5432 -U postgres --maxsize 1000000000 --nullsonly --ignoreparts --dryrun
<br/><br/>
`Same as before but only do it for the first 50 tables.`<br/>
pg_vacuum.py -H localhost -d testing -p 5432 -U postgres -s 1000000000 --nullsonly --ignoreparts --dryrun -b 50
<br/><br/>
`Same as before but only do it for a specific schema.`<br/>
pg_vacuum.py -H localhost -d testing -p 5432 -U postgres -s 1000000000 --nullsonly --ignoreparts --dryrun -b 50 --schema concept
<br/><br/>
`Vacuum tables that haven't been vacuumed in 10 days, 20 days for analyzes. Dryrun first.`<br/>
pg_vacuum.py -H localhost -d testing -p 5432 -U postgres -s 1000000000 -x 20 -y 20 --dryrun
<br/><br/>
`Vacuum tables that have more than 1000 dead tuples and haven't been vacuumed in 20 days. Dryrun first.`<br/>
pg_vacuum.py -H localhost -d testing -p 5432 -U postgres -s 1000000000 -x 20 -y 20 -t 1000 --dryrun
<br/><br/>
`Run a check to get the overall status of vacuuming in the database.`<br/>
pg_vacuum.py -H localhost -d testing -p 5432 -U postgres -s 1000000000 --check
<br/><br/>
`Vacuum Freeze tables that are at the 90% threshold for transaction wrap-around to kick in.`<br/>
pg_vacuum.py -H localhost -d testing -p 5432 -U postgres -s 1000000000 --pctfreeze 90 --freeze --dryrun
<br/><br/>
<br/><br/>


