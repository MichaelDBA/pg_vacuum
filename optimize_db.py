#!/usr/bin/env python
# may have to use python2 instead of python on above line. CentOS 8
##################################################################################################
# optimize_db.py
#
# author: Michael Vitale, michaeldba@sqlexec.com
#
# Description: This program does dynamic FREEZEs, VACUUMs, and ANALYZEs based on PG statistics.
#              For large tables, it does these asynchronously, synchronous for all others.
#              The program constrains the number of processes running in asynchronous mode.
#              The program waits occasionally for 5 minutes if max processes is still surpassed.
#              The program avoids extremely large tables over 400GB, expecting them to be done manually.
#
# Date Created : June 19, 2016    Original Coding (v 1.0)
#                                 Tested with Ubuntu 14.04, PostgreSQL v9.6, Python 2.7
# Date Modified:
# March 15, 2019.  Updated for Python 3. Added asynchronous/monitoring logic.
#                                 Tested with Redhat 7.5, PostgreSQL v10, Python 3.6.3
# March 26, 2019.  Added Freeze logic.
# March 29, 2019.  Added better exception handling.
# April 25, 2019.  Added threshold for dead tups to parameters instead of hard coded default of 10,000.
#                  Fixed small table analyze query that didn't bring back correct result set.
# April 27, 2019.  Fixed my fix for small table analyze query. ha.
# May   24, 2019.  V2.3: Fixed vacuum/analyze check due to date comparison problem. Also reduced dead tuple threshold from 10,000 to 1,000
#                        Also added additional query to do vacuums/analyzes on tables not touched for over 2 weeks
# July  18, 2019.  V2.4: Fixed vacuum/analyze check query (Step 2).  Wasn't catching rows where no vacuums/analyzes ever done.
# Aug.  04, 2019.  V2.5: Add new parameter to do VACUUM FREEZE: --freeze. Default is not, but dryrun will show what could have been done.
# Dec.  15, 2019.  V2.6: Add schema filter support. Fixed bugs: not skipping duplicate tables, invalid syntax for vacuum with 2+ parms where need to be in parens.
# Dec.  16, 2019.  V2.6: Replace optparse with argparse which fixes a bug with optparse. Added freeze threshold logic. Fixed nohup async calls which left out psql connection parms.
# Mar.  18, 2020.  V2.7: Add option to run inquiry queries to validate work to be done. Fixed bug where case-sensitive table names caused errors. Added signal interrupts.
# Sept. 13, 2020.  V2.8: Fixed bug in dryrun mode where inquiry section was not indented correctly causing an exception.
#
# Notes:
#   1. Do not run this program multiple times since it may try to vacuum or analyze the same table again
#      since the logic is based on timestamps that are not updated until AFTER the action is done.
#
# call example with all parameters:
# optimize_db.py -H localhost -d testing -p 5432 -u postgres --maxsize 400000000000 --maxdays 1 --mindeadtups 1000 --schema public --inquiry --dryrun
# optimize_db.py -H localhost -d testing -p 5432 -u postgres -s 400000000000 -y 1 -t 1000 -m public --freeze
#
# crontab example that runs every morning at 3am local time and will vacuum if more than 5000 dead tuples and/or its been over 5 days since the last vacuum/analyze.
# SHELL=/bin/sh
# PATH=<your binary paths as postgres user>
# 00 03 * * * /home/postgres/mjv/optimize_db.py -H localhost -d <dbname> -u postgres -p 5432 -y 5 -t 5000 --dryrun >/home/postgres/mjv/optimize_db_`/bin/date +'\%Y-\%m-\%d-\%H.\%M.\%S'`.log 2>&1
#
##################################################################################################
import sys, os, threading, argparse, time, datetime, signal
from optparse import OptionParser
import psycopg2
import subprocess

version = '2.8  Sept. 13, 2020'
OK = 0
BAD = -1

fmtrows  = '%11d'
fmtbytes = '%13d'

1,000,000,000,000
#threshold for freezing tables: 25 million rows from wraparound
threshold_freeze = 25000000

# minimum dead tuples
threshold_dead_tups = 1000

# last vacuum or analyze
threshold_max_days = 5

# 200 million row threshold
threshold_async_rows = 200000000

# 400 GB threshold, above this table actions are deferred
threshold_max_size = 400000000000

# 50 MB minimum threshold
threshold_min_size = 50000000

# 50 GB threshold, above this table actions are done asynchronously
threshold_max_sync = 50000000000

# max async processes
threshold_max_processes = 12

# load threshold, wait for a time if very high
load_threshold = 250

def signal_handler(signal, frame):
     printit('User-interrupted!')
     sys.exit(1)

def printit(text):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    txt = now + ' ' + text
    print (txt)
    # don't use v3 way to flush output since we want this to work for v2 and 3
    #print (txt, flush=True)
    sys.stdout.flush()
    return

def execute_cmd(text):
    rc = os.system(text)
    return rc

def get_process_cnt():
    cmd = "ps -ef | grep 'VACUUM VERBOSE\|ANALYZE VERBOSE' | grep -v grep | wc -l"
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    result = int(result.decode('ascii'))
    return result

def highload():
    cmd = "uptime | sed 's/.*load average: //' | awk -F\, '{print $1}'"
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    min1  = str(result.decode())
    loadn = int(float(min1))
    cmd = "cat /proc/cpuinfo | grep processor | wc -l"
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    cpus  = str(result.decode())
    cpusn = int(float(cpus))
    load = (loadn / cpusn) * 100
    if load < load_threshold:
        return False
    else:
        printit ("High Load: loadn=%d cpusn=%d load=%d" % (loadn, cpusn, load))
        return True

'''
func requires psutil package
def getload():
    import psutil, os, sys
    #cpu_percent = float(psutil.cpu_percent())
    total_cpu=float(psutil.cpu_count())
    load_average=os.getloadavg()
    load_now=float(load_average[0])
    print "load average=%s" % (load_average,) # print "total cpu=%d" % total_cpu # print "load now=%d" % load_now

    if (load_now > total_cpu):
        cpu_usage = ("CPU is :" + str(cpu_percent))
        Num_CPU = ("Number of CPU's : " + str(total_cpu))
        Load_Average = ("Load Average is : " + str(load_average))
        load_out = open("/tmp/load_average.out", "w")
        load_out.write(Num_CPU + "\n" + Load_Average);
        load_out.close();
        os.system("mail -s \"Load Average is Higher than Number of CPU's abc@abc.com 123@123.com < /tmp/load_average.out")
'''

def get_query_cnt(conn, cur):
    sql = "select count(*) from pg_stat_activity where state = 'active' and query like 'VACUUM FREEZE VERBOSE%' OR query like 'VACUUM ANALYZE VERBOSE%' OR query like 'VACUUM VERBOSE%' OR query like 'ANALYZE VERBOSE%'"
    cur.execute(sql)
    rows = cur.fetchone()
    return int(rows[0])

def get_vacuums_in_progress(conn, cur):
    sql = "SELECT 'tables', array_agg(relid::regclass) from pg_stat_progress_vacuum group by 1"
    cur.execute(sql)
    rows = cur.fetchone()
    return rows[1]

def skip_table (atable, tablist):
    if atable in tablist:
        return True
    else:
        return False

def wait_for_processes(conn,cur):
    cnt = 0
    while True:
        rc = get_query_cnt(conn, cur)
        cnt = cnt + 1
        if cnt > 20:
            printit ("NOTE: Program ending, but vacuums/analyzes(%d) still in progress." % (rc))
            break
        if rc > 0:
            tables = get_vacuums_in_progress(conn, cur)
            printit ("NOTE: vacuums still running: %d (%s) Waiting another 5 minutes before exiting..." % (rc, tables))
            time.sleep(300)
        else:
            break
    return


####################
# MAIN ENTRY POINT #
####################

# Register the signal handler for CNTRL-C logic
signal.signal(signal.SIGINT, signal_handler)

#while True:
#    print('Waiting...')
#    time.sleep(5)

# Delay if high load encountered, give up after 30 minutes.
cnt = 0
while True:
    if highload():
        printit("Deferring program start for another 5 minutes while high load encountered.")
        cnt = cnt + 1
        time.sleep(300)
        if cnt > 5:
            printit("Aborting program due to high load.")
            sys.exit(1)
    else:
        break

total_freezes = 0
total_vacuums_analyzes = 0
total_vacuums  = 0
total_analyzes = 0
tables_skipped = 0
tablist = []

# Setup up the argument parser
# parser = OptionParser("PostgreSQL Vacumming Tool", add_help_option=False)
'''
# parser = OptionParser("PostgreSQL Vacumming Tool", add_help_option=True)
parser.add_option("-r", "--dryrun", dest="dryrun",   help="dry run", default=False, action="store_true", metavar="DRYRUN")
parser.add_option("-f", "--freeze", dest="freeze",   help="vacuum freeze directive", default=False, action="store_true", metavar="FREEZE")
parser.add_option("-H", "--host",   dest="hostname", help="host name",      type=str, default="localhost",metavar="HOSTNAME")
parser.add_option("-d", "--dbname", dest="dbname",   help="database name",  type=str, default="",metavar="DBNAME")
parser.add_option("-u", "--dbuser", dest="dbuser",   help="database user",  type=str, default="postgres",metavar="DBUSER")
parser.add_option("-m", "--schema",dest="schema",    help="schema",         type=str, default="",metavar="SCHEMA")
parser.add_option("-p", "--dbport", dest="dbport",   help="database port",  type=int, default="5432",metavar="DBPORT")
parser.add_option("-s", "--maxsize",dest="maxsize",  help="max table size", type=int, default=-1,metavar="MAXSIZE")
parser.add_option("-y", "--maxdays",dest="maxdays",  help="max days",       type=int, default=5,metavar="MAXDAYS")
iparser.add_option("-t", "--mindeadtups",dest="mindeadtups",  help="min dead tups", type=int, default=10000,metavar="MINDEADTUPS")
parser.add_option("-q", "--inquiry", dest="inquiry",   help="inquiry queries requested", default=False, action="store_true", metavar="INQUIRY")
(options,args) = parser.parse_args()
'''
parser = argparse.ArgumentParser("PostgreSQL Vacumming Tool", add_help=True)
parser.add_argument("-r", "--dryrun", dest="dryrun",          help="dry run",        default=False, action="store_true")
parser.add_argument("-f", "--freeze", dest="freeze",          help="vacuum freeze directive", default=False, action="store_true")
parser.add_argument("-H", "--host",   dest="hostname",        help="host name",      type=str, default="localhost",metavar="HOSTNAME")
parser.add_argument("-d", "--dbname", dest="dbname",          help="database name",  type=str, default="",metavar="DBNAME")
parser.add_argument("-u", "--dbuser", dest="dbuser",          help="database user",  type=str, default="postgres",metavar="DBUSER")
parser.add_argument("-m", "--schema",dest="schema",           help="schema",         type=str, default="",metavar="SCHEMA")
parser.add_argument("-p", "--dbport", dest="dbport",          help="database port",  type=int, default="5432",metavar="DBPORT")
parser.add_argument("-s", "--maxsize",dest="maxsize",         help="max table size", type=int, default=-1,metavar="MAXSIZE")
parser.add_argument("-y", "--maxdays",dest="maxdays",         help="max days",       type=int, default=5,metavar="MAXDAYS")
parser.add_argument("-z", "--pctfreeze",dest="pctfreeze",     help="max pct until wraparound", type=int, default=90, metavar="PCTFREEZE")
parser.add_argument("-t", "--mindeadtups",dest="mindeadtups", help="min dead tups",  type=int, default=10000,metavar="MINDEADTUPS")
parser.add_argument("-q", "--inquiry", dest="inquiry",        help="inquiry queries requested", default=False, action="store_true")
args = parser.parse_args()

dryrun = False
freeze = False
if args.dryrun:
    dryrun = True
if args.freeze:
    freeze = True;

if args.dbname == "":
    printit("DB Name must be provided.")
    sys.exit(1)

# if args.maxsize <> -1:
if args.maxsize != -1:
    # use user-provided max instead of program default (300 GB)
        threshold_max_size = args.maxsize

dbname   = args.dbname
hostname = args.hostname
dbport   = args.dbport
dbuser   = args.dbuser
schema   = args.schema
threshold_max_days = args.maxdays
min_dead_tups = args.mindeadtups
pctfreeze = args.pctfreeze
if pctfreeze > 99 or pctfreeze < 10:
    printit("pctfreeze must range between 10 and 99.")
    sys.exit(1)

if min_dead_tups > 100:
    threshold_dead_tups = min_dead_tups

inquiry = args.inquiry
printit ("version: *** %s ***  Parms: dryrun(%r) inquiry(%r) freeze(%r)  host:%s dbname=%s schema=%s dbuser=%s dbport=%d max days: %d  min dead tups: %d  max table size: %d  pct freeze: %d" \
        % (version, dryrun, inquiry, freeze, hostname, dbname, schema, dbuser, dbport, threshold_max_days, threshold_dead_tups, threshold_max_size, pctfreeze ))

# printit ("Exiting program prematurely for debug purposes.")
# sys.exit(0)

# Connect
try:
    # conn = psycopg2.connect("dbname=testing user=postgres host=locahost password=postgrespass")
    # connstr = "dbname=%s port=%d user=%s host=%s password=postgrespass" % (dbname, dbport, dbuser, hostname )
    connstr = "dbname=%s port=%d user=%s host=%s application_name=%s" % (dbname, dbport, dbuser, hostname, 'optimize_db' )
    conn = psycopg2.connect(connstr)
except psycopg2.Error as e:
    printit ("Database Connection Error: %s" % (e))
    sys.exit(1)

printit ("connected to database successfully.")

# to run vacuum through the psycopg2 driver, the isolation level must be changed.
old_isolation_level = conn.isolation_level
conn.set_isolation_level(0)

# Open a cursor to perform database operation
cur = conn.cursor()

active_processes = 0


#################################
# 1. Freeze Tables              #
#################################
'''
-- all
SELECT n.nspname || '.' || c.relname as table, c.reltuples::bigint as rows, age(c.relfrozenxid) as xid_age, CAST(current_setting('autovacuum_freeze_max_age') AS bigint) as freeze_max_age,
CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint as howclose,
pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty, pg_total_relation_size(c.oid) as table_size FROM pg_class c, pg_namespace n WHERE n.nspname not in ('pg_toast') and n.oid = c.relnamespace and c.relkind not in ('i','v','S','c') AND CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint > 1::bigint and  CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint < 25000000 ORDER BY age(c.relfrozenxid) DESC LIMIT 60;

-- public schema
SELECT n.nspname || '.' || c.relname as table, c.reltuples::bigint as rows, age(c.relfrozenxid) as xid_age, CAST(current_setting('autovacuum_freeze_max_age') AS bigint) as freeze_max_age,
CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint as howclose,
pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty, pg_total_relation_size(c.oid) as table_size FROM pg_class c, pg_namespace n WHERE n.nspname = 'public' and n.oid = c.relnamespace and c.relkind not in ('i','v','S','c') AND CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint > 1::bigint and  CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint < 25000000 ORDER BY age(c.relfrozenxid) DESC LIMIT 60;
'''
if schema == "":
   sql = "SELECT n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint as rows, age(c.relfrozenxid) as xid_age, CAST(current_setting('autovacuum_freeze_max_age') AS bigint) as freeze_max_age, " \
      "CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint as howclose, pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty,  " \
      "pg_total_relation_size(c.oid) as table_size FROM pg_class c, pg_namespace n WHERE n.nspname not in ('pg_toast') and n.oid = c.relnamespace and c.relkind not in ('i','v','S','c') AND CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - " \
      "age(c.relfrozenxid)::bigint > 1::bigint and  CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint < %d ORDER BY age(c.relfrozenxid) DESC LIMIT 60" % (threshold_freeze)
else:
   sql = "SELECT n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint as rows, age(c.relfrozenxid) as xid_age, CAST(current_setting('autovacuum_freeze_max_age') AS bigint) as freeze_max_age, " \
      "CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint as howclose, pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty,  " \
      "pg_total_relation_size(c.oid) as table_size FROM pg_class c, pg_namespace n WHERE n.nspname = '%s' and n.oid = c.relnamespace and c.relkind not in ('i','v','S','c') AND CAST(current_setting('autovacuum_freeze_max_age') " \
      "AS bigint) - age(c.relfrozenxid)::bigint > 1::bigint and  CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint < %d ORDER BY age(c.relfrozenxid) DESC LIMIT 60" % (schema, threshold_freeze)
try:
     cur.execute(sql)
#except psycopg2.Error, e:
except psycopg2.Error as e:
    printit ("Select Error: %s" % (e))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No FREEZEs need to be done.")
else:
    printit ("VACUUM FREEZEs to be processed=%d.  Includes deferred ones too." % len(rows) )

cnt = 0
action_name = 'VACUUM FREEZE'
if not dryrun and len(rows) > 0 and not freeze:
    printit ('Bypassing VACUUM FREEZE action for %d tables. Otherwise specify "--freeze" to do them.' % len(rows))

for row in rows:
    if not freeze and not dryrun:
        continue
    if active_processes > threshold_max_processes:
        # see how many are currently running and update the active processes again
        # rc = get_process_cnt()
        rc = get_query_cnt(conn, cur)
        if rc > threshold_max_processes:
            printit ("Current process cnt(%d) is still higher than threshold (%d). Sleeping for 5 minutes..." % (rc, threshold_max_processes))
            time.sleep(300)
        else:
            printit ("Current process cnt(%d) is less than threshold (%d).  Processing will continue..." % (rc, threshold_max_processes))
        active_processes = rc

    cnt = cnt + 1
    table    = row[0]
    tups     = row[1]
    xidage   = row[2]
    maxage   = row[3]
    howclose = row[4]
    sizep    = row[5]
    size   =   row[6]

    # also bypass tables that are less than 15% of max age
    pctmax = float(xidage) / float(maxage)
    # print("maxage=%10f  xidage=%10f  pctmax=%4f  pctfreeze=%4f" % (maxage, xidage, pctmax, pctfreeze))
    if (100 * pctmax) < float(pctfreeze):
       printit ("Async %15s  %03d %-52s rows: %11d  size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d  pct: %d: Defer" \
               % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose, 100 * pctmax))
       tables_skipped = tables_skipped + 1
       continue

       if size > threshold_max_size:
          # defer action
          printit ("Async %15s  %03d %-52s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d NOTICE: Skipping large table.  Do manually." \
                  % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose))
          tables_skipped = tables_skipped + 1
          continue
    elif tups > threshold_async_rows or size > threshold_max_sync:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d  pct: %d" % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose, (100 * pctmax)))
            total_freezes = total_freezes + 1
            tablist.append(table)
            active_processes = active_processes + 1
        else:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (FREEZE, VERBOSE) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, table)
            print(cmd)
            time.sleep(0.5)
            rc = execute_cmd(cmd)
            total_freezes = total_freezes + 1
            tablist.append(table)
            active_processes = active_processes + 1

    else:
        if dryrun:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d  pct: %d" % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose, (100 * pctmax)))
            total_freezes = total_freezes + 1
        else:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d  pct: %d" % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose, (100 * pctmax)))
            sql = "VACUUM (FREEZE, VERBOSE) %s" % table
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except psycopg2.Warning, w:
                printit("Warning: %s %s %s" % (w.pgcode, w.diag.severity, w.diag.message_primary))
                continue
            except psycopg2.Error, e:
                printit("Error  : %s %s %s" % (e.pgcode, e.diag.severity, e.diag.message_primary))
                continue
            total_freezes = total_freezes + 1
            tablist.append(table)


# if action is freeze just exit at this point gracefully
if freeze:
   conn.close()
   printit ("End of Freeze action.  Closing the connection and exiting normally.")
   sys.exit(0)


#################################
# 2. Vacuum and Analyze query
#    older than threshold date OR (dead tups greater than threshold and table size greater than threshold min size)
#################################

# V2.3: Fixed query date problem
#V 2.4 Fix, needed to add logic to check for null timestamps!
'''
-- all
SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND
(pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) > 50000000 AND u.n_dead_tup > 1000 AND (now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 5 AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 5) OR ((last_analyze IS NULL AND last_autoanalyze IS NULL) OR (last_vacuum IS NULL AND last_autovacuum IS NULL))) order by 4,1;

-- public schema only
SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and and n.nspname = 'public' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND
(pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) > 50000000 AND u.n_dead_tup > 1000 AND (now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 5 AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 5) OR ((last_analyze IS NULL AND last_autoanalyze IS NULL) OR (last_vacuum IS NULL AND last_autovacuum IS NULL))) order by 4,1;

'''
if schema == "":
   sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
      "u.n_dead_tup::bigint AS dead_tup, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
      "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze  " \
      "FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname  " \
      "and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND (u.n_dead_tup > %d AND   pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) > %d AND " \
      "(now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d) OR ((last_analyze IS NULL AND last_autoanalyze IS NULL) OR " \
      "(last_vacuum IS NULL AND last_autovacuum IS NULL))) order by 4,1" % (threshold_dead_tups, threshold_min_size, threshold_max_days, threshold_max_days)
else:
   sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
      "u.n_dead_tup::bigint AS dead_tup, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
      "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze  " \
      "FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = '%s' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname  " \
      "and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND (u.n_dead_tup > %d AND   pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) > %d AND " \
      "(now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d) OR ((last_analyze IS NULL AND last_autoanalyze IS NULL) OR " \
      "(last_vacuum IS NULL AND last_autovacuum IS NULL))) order by 4,1" % (schema, threshold_dead_tups, threshold_min_size, threshold_max_days, threshold_max_days)
try:
     cur.execute(sql)
#except psycopg2.Error, e:
except psycopg2.Error as e:
    printit ("Select Error: %s" % (e))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No vacuum/analyze pairs to be done.")
else:
    printit ("vacuums/analyzes to be processed=%d" % len(rows) )

cnt = 0
action_name = 'VACUUM/ANALYZE'
for row in rows:
    if active_processes > threshold_max_processes:
        # see how many are currently running and update the active processes again
        # rc = get_process_cnt()
        rc = get_query_cnt(conn, cur)
        if rc > threshold_max_processes:
            printit ("Current process cnt(%d) is still higher than threshold (%d). Sleeping for 5 minutes..." % (rc, threshold_max_processes))
            time.sleep(300)
        else:
            printit ("Current process cnt(%d) is less than threshold (%d).  Processing will continue..." % (rc, threshold_max_processes))
        active_processes = rc

    cnt = cnt + 1
    table  = row[0]
    sizep  = row[1]
    size   = row[2]
    tups   = row[3]
    live   = row[4]
    dead   = row[5]

    # check if we already processed this table
    if skip_table(table, tablist):
        continue

    if size > threshold_max_size:
        # defer action
        if dryrun:
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, sizep, size, dead))
            tables_skipped = tables_skipped + 1
        continue
    elif tups > threshold_async_rows or size > threshold_max_sync:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            total_vacuums_analyzes = total_vacuums_analyzes + 1
            tablist.append(table)
            active_processes = active_processes + 1
        else:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (ANALYZE, VERBOSE) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, table)

            time.sleep(0.5)
            rc = execute_cmd(cmd)
            total_vacuums_analyzes = total_vacuums_analyzes + 1
            tablist.append(table)
            active_processes = active_processes + 1

    else:
        if dryrun:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            total_vacuums_analyzes = total_vacuums_analyzes + 1
            tablist.append(table)
        else:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            sql = "VACUUM (ANALYZE, VERBOSE) %s" % table
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except psycopg2.Warning, w:
                printit("Warning: %s %s %s cmd=%s" % (w.pgcode, w.diag.severity, w.diag.message_primary, sql))
                continue
            except psycopg2.Error, e:
                printit("Error  : %s %s %s cmd=%s" % (e.pgcode, e.diag.severity, e.diag.message_primary, sql))
                continue

            total_vacuums_analyzes = total_vacuums_analyzes + 1
            tablist.append(table)


#################################
# 3. Vacuum determination query #
#################################
'''
-- all
SELECT psut.schemaname || '.' || psut.relname as table, to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum,  to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,
pg_class.reltuples::bigint AS n_tup,  psut.n_dead_tup::bigint AS dead_tup, pg_size_pretty(pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname))::bigint),
pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname)) as size, to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) +
(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples), '9G999G999G999') AS av_threshold, CASE WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) +
(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples) < psut.n_dead_tup THEN '*' ELSE '' END AS expect_av
FROM pg_stat_user_tables psut JOIN pg_class on psut.relid = pg_class.oid  where psut.n_dead_tup > 10000  OR (last_vacuum is null and last_autovacuum is null) ORDER BY 5 desc, 1;

-- public schema
SELECT psut.schemaname || '.' || psut.relname as table, to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum,  to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,
pg_class.reltuples::bigint AS n_tup,  psut.n_dead_tup::bigint AS dead_tup, pg_size_pretty(pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname))::bigint),
pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname)) as size, to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) +
(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples), '9G999G999G999') AS av_threshold, CASE WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) +
(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples) < psut.n_dead_tup THEN '*' ELSE '' END AS expect_av
FROM pg_stat_user_tables psut JOIN pg_class on psut.relid = pg_class.oid  where psut.schemaname = 'public' and (psut.n_dead_tup > 10000  OR (last_vacuum is null and last_autovacuum is null)) ORDER BY 5 desc, 1;

'''
if schema == "":
   sql = "SELECT psut.schemaname || '.\"' || psut.relname || '\"' as table, to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, " \
      "pg_class.reltuples::bigint AS n_tup,  psut.n_dead_tup::bigint AS dead_tup, pg_size_pretty(pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname))::bigint), " \
      "pg_total_relation_size(quote_ident(psut.schemaname) || '.' ||quote_ident(psut.relname)) as size, to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples), '9G999G999G999') AS av_threshold, CASE WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples) < psut.n_dead_tup THEN '*' ELSE '' END AS expect_av " \
      "FROM pg_stat_user_tables psut JOIN pg_class on psut.relid = pg_class.oid  where psut.n_dead_tup > %d OR (last_vacuum is null and last_autovacuum is null) ORDER BY 5 desc, 1;" % (threshold_dead_tups)
else:
   sql = "SELECT psut.schemaname || '.\"' || psut.relname || '\"' as table, to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, " \
      "pg_class.reltuples::bigint AS n_tup,  psut.n_dead_tup::bigint AS dead_tup, pg_size_pretty(pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname))::bigint), " \
      "pg_total_relation_size(quote_ident(psut.schemaname) || '.' ||quote_ident(psut.relname)) as size, to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples), '9G999G999G999') AS av_threshold, CASE WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples) < psut.n_dead_tup THEN '*' ELSE '' END AS expect_av " \
      "FROM pg_stat_user_tables psut JOIN pg_class on psut.relid = pg_class.oid  where psut.schemaname = '%s' and (psut.n_dead_tup > %d OR (last_vacuum is null and last_autovacuum is null)) ORDER BY 5 desc, 1;" % (schema, threshold_dead_tups)

try:
     cur.execute(sql)
#except psycopg2.Error, e:
except psycopg2.Error as e:
    printit ("Select Error: %s" % (e))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No vacuums to be done.")
else:
    printit ("vacuums to be processed=%d" % len(rows) )

cnt = 0
action_name = 'VACUUM'
for row in rows:
    if active_processes > threshold_max_processes:
        # see how many are currently running and update the active processes again
        # rc = get_process_cnt()
        rc = get_query_cnt(conn, cur)
        if rc > threshold_max_processes:
            printit ("Current process cnt(%d) is still higher than threshold (%d). Sleeping for 5 minutes..." % (rc, threshold_max_processes))
            time.sleep(300)
        else:
            printit ("Current process cnt(%d) is less than threshold (%d).  Processing will continue..." % (rc, threshold_max_processes))
        active_processes = rc

    cnt = cnt + 1
    table= row[0]
    tups = row[3]
    dead = row[4]
    sizep= row[5]
    size = row[6]

    # check if we already processed this table
    if skip_table(table, tablist):
        continue

    if size > threshold_max_size:
        # defer action
        printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, sizep, size, dead))
        tables_skipped = tables_skipped + 1
        continue
    elif tups > threshold_async_rows or size > threshold_max_sync:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            total_vacuums  = total_vacuums + 1
            active_processes = active_processes + 1
        else:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (VERBOSE) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, table)
            time.sleep(0.5)
            rc = execute_cmd(cmd)
            total_vacuums  = total_vacuums + 1
            active_processes = active_processes + 1

    else:
        if dryrun:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            total_vacuums  = total_vacuums + 1
        else:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" %  (action_name, cnt, table, tups, sizep, size, dead))
            sql = "VACUUM VERBOSE %s" % table
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except psycopg2.Warning, w:
                printit("Warning: %s %s %s" % (w.pgcode, w.diag.severity, w.diag.message_primary))
                continue
            except psycopg2.Error, e:
                printit("Error  : %s %s %s" % (e.pgcode, e.diag.severity, e.diag.message_primary))
                continue
            total_vacuums  = total_vacuums + 1

#################################
# 4. Analyze on Small Tables    #
#################################
'''
-- all
select n.nspname || '.' || c.relname as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname))::bigint),  pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and
u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 5 and
pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= 50000000 order by 1,2;

-- public schema
select n.nspname || '.' || c.relname as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname))::bigint),  pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = 'public' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 5 and
pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= 50000000 order by 1,2;
'''

if schema == "":
   sql = "select n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' ||  " \
      "quote_ident(c.relname))::bigint), pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, " \
      "now()::date  - last_analyze::date as lastanalyzed2 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and " \
      "c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d " \
      "and pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= %d order by 1,2" % (threshold_max_days, threshold_min_size)
else:
   sql = "select n.nspname || '."' || c.relname || '"' as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || " \
      "quote_ident(c.relname))::bigint), pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, " \
      "now()::date  - last_analyze::date as lastanalyzed2 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname and " \
      "t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d " \
      "and pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= %d order by 1,2" % (schema, threshold_max_days, threshold_min_size)
try:
    cur.execute(sql)
#except psycopg2.Error, e:
except psycopg2.Error as e:
    printit ("Select Error: %s" % (e))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No small tables require analyzes to be done.")
else:
    printit ("Small table analyzes to be processed=%d" % len(rows) )

cnt = 0
action_name = 'ANALYZE'
for row in rows:
    cnt = cnt + 1
    table= row[0]
    tups = row[1]
    dead = row[3]
    sizep= row[4]
    size = row[5]

    # check if we already processed this table
    if skip_table(table, tablist):
        continue

    if dryrun:
        printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
        total_analyzes  = total_analyzes + 1
        tablist.append(table)
    else:
        printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
        sql = "ANALYZE VERBOSE %s" % table
        time.sleep(0.5)
        total_analyzes  = total_analyzes + 1
        tablist.append(table)
        try:
            cur.execute(sql)
        except psycopg2.Warning, w:
            printit("Warning: %s %s %s" % (w.pgcode, w.diag.severity, w.diag.message_primary))
        except psycopg2.Error, e:
            printit("Error  : %s %s %s" % (e.pgcode, e.diag.severity, e.diag.message_primary))


#################################
# 5. Analyze on Big Tables      #
#################################
'''
query gets rows > 50MB
-- all
select n.nspname || '.' || c.relname as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname))::bigint),  pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, u.last_analyze, u.last_autoanalyze,
case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2
 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and ((last_analyze is null and last_autoanalyze is null) or (now()::date  - last_analyze::date > 5 AND now()::date - last_autoanalyze::date > 5)) and
 pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) > 50000000 order by 1,2;

-- public schema
select n.nspname || '.' || c.relname as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname))::bigint),  pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, u.last_analyze, u.last_autoanalyze,
case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2
 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = 'public' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and ((last_analyze is null and last_autoanalyze is null) or (now()::date  - last_analyze::date > 5 AND now()::date - last_autoanalyze::date > 5)) and
 pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) > 50000000 order by 1,2;

'''
if schema == "":
   sql = "select n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || " \
      "quote_ident(c.relname))::bigint), pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, u.last_analyze, u.last_autoanalyze, " \
      "case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2 " \
      "from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and  " \
      "u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and ((last_analyze is null and last_autoanalyze is null) or (now()::date  - last_analyze::date > %d AND  " \
      "now()::date - last_autoanalyze::date > %d)) and pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) > %d order by 1,2;" % (threshold_max_days,threshold_max_days, threshold_min_size)
else:
   sql = "select n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || " \
      "quote_ident(c.relname))::bigint), pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, u.last_analyze, u.last_autoanalyze, " \
      "case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2 " \
      "from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and  " \
      "u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and ((last_analyze is null and last_autoanalyze is null) or (now()::date  - last_analyze::date > %d AND  " \
      "now()::date - last_autoanalyze::date > %d)) and pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) > %d order by 1,2;" % (schema, threshold_max_days,threshold_max_days, threshold_min_size)
try:
    cur.execute(sql)
#except psycopg2.Error, e:
except psycopg2.Error as e:
    printit ("Select Error: %s" % (e))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No stale tables require analyzes to be done.")
    printit ("Stale table statistics require analyzes to be processed=%d" % len(rows) )
else:
    printit ("Big table analyzes to be processed=%d" % len(rows) )

cnt = 0
action_name = 'ANALYZE'
for row in rows:
    cnt = cnt + 1
    table= row[0]
    tups = row[1]
    dead = row[3]
    sizep= row[4]
    size = row[5]

    # check if we already processed this table
    if skip_table(table, tablist):
        continue

    # skip tables that are too large
    if active_processes > threshold_max_processes:
        # see how many are currently running and update the active processes again
        # rc = get_process_cnt()
        rc = get_query_cnt(conn, cur)
        if rc > threshold_max_processes:
            printit ("Current process cnt(%d) is still higher than threshold (%d). Sleeping for 5 minutes..." % (rc, threshold_max_processes))
            time.sleep(300)
        else:
            printit ("Current process cnt(%d) is less than threshold (%d).  Processing will continue..." % (rc, threshold_max_processes))
        active_processes = rc

    if size > threshold_max_size:
        if dryrun:
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, sizep, size, dead))
            tables_skipped = tables_skipped + 1
        else:
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, sizep, size, dead))
            tables_skipped = tables_skipped + 1
        continue
    elif size > threshold_max_sync:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %-52s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            active_processes = active_processes + 1
            total_analyzes  = total_analyzes + 1
            tablist.append(table)
        else:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %-52s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            tablist.append(table)
            cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "ANALYZE VERBOSE %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, table)
            time.sleep(0.5)
            rc = execute_cmd(cmd)
            active_processes = active_processes + 1
            total_analyzes  = total_analyzes + 1
    else:
        if dryrun:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            total_analyzes  = total_analyzes + 1
            tablist.append(table)
        else:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            tablist.append(table)
            sql = "ANALYZE VERBOSE %s" % table
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except psycopg2.Warning, w:
                printit("Warning: %s %s %s" % (w.pgcode, w.diag.severity, w.diag.message_primary))
                continue
            except psycopg2.Error, e:
                printit("Error  : %s %s %s" % (e.pgcode, e.diag.severity, e.diag.message_primary))
                continue
            total_analyzes  = total_analyzes + 1

#################################
# 6. Catchall query for analyze that have not happened for over 2 weeks.
#################################
# V2.3: Introduced
'''
-- all
SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 14  order by 4,1;

-- public schema
SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = 'public' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 14  order by 4,1;

'''
if schema == "":
   sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup,  " \
      "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,  " \
      "to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname  " \
      "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND  " \
      "now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 14  order by 4,1"
else:
   sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup,  " \
      "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,  " \
      "to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname  " \
      "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND  " \
      "now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 14  order by 4,1" % (schema)
try:
     cur.execute(sql)
except psycopg2.Error as e:
    printit ("Select Error: %s" % (e))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No very old analyzes to be done.")
else:
    printit ("very old analyzes to be processed=%d" % len(rows) )

cnt = 0
action_name = 'ANALYZE(2)'
for row in rows:
    if active_processes > threshold_max_processes:
        # see how many are currently running and update the active processes again
        # rc = get_process_cnt()
        rc = get_query_cnt(conn, cur)
        if rc > threshold_max_processes:
            printit ("Current process cnt(%d) is still higher than threshold (%d). Sleeping for 5 minutes..." % (rc, threshold_max_processes))
            time.sleep(300)
        else:
            printit ("Current process cnt(%d) is less than threshold (%d).  Processing will continue..." % (rc, threshold_max_processes))
        active_processes = rc

    cnt = cnt + 1
    table  = row[0]
    sizep  = row[1]
    size   = row[2]
    tups   = row[3]
    live   = row[4]
    dead   = row[5]

    # check if we already processed this table
    if skip_table(table, tablist):
        continue

    if size > threshold_max_size:
        # defer action
        if dryrun:
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, sizep, size, dead))
            tables_skipped = tables_skipped + 1
        continue
    elif tups > threshold_async_rows or size > threshold_max_sync:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            total_analyzes = total_analyzes + 1
            tablist.append(table)
            active_processes = active_processes + 1
        else:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "ANALYZE VERBOSE %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, table)
            time.sleep(0.5)
            rc = execute_cmd(cmd)
            total_analyzes = total_analyzes + 1
            tablist.append(table)
            active_processes = active_processes + 1

    else:
        if dryrun:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            tablist.append(table)
        else:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            sql = "ANALYZE VERBOSE %s" % table
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except psycopg2.Warning, w:
                printit("Warning: %s %s %s" % (w.pgcode, w.diag.severity, w.diag.message_primary))
                continue
            except psycopg2.Error, e:
                printit("Error  : %s %s %s" % (e.pgcode, e.diag.severity, e.diag.message_primary))
                continue

            total_analyzes = total_analyzes + 1
            tablist.append(table)

#################################
# 7. Catchall query for vacuums that have not happened for over 2 weeks.
#################################
# V2.3: Introduced
'''
-- all
SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 14  order by 4,1;

-- public schema
SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = 'public' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 14  order by 4,1;
'''
if schema == "":
   sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup,  " \
      "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,  " \
      "to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname  " \
      "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND  " \
      "now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 14  order by 4,1"
else:
   sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup,  " \
      "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,  " \
      "to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname  " \
      "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND  " \
      "now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 14  order by 4,1" % (schema)
try:
     cur.execute(sql)
except psycopg2.Error as e:
    printit ("Select Error: %s" % (e))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No very old vacuums to be done.")
else:
    printit ("very old vacuums to be processed=%d" % len(rows) )

cnt = 0
action_name = 'VACUUM(2)'
for row in rows:
    if active_processes > threshold_max_processes:
        # see how many are currently running and update the active processes again
        # rc = get_process_cnt()
        rc = get_query_cnt(conn, cur)
        if rc > threshold_max_processes:
            printit ("Current process cnt(%d) is still higher than threshold (%d). Sleeping for 5 minutes..." % (rc, threshold_max_processes))
            time.sleep(300)
        else:
            printit ("Current process cnt(%d) is less than threshold (%d).  Processing will continue..." % (rc, threshold_max_processes))
        active_processes = rc

    cnt = cnt + 1
    table  = row[0]
    sizep  = row[1]
    size   = row[2]
    tups   = row[3]
    live   = row[4]
    dead   = row[5]

    # check if we already processed this table
    if skip_table(table, tablist):
        continue

    if size > threshold_max_size:
        # defer action
        if dryrun:
            printit ("Async %15s: %03d %-52s rows: %11d  dead: %8d  size: %10s :%13d NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, dead, sizep, size))
            tables_skipped = tables_skipped + 1
        continue
    elif tups > threshold_async_rows or size > threshold_max_sync:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            total_vacuums = total_vacuums + 1
            tablist.append(table)
            active_processes = active_processes + 1
        else:
            if active_processes > threshold_max_processes:
                printit ("%15s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (VERBOSE) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, table)
            time.sleep(0.5)
            rc = execute_cmd(cmd)
            total_vacuums = total_vacuums + 1
            tablist.append(table)
            active_processes = active_processes + 1

    else:
        if dryrun:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            tablist.append(table)
        else:
            printit ("Sync  %15s: %03d %-52s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            sql = "VACUUM VERBOSE %s" % table
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except psycopg2.Warning, w:
                printit("Warning: %s %s %s" % (w.pgcode, w.diag.severity, w.diag.message_primary))
                continue
            except psycopg2.Error, e:
                printit("Error  : %s %s %s" % (e.pgcode, e.diag.severity, e.diag.message_primary))
                continue

            total_vacuums = total_vacuums + 1
            tablist.append(table)


# wait for up to 2 hours for ongoing vacuums/analyzes to finish.
if not dryrun:
    wait_for_processes(conn,cur)

printit ("Total Vacuum Freeze: %d  Total Vacuum Analyze: %d  Total Vacuum: %d  Total Analyze: %d  Total Skipped Tables: %d" % (total_freezes, total_vacuums_analyzes, total_vacuums, total_analyzes, tables_skipped))
rc = get_query_cnt(conn, cur)
if rc > 0:
    printit ("NOTE: Current vacuums/analyzes still in progress: %d" % (rc))

# v 2.7 feature: if inquiry, then show results of 2 queries
if inquiry:
   if schema == "":
      sql = "SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
         "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, age(c.relfrozenxid) as xid_age,c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, " \
         "u.n_dead_tup::bigint AS dead_tup, coalesce(to_char(u.last_vacuum, 'YYYY-MM-DD'),'') as last_vacuum, coalesce(to_char(u.last_autovacuum, 'YYYY-MM-DD'),'') as last_autovacuum, " \
         "coalesce(to_char(u.last_analyze,'YYYY-MM-DD'),'') as last_analyze, coalesce(to_char(u.last_autoanalyze,'YYYY-MM-DD'),'') as last_autoanalyze " \
         "FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname " \
         "and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') order by 1"
   else:
      sql = "SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
         "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, age(c.relfrozenxid) as xid_age,c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, " \
         "u.n_dead_tup::bigint AS dead_tup, coalesce(to_char(u.last_vacuum, 'YYYY-MM-DD'),'') as last_vacuum, coalesce(to_char(u.last_autovacuum, 'YYYY-MM-DD'),'') as last_autovacuum, " \
         "coalesce(to_char(u.last_analyze,'YYYY-MM-DD'),'') as last_analyze, coalesce(to_char(u.last_autoanalyze,'YYYY-MM-DD'),'') as last_autoanalyze " \
         "FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname " \
         "and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') order by 1" % schema
   try:
        cur.execute(sql)
   except psycopg2.Error as e:
       printit ("Select Error: %s" % (e))
       conn.close()
       sys.exit (1)

   rows = cur.fetchall()
   if len(rows) == 0:
       printit ("Not able to retrieve inquiry results.")
   else:
       printit ("Inquiry Results Follow...")

   # v2.8 fix: indented following section else if was not part of the inquiry if section for cases that did not specify inquiry action.
   cnt = 0
   for row in rows:
      cnt = cnt + 1
      table            = row[0]
      sizep            = row[1]
      size             = row[2]
      xid_age          = row[3]
      n_tup            = row[4]
      n_live_tup       = row[5]
      dead_tup         = row[6]
      last_vacuum      = str(row[7])
      last_autovacuum  = str(row[8])
      last_analyze     = str(row[9])
      last_autoanalyze = str(row[10])
    
      if cnt == 1:
         printit("%50s %14s %14s %14s %12s %10s %10s %11s %12s %12s %16s" % ('table', 'sizep', 'size', 'xid_age', 'n_tup', 'n_live_tup', 'dead_tup', 'last_vacuum', 'last_autovac', 'last_analyze', 'last_autoanalyze'))
         printit("%50s %14s %14s %14s %12s %10s %10s %11s %12s %12s %16s" % ('-----', '-----', '----', '-------', '-----', '----------', '--------', '-----------', '------------', '------------', '----------------'))
      printit("%50s %14s %14d %14d %12d %10d %10d %11s %12s %12s %16s" % (table, sizep, size, xid_age, n_tup, n_live_tup, dead_tup, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze))

# end of inquiry section

# Close communication with the database
conn.close()
printit ("Closed the connection and exiting normally.")
sys.exit(0)
