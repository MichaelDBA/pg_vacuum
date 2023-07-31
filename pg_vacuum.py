#!/usr/bin/env python2
#!/usr/bin/env python3
#!/usr/bin/env python2
############### NOTE: weird errors occur when weird characters are saved. Go to Notepad++ and select macro-->Trim trailing space and save
##################################################################################################
# pg_vacuum.py
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
# Nov.  12, 2020.  V2.9: Fixed schema sql bug in small table analyze section.  Did not escape double-quotes.
#                        Adjusted format to allow for longer table names.
#                        Added parameter, --ignoreparts and sql logic supporting it.
#                        Ignore pg_catalog and information_schema tables not in ('pg_catalog', 'pg_toast', 'information_schema')
# Nov.  17, 2020.  V2.9: Deal with missing relispartition column from queries since it does not exist prior to version 10
# Dec.  04, 2020.  V2.9: Undo logic for #6 Catchall query for analyze that have not happened for over 30 days, not 14. Also, fixed dup tables again. and for very old vacuums
# Dec.  10, 2020.  V3.0: Rewrite for compatibiliity with python 2 and python3 and all versions of psycopg2.
#                        Changed "<>" to <!=>
#                        Changed print "whatever" to print ("whatever")
#                        Removed Psycopg2 exception handling and replaced with general one.
# Jan.  03, 2021   V3.1: Show aysync jobs summary. Added new parm, async, to trigger async jobs even if thresholds are not met (coded, but not implemented yet).
#                        Added application name to async jobs.  Lowered async threshold for max rows (threshold_async_rows) and max sync size (threshold_max_sync)
# Jan.  12, 2021   V3.2: Fix nohup missing double quotes!
# Jan.  26, 2021   V3.3: Add another parameter to do vacuums longer than x days, just like we do now for analyzes.
#                        Also prevent multiple instances of this program to run against the same PG database.
# Feb.  23, 2021   V3.4: Fix message for very old vacuums. and add min dead tups to > 0 --> vacuum(2) only at this time
# May   01, 2021   V4.0: Compatible with Windows.
#                        Fixed case where min dead tups was not being honored from the user.
#                        Compare with >= dead tups not > dead tups.
#                        Added date logic to vacuum determination logic
# May   03 2021    V4.0  Added additional filter for vacuum(2) to consider dead tups.
# May   08 2021    V4.1  Fixed case for vacuum/analyze branch where analyze value given for both analyze and vacuum max days.
#                        Also had to replace > dead tups to >= dead tups for vacuum/analyze branch.
#                        Also removed min size threshold for all cases
# June  02 2021    V4.1  Fixed signal handler, it was not exiting correctly.
#                        Fixed async call by escaping double quotes
# June  03 2021    V4.1  Added new input, maxtables to restrict how much work we do.
# June  11 2021    V4.1  Fixed VAC/ANALYZ branch where ORing condition caused actions even when dead tuple threshold specified.
# June  22 2021    V4.2  Enhancement: nullsonly option where only vacuum/analyzes done on tables with no vacuum/analyze history.
# June  23 2021    V4.2  Enhancement: add check option to get counts of objects that need vacuuming/analyzing
# July  01 2021    V4.3  Fix: change/add branch logic for size of relation was backwards.  > input instead of < input for maxsize and some did not use max relation size at all
# July  06 2022    V4.4  Fix: large table analysis was wrong, used less than instead of greater than
# July  12 2022    V4.5  Fix: checkstats did not consider autovacuum_count, only vacuum_count.  Also changed default dead tups min from 10,000 to 1,000.
# July  25 2022    V4.6  Enhancement: Make it work for MACs (darwin)
# Sept. 02 2022    V4.7  Allow users to not provide hostname, instead of defaulting to localhost. Also, fix vacuums where dead tups provided found tables, but not greater than max days so ignored!
# Nov.  28 2022    V4.8  Enable verbose parm for debugging purposes.  Also fix schema constraint that does not work in all cases.
#                        Also enable vacuum in parallel for PG V13+ up to max maintenenance parallel workers. Also fixed user-interrupted termination error.
# Dec.  20 2022    V5.0  Implemented new "autotune" feature that specifies the scale factor to use for vacuums and analyzes.
#                        Added new parm, minmodanalyzed, to work with analyzes like mindeadtups works with vacuums.
#                        Also changed algorithm: if vacuum days provided and deadtups provided, then both have to qualify to proceed.
#                        Don't consider null vacuums/analyzes anymore.  User must provide --nullsonly to do them separately.
#                        If only vacuum days provided, and no deadtups provided, proceed only if days qualifies
#                        If only deadtups provided, then proceed only if deadtups qualifies, ignore null vacuums/analyzes
#                        If neither days nor deadtups provided, then vacuum everything
# Dec.  21 2022    V5.1  Change maxsize parameter expected value unit from bytes to Gigabytes GB.
# June  28 2023    V5.2  When checking for number of vacuum/analyze processes running, do case insensitive grep search
#                        Also, add vacuums already running to the tablist
# July  09,2023    V5.3  Change default max sync from 100GB to 1TB, and max rows from 100 million to 10 billion before defaulting to async mode.
#                        Fixed bug with setting optional parameters to vacuum like page skipping and parallel workers
#                        Added another flag to indicate disable page skipping, default is to skip pages where possible based on the VM to make vacuums run faster
#                        Added another flag to order the results by last vacuumed date ascending.  Otherwise it defaults to tablename.
#                        Set max size to 1TB. Anything greater is rejected.
# July 22, 2023    V5.4  Changed logic for freezing tables. Now it is based on an xid_age minimum value instead of a percentage of max freeze threshold
#
# Notes:
#   1. Do not run this program multiple times since it may try to vacuum or analyze the same table again
#      since the logic is based on timestamps that are not updated until AFTER the action is done.
#   2. Change top shebang line to account for python, python or python2
#   3. By default, system schemas are not included when schemaname is ommitted from command line.
#   4. Setting dead_tuples to zero will force pg_vacuum to vacuum/analyze all tables that have no previous vacuum/analyzes.
#
# pgbench setup for testing:
#     turn off autovacuum for testing
#     pgbench -p 5414 -d -i -s 100 --init-steps=tgvpf pgbench
#     pgbench -c 10 -j 2 -n -M simple -T 60 -U postgres -p 5414 pgbench
#
#
# call example with all parameters:
# pg_vacuum.py -H localhost -d testing -p 5432 -u postgres --maxsize 400000000000 --maxdays 1 --mindeadtups 1000 --schema public --inquiry --dryrun
# pg_vacuum.py -H localhost -d testing -p 5432 -u postgres -s 400000000000 -y 1 -t 1000 -m public --freeze
#
# crontab example that runs every morning at 3am local time and will vacuum if more than 5000 dead tuples and/or its been over 5 days since the last vacuum/analyze.
# SHELL=/bin/sh
# PATH=<your binary paths as postgres user>
# 00 03 * * * /home/postgres/mjv/pg_vacuumb.py -H localhost -d <dbname> -u postgres -p 5432 -y 5 -t 5000 --dryrun >/home/postgres/mjv/optimize_db_`/bin/date +'\%Y-\%m-\%d-\%H.\%M.\%S'`.log 2>&1
#
##################################################################################################
import sys, os, threading, argparse, time, datetime, signal
from optparse import OptionParser
import psycopg2
import subprocess

version = '5.4  July 22, 2023'
pgversion = 0
OK = 0
BAD = -1

fmtrows  = '%11d'
fmtbytes = '%13d'

#minimum threshold for freezing tables: 50 million
threshold_freeze = 50000000

# minimum dead tuples
threshold_dead_tups = 1000

# 10 billion row threshold
threshold_async_rows = 10000000000

# v 4.1 fix: no minimum, go by max only!
# 50 MB minimum threshold
#threshold_min_size = 50000000
threshold_min_size = -1

# 1 TB threshold, above this table actions are done asynchronously
threshold_max_sync = 1073741824000

# max async processes
threshold_max_processes = 12

# load threshold, wait for a time if very high
load_threshold = 250

# PG v13+ enable parallel vacuuming for indexes > 130000
bParallel = False
parallelworkers = 0

class Range(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __eq__(self, other):
        return self.start <= other <= self.end

    def __contains__(self, item):
        return self.__eq__(item)

    def __iter__(self):
        yield self

    def __str__(self):
        return '[{0},{1}]'.format(self.start, self.end)


def signal_handler(signal, frame):
     printit('User-interrupted!')
     # sys.exit only creates an exception, it doesn't really exit!
     #sys.exit(1)
     if sys.platform == 'win32':
         sys._exit(1)
     else:
         #For Linux:
         # v4.8 fix
         #os.kill(os.getpid(), signal.SIGINT)
         # --> Exception: <type 'exceptions.AttributeError'> *** 'int' object has no attribute 'SIGINT'
         #os.kill(os.getpid(), signal.SIGTERM)
         # --> Exception: <type 'exceptions.AttributeError'> *** 'int' object has no attribute 'SIGTERM'
         #sys._exit(1)
         # --> AttributeError: 'module' object has no attribute '_exit'
         exit(1)

def check_maxtables():
    if len(tablist) > threshold_max_tables:
        printit ("Max Tables Reached (%d). Consider increasing max tables." % (len(tablist)))
        conn.close()
        sys.exit (1)

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
    # v5.2 fix: added case insensitive grep search ("-i")
    cmd = "ps -ef | grep -i 'VACUUM (VERBOSE\|ANALYZE VERBOSE' | grep -v grep | wc -l"
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    result = int(result.decode('ascii'))
    return result

def highload():
    # V4 fix:
    if sys.platform == 'win32':
        printit ("High Load detection not available for Windows at the present time.")
        return False
    elif sys.platform == 'darwin':
        # V4.6
        cmd = "uptime | awk '{print $(NF-2)}'"
    else:
        cmd = "uptime | sed 's/.*load average: //' | awk -F\, '{print $1}'"
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    min1  = str(result.decode())
    loadn = int(float(min1))

    if sys.platform == 'darwin':
        # V4.6
        cmd = "sysctl -a | grep machdep.cpu.thread_count | awk '{print $2}'"
    else:
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
    print ("load average=%s" % (load_average)
    # print ("total cpu=%d" % total_cpu)
    # print ("load now=%d" % load_now)

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
    sql = "select count(*) from pg_stat_activity where state = 'active' and application_name = 'pg_vacuum' and query like 'VACUUM FREEZE VERBOSE%' OR query like 'VACUUM (ANALYZE VERBOSE%' OR query like 'VACUUM (VERBOSE%' OR query like 'ANALYZE VERBOSE%'"
    cur.execute(sql)
    rows = cur.fetchone()
    return int(rows[0])

def get_vacuums_in_progress(conn, cur):
    sql = "SELECT 'tables', array_agg(relid::regclass) from pg_stat_progress_vacuum group by 1"
    cur.execute(sql)
    rows = cur.fetchone()
    return rows[1]

def skip_table (atable, tablist):
    #print ("table=%s  tablist=%s" % (atable, tablist))
    if atable in tablist:
        #print ("table is in the list")
        return True
    else:
        #print ("table is not in the list")
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

def _inquiry(conn,cur,tablist):
  # v 2.7 feature: if inquiry, then show results of 2 queries
  # print ("tables evaluated=%s" % tablist)
  # v5.0: fix query so materialized views show up as well (eliminated join to pg_tables)
  '''
  SELECT u.schemaname || '.\"' || u.relname || '\"' as table,
  pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size,
  age(c.relfrozenxid) as xid_age, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze::bigint as anal_tup,
  CASE WHEN u.n_live_tup = 0 AND u.n_dead_tup = 0 THEN 0.00 WHEN u.n_live_tup = 0 AND u.n_dead_tup > 0 THEN 100.00 ELSE round((u.n_dead_tup::numeric / u.n_live_tup::numeric),5) END pct_dead,
  CASE WHEN u.n_live_tup = 0 AND u.n_mod_since_analyze = 0 THEN 0.00 WHEN u.n_live_tup = 0 AND u.n_mod_since_analyze > 0 THEN 100.00 ELSE round((u.n_mod_since_analyze::numeric / u.n_live_tup::numeric),5) END pct_analyze,
  GREATEST(u.last_vacuum, u.last_autovacuum)::date as last_vacuumed, GREATEST(u.last_analyze, u.last_autoanalyze)::date as last_analyzed
  FROM pg_namespace n, pg_class c, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = u.schemaname and u.relname = c.relname and u.relid = c.oid and c.relkind in ('r','m','p')
  and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') order by 1;
  '''
  if schema == "":
    sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, " \
          "pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
          "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, " \
          "age(c.relfrozenxid) as xid_age," \
          "c.reltuples::bigint AS n_tup, " \
          "u.n_live_tup::bigint as n_live_tup, " \
          "u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze::bigint as anal_tup, " \
          "CASE WHEN u.n_live_tup = 0 AND u.n_dead_tup = 0 THEN 0.00 WHEN u.n_live_tup = 0 AND u.n_dead_tup > 0 THEN 100.00 ELSE round((u.n_dead_tup::numeric / u.n_live_tup::numeric),5) END pct_dead, " \
          "CASE WHEN u.n_live_tup = 0 AND u.n_mod_since_analyze = 0 THEN 0.00 WHEN u.n_live_tup = 0 AND u.n_mod_since_analyze > 0 THEN 100.00 ELSE round((u.n_mod_since_analyze::numeric / u.n_live_tup::numeric),5) END pct_analyze, " \
          "GREATEST(u.last_vacuum, u.last_autovacuum)::date as last_vacuumed, " \
          "GREATEST(u.last_analyze, u.last_autoanalyze)::date as last_analyzed " \
          "FROM pg_namespace n, pg_class c, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = u.schemaname and u.relname = c.relname and u.relid = c.oid and c.relkind in ('r','m','p') " \
          "and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') order by 1"
  else:
    sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table,  " \
           "pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
           "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size,  " \
           "age(c.relfrozenxid) as xid_age,  " \
           "c.reltuples::bigint AS n_tup,  " \
           "u.n_live_tup::bigint as n_live_tup, " \
           "u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze::bigint as anal_tup, " \
          "CASE WHEN u.n_live_tup = 0 AND u.n_dead_tup = 0 THEN 0.00 WHEN u.n_live_tup = 0 AND u.n_dead_tup > 0 THEN 100.00 ELSE round((u.n_dead_tup::numeric / u.n_live_tup::numeric),5) END pct_dead, " \
          "CASE WHEN u.n_live_tup = 0 AND u.n_mod_since_analyze = 0 THEN 0.00 WHEN u.n_live_tup = 0 AND u.n_mod_since_analyze > 0 THEN 100.00 ELSE round((u.n_mod_since_analyze::numeric / u.n_live_tup::numeric),5) END pct_analyze, " \
           "GREATEST(u.last_vacuum, u.last_autovacuum)::date as last_vacuumed, GREATEST(u.last_analyze, u.last_autoanalyze)::date as last_analyzed " \
           "FROM pg_namespace n, pg_class c, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = '%s' and u.schemaname = n.nspname and u.relname = c.relname and u.relid = c.oid and c.relkind in ('r','m','p') " \
           "order by 1" % schema

  try:
    cur.execute(sql)
  except Exception as error:
   printit("Exception: %s *** %s" % (type(error), error))
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
    anal_tup         = row[7]
    dead_pct         = row[8]
    anal_pct         = row[9]
    last_vacuumed    = str(row[10])
    last_analyzed    = str(row[11])

    if cnt == 1:
        printit("%55s %14s %14s %14s %12s %10s %10s %10s %10s %10s %12s %12s" % ('table', 'sizep', 'size', 'xid_age', 'n_tup', 'n_live_tup', 'dead_tup', 'anal_tup', 'dead_pct', 'anal_pct', 'last_vacuumed', 'last_analyzed'))
        printit("%55s %14s %14s %14s %12s %10s %10s %10s %10s %10s %12s %12s" % ('-----', '-----', '----', '-------', '-----', '----------', '--------', '--------', '--------', '--------', '-------------', '-------------'))

    #print ("table = %s  len=%d" % (table, len(table)))

    #pretty_size_span = 14
    #reduce = len(table) - 50
    #if reduce > 0 and reduce < 8:
    #    pretty_size_span = pretty_size_span - reduce

    if inquiry == 'all':
        printit("%55s %14s %14d %14d %12d %10d %10d %10d %10f %10f %12s %12s" % (table, sizep, size, xid_age, n_tup, n_live_tup, dead_tup, anal_tup, dead_pct, anal_pct, last_vacuumed, last_analyzed))
    else:
        if skip_table(table, tablist):
            printit("%55s %14s %14d %14d %12d %10d %10d %10d %10f %10f %12s %12s" % (table, sizep, size, xid_age, n_tup, n_live_tup, dead_tup, anal_tup, dead_pct, anal_pct, last_vacuumed, last_analyzed))

  # end of inquiry section

  return

def add_runningvacs_to_tablist(conn,cur,tablist):
  sql = "SELECT query FROM pg_stat_activity WHERE query ilike 'vacuum %' or query ilike 'analyze %' ORDER BY 1"
  try:
    cur.execute(sql)
  except Exception as error:
    printit("Exception: %s *** %s" % (type(error), error))
    conn.close()
    sys.exit (1)

  rows = cur.fetchall()
  if len(rows) == 0:
    printit ("No currently running vacuum or analyze jobs.")
    return

  cnt = 0
  for row in rows:
    # results look like this:  VACUUM (ANALYZE) public.scanner_call_count_2023_06;
    # so parse with spaces delimiter to find the table
    cnt = cnt + 1
    parts = row[0].split(' ')
    partlen = len(parts)
    #printit ("partlen=%d" % partlen)
    cnt2 = 0
    for apart in parts:
      cnt2 = cnt2 + 1
      # printit ("row=%s  apart=%s" % (row,apart))
      if cnt2 == partlen:
        # must be table name
        apart = apart.replace(";", "")
        tablist.append(apart)

# for atable in tablist:
#    print("excluded table: %s" % atable)
  return

def get_instance_cnt(conn,cur):
  sql = "select count(*) from pg_stat_activity where application_name = 'pg_vacuum'"
  try:
      cur.execute(sql)
  except Exception as error:
      printit ("Unable to check for multiple pg_vacuum instances: %s" % (e))
      return rc, BAD

  rows = cur.fetchone()
  instances = int(rows[0]) - 1
  return OK, instances


####################
# MAIN ENTRY POINT #
####################

# Register the signal handler for CNTRL-C logic
if sys.platform != 'win32':
    signal.signal(signal.SIGINT, signal_handler)
    signal.siginterrupt(signal.SIGINT, False)
else:
    # v4 fix:
    signal.signal(signal.SIGINT, signal_handler)
#while True:
#    print('Waiting...')
#    time.sleep(5)
#sys.exit(0)

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
partitioned_tables_skipped = 0
asyncjobs = 0
tablist = []

# Setup up the argument parser
parser = argparse.ArgumentParser("PostgreSQL Vacumming Tool",  add_help=True)
parser.add_argument("-H", "--host",   dest="hostname",              help="host name",         type=str, default="",metavar="HOSTNAME")
parser.add_argument("-d", "--dbname", dest="dbname",                help="database name",     type=str, default="",metavar="DBNAME")
parser.add_argument("-U", "--dbuser", dest="dbuser",                help="database user",     type=str, default="postgres",metavar="DBUSER")
parser.add_argument("-m", "--schema",dest="schema",                 help="schema",            type=str, default="",metavar="SCHEMA")
parser.add_argument("-p", "--dbport", dest="dbport",                help="database port",     type=int, default="5432",metavar="DBPORT")
parser.add_argument("-s", "--maxsize",dest="maxsize",               help="max table size in Gbytes",    type=int, default=-1,metavar="MAXSIZE")
parser.add_argument("-y", "--analyzemaxdays" ,dest="maxdaysA",      help="Analyze max days",  type=int, default=-1,metavar="ANALYZEMAXDAYS")
parser.add_argument("-x", "--vacuummaxdays"  ,dest="maxdaysV",      help="Vacuum  max days",  type=int, default=-1,metavar="VACUUMMAXDAYS")
parser.add_argument("-t", "--mindeadtups",dest="mindeadtups",       help="min dead tups",     type=int, default=-1,metavar="MINDEADTUPS")
parser.add_argument("-z", "--minmodanalyzed",dest="minmodanalyzed", help="min tups analyzed", type=int, default=-1,metavar="MINMODANALYZED")
parser.add_argument("-b", "--maxtables",dest="maxtables",           help="max tables",        type=int, default=9999,metavar="MAXTABLES")
parser.add_argument("-f", "--freeze", dest="freeze",                help="vacuum freeze xid%",   type=int, default=-1, metavar="FREEZE > xid_age")
parser.add_argument("-e", "--autotune", dest="autotune",            help="autotune",          type=float, choices=[Range(0.00001, 0.2)], metavar="AUTOTUNE 0.00001 to 0.2")
parser.add_argument("-q", "--inquiry", dest="inquiry",              help="inquiry requested", type=str, default="", choices=['all', 'found', ''],  metavar="INQUIRY all | found")

parser.add_argument("-r", "--dryrun", dest="dryrun",                help="dry run",                 default=False, action="store_true")
parser.add_argument("-i", "--ignoreparts", dest="ignoreparts",      help="ignore partition tables", default=False, action="store_true")
parser.add_argument("-a", "--async", dest="async_",                 help="run async jobs",          default=False, action="store_true")
parser.add_argument("-n", "--nullsonly", dest="nullsonly",          help="nulls only",              default=False, action="store_true")
parser.add_argument("-c", "--check", dest="check",                  help="check vacuum metrics",    default=False, action="store_true")
parser.add_argument("-v", "--verbose", dest="verbose",              help="verbose mode",            default=False, action="store_true")
parser.add_argument("-l", "--debug", dest="debug",                  help="debug mode",              default=False, action="store_true")
parser.add_argument("-k", "--disablepageskipping", dest="disablepageskipping",              help="disable page skipping",      default=False, action="store_true")
parser.add_argument("-g", "--orderbydate", dest="orderbydate",      help="order by date ascending", default=False, action="store_true")

args = parser.parse_args()

dryrun              = False
bfreeze             = False
bautotune           = False
ignoreparts         = False
async_              = False
orderbydate         = args.orderbydate
disablepageskipping = args.disablepageskipping
debug               = args.debug

if args.dryrun:
    dryrun = True
if args.ignoreparts:
    ignoreparts = True;
if args.async_:
    async_ = True;
if args.dbname == "":
    printit("DB Name must be provided.")
    sys.exit(1)

# 5 TB threshold, above this table actions are deferred
threshold_max_size = 5368709120000 

if args.maxsize != -1:
    # use user-provided max instead of program default (1 TB)
    temp = 1073741824 * args.maxsize
    if temp > threshold_max_size:
        printit("Max Size (1TB) Exceeded. You must vacuum tables larger than 5TB manually. Value Provided: %d." % temp)
        sys.exit(1)
    threshold_max_size = temp

dbname   = args.dbname
hostname = args.hostname
dbport   = args.dbport
dbuser   = args.dbuser
schema   = args.schema

# v3.1 change to include application name
connparms = "dbname=%s port=%d user=%s host=%s application_name=%s" % (dbname, dbport, dbuser, hostname, 'pg_vacuum' )

threshold_max_days_analyze = args.maxdaysA
threshold_max_days_vacuum  = args.maxdaysV
threshold_max_tables       = args.maxtables

min_dead_tups  = args.mindeadtups

# new in v5.0
minmodanalyzed = args.minmodanalyzed

freeze = args.freeze
if freeze != -1:
    bfreeze = True

if bfreeze and (freeze < 100000000 or freeze >  15000000000):
    printit("You must specify --freeze value range between 100,000,000 (100 million) and 1,500,000,000 (1.5 billion). You specified %d" % freeze)
    sys.exit(1)

# v4.1 fix: accept what the user says and don't override it
# if min_dead_tups > 100:
threshold_dead_tups = min_dead_tups

inquiry = args.inquiry
if inquiry == 'all' or inquiry == 'found' or inquiry == '':
    pass
else:
    printit("Inquiry parameter invalid.  Must be 'all' or 'found'")
    sys.exit(1)

nullsonly = args.nullsonly

# v5.0 feature
autotune  = args.autotune

_verbose = args.verbose
checkstats = args.check
if nullsonly and (checkstats or bfreeze or autotune):
    printit('Invalid Parameters:  You can only select "nullsonly" but not with either "check", "autotune", or "freeze".')
    sys.exit(1)
elif nullsonly and (threshold_max_days_analyze > -1 or threshold_max_days_vacuum > -1 or threshold_dead_tups > -1 or bfreeze):
    printit('Invalid Parameters:  You can only select "nullsonly" and not specify max vacuum/analyze days/dead tuples/freeze.')
    sys.exit(1)

if bfreeze and (checkstats or nullsonly or autotune):
    printit('Invalid Parameters:  You can only select "freeze" but not with either "check", "autotune", or "nullsonly".')
    sys.exit(1)
elif bfreeze and (threshold_max_days_analyze > -1 or threshold_max_days_vacuum > -1 or threshold_dead_tups > -1):
    printit('Invalid Parameters:  You can only select "freeze" and not specify max vacuum/analyze days/dead tuples.')
    sys.exit(1)

if autotune and (checkstats or nullsonly or bfreeze):
    printit('Invalid Parameters:  You can only select "autotune" but not with either "check", "nullsonly", or "freeze".')
    sys.exit(1)
elif autotune and (threshold_max_days_analyze > -1 or threshold_max_days_vacuum > -1 or threshold_dead_tups > -1 or bfreeze):
    printit('Invalid Parameters:  You can only select "autotune" and not specify max vacuum/analyze days/dead tuples/freeze.')
    sys.exit(1)

if autotune is None:
    autotune = -1
else:
    bautotune = True
    if autotune <= 0.000 or autotune > 0.500:
        printit('autotune range must be > 0.00001 and < 0.2.  Value provided = %f' % autotune)
        sys.exit(1)

printit ("version: %s" % version)
printit ("dryrun(%r) inquiry(%s) ignoreparts(%r) host:%s dbname=%s schema=%s dbuser=%s dbport=%d  Analyze max days:%d  Vacuumm max days:%d  min dead tups:%d  max table size(GB/bytes):%d  freeze:%d nullsonly=%r autotune=%f check=%r" \
        % (dryrun, inquiry, ignoreparts, hostname, dbname, schema, dbuser, dbport, threshold_max_days_analyze, threshold_max_days_vacuum, threshold_dead_tups, args.maxsize, freeze, nullsonly, autotune, checkstats))

# Connect
# conn = psycopg2.connect("dbname=testing user=postgres host=locahost password=postgrespass")
# connstr = "dbname=%s port=%d user=%s host=%s password=postgrespass" % (dbname, dbport, dbuser, hostname )
# v4.7 fix for ident cases, ie, no hostname provided, even localhost
if hostname == '':
    connstr = "dbname=%s port=%d user=%s application_name=%s connect_timeout=5" % (dbname, dbport, dbuser, 'pg_vacuum' )
else:
    connstr = "dbname=%s port=%d user=%s host=%s application_name=%s connect_timeout=5" % (dbname, dbport, dbuser, hostname, 'pg_vacuum' )
try:
    conn = psycopg2.connect(connstr)
except Exception as error:
    printit("Database Connection Error: %s *** %s" % (type(error), error))
    sys.exit (1)

printit("connected to database successfully. NOTE: Materialized Views are included with this utility.")

# to run vacuum through the psycopg2 driver, the isolation level must be changed.
old_isolation_level = conn.isolation_level
conn.set_isolation_level(0)

# Open a cursor to perform database operation
cur = conn.cursor()

# Abort if a pg_vacuum instance is already running against this database.
rc,instances = get_instance_cnt(conn,cur)
if rc == BAD:
    printit("Closing due to previous errors.")
    conn.close()
    sys.exit (1)
elif instances > 1:
    # allow for at least one other instance
    printit("Other pg_vacuum instances already running (%d).  Program will close." % (instances))
    conn.close()
    sys.exit (1)

# get version since 9.6 and earlier do not have a relispartition column in pg_class table
# it will look something like this or this: 90618 or 100013, so anything greater than 100000 would be 10+ versions.
# also it looks like we are not currently using the relispartition column anyway, so just remove it for now
# substitute this case statement for c.relispartition wherever it is used.
# CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False' ELSE 'True' END as partitioned

sql = "show server_version_num"
try:
    cur.execute(sql)
except Exception as error:
    printit ("Unable to get server version number: %s" % (e))
    conn.close()
    sys.exit (1)

rows = cur.fetchone()
pgversion = int(rows[0])


#v5.0 fix: DISABLE PAGE SKIPPING Followed by boolean is not valid pre PG v12
parallelstatement = ''
if pgversion > 120000:
    if disablepageskipping:
        parallelstatement = 'DISABLE_PAGE_SKIPPING FALSE'

if pgversion > 130000:
    bParallel = True
    sql = "show max_parallel_maintenance_workers"
    try:
        cur.execute(sql)
    except Exception as error:
        printit ("Unable to get max parallel maintenance workers: %s" % (e))
        conn.close()
        sys.exit (1)

    rows = cur.fetchone()
    parallelworkers = int(rows[0])

    if parallelstatement == '':
        parallelstatement = 'PARALLEL ' + str(parallelworkers)
    else:
        parallelstatement = parallel_statement + ', PARALLEL ' + str(parallelworkers)

if _verbose: printit("VERBOSE MODE: PG Version: %s  Parallel:%r  Max Parallel Maintenance Workers: %d  Parallel clause: %s" % (pgversion, bParallel, parallelworkers, parallelstatement))
active_processes = 0

# add running vacuum/analyzes to table bypass list
add_runningvacs_to_tablist(conn,cur,tablist)


#################################
# 1. Check Action Only          #
#################################
if checkstats:
  '''
  V.4.5 Fix: add autovacuum and autoanalyze counts
  select schemaname, count(*) as no_vacuums   from pg_stat_user_tables where schemaname not like 'pg_temp%' and vacuum_count = 0 and autovacuum_count = 0 group by 1 order by 1;
  select schemaname, count(*) as no_analyzes  from pg_stat_user_tables where schemaname not like 'pg_temp%' and analyze_count = 0 and autoanalyze_count = 0 group by 1 order by 1;
  select schemaname, count(*) as old_vacuums  from pg_stat_user_tables where schemaname not like 'pg_temp%' and greatest(last_vacuum, last_autovacuum) < now() - interval '10 day' group by 1 order by 1;
  select schemaname, count(*) as old_analyzes from pg_stat_user_tables where schemaname not like 'pg_temp%' and greatest(last_analyze, last_autoanalyze) < now() - interval '20 day' group by 1 order by 1;
  '''
  if _verbose: printit("VERBOSE MODE: Check Action Only branch")
  sql = "select aa.no_vacuums, bb.no_analyzes, cc.old_vacuums as old_vacuums_10days, dd.old_analyzes as old_analyzes_20days FROM " \
        "(select coalesce(sum(foo.no_vacuums), 0)   as no_vacuums   FROM (select schemaname, count(*) as no_vacuums   from pg_stat_user_tables where schemaname not like 'pg_temp%' and vacuum_count = 0 and autovacuum_count = 0 group by 1 order by 1) as foo) aa, " \
        "(select coalesce(sum(foo.no_analyzes), 0)  as no_analyzes  FROM (select schemaname, count(*) as no_analyzes  from pg_stat_user_tables where schemaname not like 'pg_temp%' and analyze_count = 0 and autoanalyze_count = 0 group by 1 order by 1) as foo) bb, " \
        "(select coalesce(sum(foo.old_vacuums), 0)  as old_vacuums  FROM (select schemaname, count(*) as old_vacuums  from pg_stat_user_tables where schemaname not like 'pg_temp%' and greatest(last_vacuum, last_autovacuum)   < now() - interval '10 day' group by 1 order by 1) as foo) cc, " \
        "(select coalesce(sum(foo.old_analyzes), 0) as old_analyzes FROM (select schemaname, count(*) as old_analyzes from pg_stat_user_tables where schemaname not like 'pg_temp%' and greatest(last_analyze, last_autoanalyze) < now() - interval '20 day' group by 1 order by 1) as foo) dd; "

  try:
      cur.execute(sql)
  except Exception as error:
      printit("Check Stats Exception: %s *** %s" % (type(error), error))
      conn.close()
      sys.exit (1)
  rows = cur.fetchone()
  no_vacuums   = rows[0]
  no_analyzes  = rows[1]
  old_vacuums  = rows[2]
  old_analyzes = rows[3]

  printit("CheckStats  :  no vacuums(%d)   no analyzes(%d)   old vacuums(%d)   old_analyzes(%d)" % (no_vacuums, no_analyzes, old_vacuums, old_analyzes))

  # get individual schema counts
  sql = "select schemaname, count(*) as no_vacuums   from pg_stat_user_tables where schemaname not like 'pg_temp%' and vacuum_count = 0 and autovacuum_count = 0 group by 1 order by 1"
  try:
      cur.execute(sql)
  except Exception as error:
      printit("Check Stats Details1 Exception: %s *** %s" % (type(error), error))
      conn.close()
      sys.exit (1)
  rows = cur.fetchall()
  if len(rows) == 0:
      printit("no vacuums: None Found")
  cnt = 0
  for row in rows:
      cnt = cnt + 1
      schema = row[0]
      cnt    = row[1]
      printit("No vacuums  : %20s  %4d" % (schema,cnt))

  sql = "select schemaname, count(*) as no_analyzes   from pg_stat_user_tables where schemaname not like 'pg_temp%' and analyze_count = 0 and autoanalyze_count = 0 group by 1 order by 1"
  try:
      cur.execute(sql)
  except Exception as error:
      printit("Check Stats Details2 Exception: %s *** %s" % (type(error), error))
      conn.close()
      sys.exit (1)
  rows = cur.fetchall()
  if len(rows) == 0:
      printit("No analyzes : %20s  %4d" % ('None Found',0))
  cnt = 0
  for row in rows:
      cnt = cnt + 1
      schema = row[0]
      cnt    = row[1]
      printit("No analyzes : %20s  %4d" % (schema,cnt))

  sql = "select schemaname, count(*) as old_vacuums  from pg_stat_user_tables where schemaname not like 'pg_temp%' and greatest(last_vacuum, last_autovacuum) < now() - interval '10 day' group by 1 order by 1"
  try:
      cur.execute(sql)
  except Exception as error:
      printit("Check Stats Details3 Exception: %s *** %s" % (type(error), error))
      conn.close()
      sys.exit (1)
  rows = cur.fetchall()
  if len(rows) == 0:
      printit("old vacuums: None Found")
  cnt = 0
  for row in rows:
      cnt = cnt + 1
      schema = row[0]
      cnt    = row[1]
      printit("old vacuums : %20s  %4d" % (schema,cnt))

  sql = "select schemaname, count(*) as old_analyzes  from pg_stat_user_tables where schemaname not like 'pg_temp%' and greatest(last_analyze, last_autoanalyze) < now() - interval '20 day' group by 1 order by 1"
  try:
      cur.execute(sql)
  except Exception as error:
      printit("Check Stats Details4 Exception: %s *** %s" % (type(error), error))
      conn.close()
      sys.exit (1)
  rows = cur.fetchall()
  cnt = 0
  if len(rows) == 0:
      printit("old analyzes: None Found")
  for row in rows:
      cnt = cnt + 1
      schema = row[0]
      cnt    = row[1]
      printit("old analyzes: %20s  %4d" % (schema,cnt))

  conn.close()
  printit ("End of check stats action.  Closing the connection and exiting normally.")
  sys.exit(0)


#################################
# 2. Nulls Only                 #
#################################
if nullsonly:
  '''
  SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,
  pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,
  u.n_dead_tup::bigint AS dead_tup, c.relispartition, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,
  to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze, u.vacuum_count, u.analyze_count
  FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and
  t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname AND (u.vacuum_count = 0 OR u.analyze_count = 0) order by 1;

  '''
  if _verbose: printit("VERBOSE MODE: Nulls Only branch")
  if pgversion > 100000:
      if schema == "":
        sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
        "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
        "u.n_dead_tup::bigint AS dead_tup, c.relispartition, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
        "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,  " \
        " u.vacuum_count, u.analyze_count " \
        "FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') " \
        "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname  " \
        "and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND (u.vacuum_count = 0 OR u.analyze_count = 0) order by 1,1"
      else:
        sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
        "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
        "u.n_dead_tup::bigint AS dead_tup, c.relispartition, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
        "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,  " \
        " u.vacuum_count, u.analyze_count " \
        "FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = '%s' and t.schemaname = n.nspname and t.tablename = c.relname and " \
        "c.relname = u.relname and u.schemaname = n.nspname AND (u.vacuum_count = 0 OR u.analyze_count = 0) order by 1,1" % (schema)
  else:
      if schema == "":
        sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
            "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
            "u.n_dead_tup::bigint AS dead_tup, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) " \
            "where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, " \
            "to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
            "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,  " \
            " u.vacuum_count, u.analyze_count " \
            "FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = n.nspname and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and " \
            "t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname  " \
            "and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND (u.vacuum_count = 0 OR u.analyze_count = 0) order by 1,1"
      else:
        sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
            "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
            "u.n_dead_tup::bigint AS dead_tup, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) " \
            "where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, " \
            "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
            "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze, " \
            " u.vacuum_count, u.analyze_count " \
            "FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = '%s' and t.schemaname = n.nspname and " \
            "t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname AND (u.vacuum_count = 0 OR u.analyze_count = 0) order by 1,1" % (schema)
  try:
      cur.execute(sql)
  except Exception as error:
      printit("Nulls Only Exception: %s *** %s" % (type(error), error))
      conn.close()
      sys.exit (1)

  rows = cur.fetchall()
  if len(rows) == 0:
      printit ("No Nulls Only need to be done.")
  else:
      printit ("Nulls Only to be evaluated=%d.  Includes deferred ones too." % len(rows) )

  cnt = 0
  partcnt = 0
  action_name = 'VAC/ANALYZ'

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
      table    = row[0]
      sizep    = row[1]
      size     = row[2]
      tups     = row[3]
      live     = row[4]
      dead     = row[5]
      part     = row[6]
      last_vac = row[7]
      last_avac= row[8]
      last_anl = row[9]
      last_aanl= row[10]
      vac_cnt  = int(row[11])
      anal_cnt = int(row[12])

      if vac_cnt == 0 and anal_cnt == 0:
          action_name = 'VAC/ANALYZ'
      elif vac_cnt == 0 and anal_cnt > 0:
          action_name = 'VACUUM'
      elif vac_cnt > 0 and anal_cnt == 0:
          action_name = 'ANALYZE'

      #printit("DEBUG action=%s  vac=%d  anal=%d  table=%s" % (action_name, vac_cnt, anal_cnt, table))

      if part and ignoreparts:
          partcnt = partcnt + 1
          #print ("ignoring partitioned table: %s" % table)
          cnt = cnt - 1
          continue

      # check if we already processed this table
      if skip_table(table, tablist):
          cnt = cnt - 1
          continue

      if size > threshold_max_size:
          if dryrun:
              # defer action
              printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, sizep, size, dead))
              tablist.append(table)
              check_maxtables()
              tables_skipped = tables_skipped + 1
          continue
      elif tups > threshold_async_rows or size > threshold_max_sync:
          if dryrun:
              if active_processes > threshold_max_processes:
                  printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                  tables_skipped = tables_skipped + 1
                  tablist.append(table)
                  check_maxtables()
                  cnt = cnt - 1
                  continue
              printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
              total_vacuums_analyzes = total_vacuums_analyzes + 1
              tablist.append(table)
              check_maxtables()
              active_processes = active_processes + 1
          else:
              if active_processes > threshold_max_processes:
                  printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                  tablist.append(table)
                  check_maxtables()
                  tables_skipped = tables_skipped + 1
                  cnt = cnt - 1
                  continue
              printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
              asyncjobs = asyncjobs + 1

              # cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (ANALYZE, VERBOSE, %s) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, parallelstatement, table)
              # V4.1 fix, escape double quotes
              tbl= table.replace('"', '\\"')
              tbl= tbl.replace('$', '\$')

              if vac_cnt == 0 and anal_cnt == 0:
                  # v5.0 fix: handle empty parallelstatement string
                  if parallelstatement == '':
                      cmd = 'nohup psql -d "%s" -c "VACUUM (ANALYZE) %s" 2>/dev/null &' % (connparms, tbl)
                  else:
                      cmd = 'nohup psql -d "%s" -c "VACUUM (ANALYZE, %s) %s" 2>/dev/null &' % (connparms, parallelstatement, tbl)
                  action_name = 'VAC/ANALYZ'
              elif vac_cnt == 0 and anal_cnt > 0:
                  # v5.0 fix: handle empty parallelstatement string
                  if parallelstatement == '':
                      cmd = 'nohup psql -d "%s" -c "VACUUM %s" 2>/dev/null &' % (connparms, tbl)
                  else:
                      cmd = 'nohup psql -d "%s" -c "VACUUM (%s) %s" 2>/dev/null &' % (connparms, parallelstatement, tbl)
              elif vac_cnt > 0 and anal_cnt == 0:
                  cmd = 'nohup psql -d "%s" -c "ANALYZE %s" 2>/dev/null &' % (connparms, tbl)

              time.sleep(0.5)
              rc = execute_cmd(cmd)
              print("rc=%d" % rc)
              total_vacuums_analyzes = total_vacuums_analyzes + 1
              tablist.append(table)
              check_maxtables()
              active_processes = active_processes + 1

      else:
          if dryrun:
              printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
              total_vacuums_analyzes = total_vacuums_analyzes + 1
              tablist.append(table)
              check_maxtables()
          else:
              printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))

              if vac_cnt == 0 and anal_cnt == 0:
                  # v5.0 fix: handle empty parallelstatement string
                  if parallelstatement == '':
                      sql = "VACUUM (ANALYZE) %s" % (table)
                  else:
                      sql = "VACUUM (ANALYZE, %s) %s" % (parallelstatement, table)
              elif vac_cnt == 0 and anal_cnt > 0:
                  # v5.0 fix: handle empty parallelstatement string
                  if parallelstatement == '':
                      sql = "VACUUM  %s" % (table)
                  else:
                      sql = "VACUUM (%s) %s" % (parallelstatement, table)
              elif vac_cnt > 0 and anal_cnt == 0:
                  sql = "ANALYZE %s" % table

              time.sleep(0.5)
              try:
                  cur.execute(sql)
              except Exception as error:
                  printit("Exception: %s *** %s" % (type(error), error))
                  cnt = cnt - 1
                  continue
              total_vacuums_analyzes = total_vacuums_analyzes + 1
              tablist.append(table)
              check_maxtables()

  if ignoreparts:
      printit ("Partitioned table vacuum/analyzes bypassed=%d" % partcnt)
      partitioned_tables_skipped = partitioned_tables_skipped + partcnt

  printit ("Tables vacuumed/analyzed: %d" % cnt)

  if inquiry:
    rc = _inquiry(conn,cur,tablist)

  conn.close()
  printit ("End of Nulls Only action.  Closing the connection and exiting normally.")
  sys.exit(0)


#################################
# 3. Freeze Tables              #
#################################
if bfreeze:
  if _verbose: printit("VERBOSE MODE: (3) Freeze section")
  # ignore tables less than the minimum threshold, 50 million
  if pgversion > 100000:
      if schema == "":
         sql = "SELECT n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint as rows, age(c.relfrozenxid)::bigint as xid_age, CAST(current_setting('autovacuum_freeze_max_age') AS bigint) as freeze_max_age, " \
         "CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint as howclose, pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty,  " \
         "pg_total_relation_size(c.oid) as table_size, c.relispartition, ROUND((age(c.relfrozenxid)::numeric / CAST(current_setting('autovacuum_freeze_max_age') AS numeric)) * 100,0) as pct " \
         "FROM pg_class c, pg_namespace n WHERE n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and n.oid = c.relnamespace and c.relkind not in ('i','v','S','c') AND " \
         "CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint > 1::bigint AND age(c.relfrozenxid)::bigint > %d ORDER BY age(c.relfrozenxid) DESC, table_size DESC" % (threshold_freeze)
      else:
         sql = "SELECT n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint as rows, age(c.relfrozenxid)::bigint as xid_age, CAST(current_setting('autovacuum_freeze_max_age') AS bigint) as freeze_max_age, " \
         "CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint as howclose, pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty,  " \
         "pg_total_relation_size(c.oid) as table_size, c.relispartition, ROUND((age(c.relfrozenxid)::numeric / CAST(current_setting('autovacuum_freeze_max_age') AS numeric)) * 100,0) as pct " \
         "FROM pg_class c, pg_namespace n WHERE n.nspname = '%s' and n.oid = c.relnamespace and c.relkind not in ('i','v','S','c') AND " \
         "CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint > 1::bigint AND age(c.relfrozenxid)::bigint > %d ORDER BY age(c.relfrozenxid) DESC, table_size DESC" % (schema, threshold_freeze)

  else:
  # put version 9.x compatible query here
  # CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False' ELSE 'True' END as partitioned
      if schema == "":
         sql = "SELECT n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint as rows, age(c.relfrozenxid) as xid_age, CAST(current_setting('autovacuum_freeze_max_age') AS bigint) as freeze_max_age, " \
        "CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint as howclose, pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty,  " \
        "pg_total_relation_size(c.oid) as table_size, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, " \
        "ROUND((age(c.relfrozenxid)::numeric / CAST(current_setting('autovacuum_freeze_max_age') AS numeric)) * 100,0) as pct " \
        "FROM pg_class c, pg_namespace n WHERE n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and n.oid = c.relnamespace and c.relkind not in ('i','v','S','c') AND CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - " \
        "age(c.relfrozenxid)::bigint > 1::bigint and  CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint > %d ORDER BY age(c.relfrozenxid) DESC" % (threshold_freeze)
      else:
         sql = "SELECT n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint as rows, age(c.relfrozenxid) as xid_age, CAST(current_setting('autovacuum_freeze_max_age') AS bigint) as freeze_max_age, " \
        "CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint as howclose, pg_size_pretty(pg_total_relation_size(c.oid)) as table_size_pretty,  " \
        "pg_total_relation_size(c.oid) as table_size, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, " \
        "ROUND((age(c.relfrozenxid)::numeric / CAST(current_setting('autovacuum_freeze_max_age') AS numeric)) * 100,0) as pct " \
        "FROM pg_class c, pg_namespace n WHERE n.nspname = '%s' and n.oid = c.relnamespace and c.relkind not in ('i','v','S','c') AND CAST(current_setting('autovacuum_freeze_max_age') " \
        "AS bigint) - age(c.relfrozenxid)::bigint > 1::bigint and  CAST(current_setting('autovacuum_freeze_max_age') AS bigint) - age(c.relfrozenxid)::bigint > %d ORDER BY age(c.relfrozenxid) DESC" % (schema, threshold_freeze)
  if debug: printit("DEBUG   QUERY: %s" % sql)

  try:
       cur.execute(sql)
  except Exception as error:
      printit("Freeze Tables Exception: %s *** %s" % (type(error), error))
      conn.close()
      sys.exit (1)

  rows = cur.fetchall()
  if len(rows) == 0:
      printit ("No FREEZEs need to be done.")
  else:
      printit ("VACUUM FREEZEs to be evaluated=%d.  Includes deferred ones too." % len(rows) )

  cnt = 0
  partcnt = 0
  action_name = 'VAC/FREEZE'
  if not dryrun and len(rows) > 0 and not bfreeze:
      printit ('Bypassing VACUUM FREEZE action for %d tables. Otherwise specify "--freeze" to do them.' % len(rows))

  for row in rows:
      if not bfreeze and not dryrun:
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
      size     = row[6]
      part     = row[7]
      #pct      = int(row[8])
      pct      = row[8]
      
      
      if total_freezes > threshold_max_tables:
          # exit, since we are finished and notify 
          printit ("exiting since we exceeded user-provided max tables=%d" % (threshold_max_tables))
          break

      # bypass tables that are less than 10% of max age
      #               table                          |   rows    |  xid_age  | freeze_max_age |  howclose  | table_size_pretty |  table_size   | relispartition | pct
      #----------------------------------------------+-----------+-----------+----------------+------------+-------------------+---------------+----------------+-----
      #public."amazonnewprod_adtags_update"          |  64689384 | 277193255 |     1500000000 | 1222806745 | 34 GB             |   36666990592 | f              |  18
      #public."amazonnewprod_adtags_active_metadata" | 233080384 | 276124136 |     1500000000 | 1223875864 | 97 GB             |  103821336576 | f              |  18

      if xidage < freeze:
        # bypassing table
        tables_skipped = tables_skipped + 1
        if _verbose: printit ("bypassing table with xidage=%d.  Threshold minimum=%d" % (xidage, freeze))
        continue

      if part and ignoreparts:
          partcnt = partcnt + 1
          #print ("ignoring partitioned table: %s" % table)
          continue

      # also bypass tables that are less than 15% of max age
      pctmax = float(xidage) / float(maxage)
      if _verbose: print("maxage=%10f  xidage=%10f  pctmax=%4f  freeze=%4f pct=%d" % (maxage, xidage, pctmax, freeze, pct))

      if size > threshold_max_size:
          # defer action
          printit ("Async %10s  %04d %-57s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d NOTICE: Skipping large table.  Do manually." \
                  % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose))
          tables_skipped = tables_skipped + 1
          cnt = cnt - 1
          continue
      elif tups > threshold_async_rows or size > threshold_max_sync:
          if dryrun:
              if active_processes > threshold_max_processes:
                  printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                  tables_skipped = tables_skipped + 1
                  cnt = cnt - 1
                  continue
              printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d  pct: %d" % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose, (100 * pctmax)))
              total_freezes = total_freezes + 1
              tablist.append(table)
              check_maxtables()
              if len(tablist) > threshold_max_tables:
                  printit ("Max Tables Reached: %d." % len(tablist))
              active_processes = active_processes + 1
          else:
              if active_processes > threshold_max_processes:
                  printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                  tables_skipped = tables_skipped + 1
                  cnt = cnt - 1
                  continue
              # cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (FREEZE, VERBOSE) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, table)
              # V4.1 fix, escape double quotes
              tbl= table.replace('"', '\\"')
              tbl= tbl.replace('$', '\$')
              cmd = 'nohup psql -d "%s" -c "VACUUM (FREEZE, VERBOSE) %s" 2>/dev/null &' % (connparms, tbl)
              time.sleep(0.5)
              asyncjobs = asyncjobs + 1
              printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d  pct: %d" % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose, (100 * pctmax)))
              rc = execute_cmd(cmd)
              total_freezes = total_freezes + 1
              tablist.append(table)
              check_maxtables()
              active_processes = active_processes + 1
      else:
          if async_:
              # force async regardless
              printit ("Async  %10s: %04d %-57s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d  pct: %d" % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose, (100 * pctmax)))
              time.sleep(0.5)
              asyncjobs = asyncjobs + 1
              cmd = 'nohup psql -d "%s" -c "VACUUM (FREEZE, VERBOSE) %s" 2>/dev/null &' % (connparms, table)
              if dryrun:
                  total_freezes = total_freezes + 1
              else:
                  rc = execute_cmd(cmd)
                  total_freezes = total_freezes + 1
                  tablist.append(table)
                  check_maxtables()
                  active_processes = active_processes + 1
          else:
              printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d freeze_max: %10d  xid_age: %10d  how close: %10d  pct: %d" % (action_name, cnt, table, tups, sizep, size, maxage, xidage, howclose, (100 * pctmax)))
              if dryrun:
                  total_freezes = total_freezes + 1
                  tablist.append(table)
                  check_maxtables()
              else:
                  sql = "VACUUM (FREEZE, VERBOSE) %s" % table
                  time.sleep(0.5)
                  try:
                      cur.execute(sql)
                  except Exception as error:
                      printit("Exception: %s *** %s" % (type(error), error))
                      cnt = cnt - 1
                      continue
                  total_freezes = total_freezes + 1
                  tablist.append(table)
                  check_maxtables()

  if ignoreparts:
      printit ("Partitioned table vacuum freezes bypassed=%d" % partcnt)
      partitioned_tables_skipped = partitioned_tables_skipped + partcnt

  printit ("Table freezes: %d  Async freezes: %d  Tables skipped: %d" % (total_freezes, asyncjobs, tables_skipped))

  if inquiry:
    rc = _inquiry(conn,cur,tablist)

  conn.close()
  printit ("End of Freeze action.  Closing the connection and exiting normally.")
  sys.exit(0)

#################################
# 4. Autotune Only              #
#################################
if bautotune:
  if _verbose: printit("VERBOSE MODE: (4) Autotune section")

  '''
  select t.schemaname || '.' || t.relname "Table", pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid)) "Sizep", pg_catalog.pg_relation_size(c.oid) size,t.n_live_tup live_tup, t.n_dead_tup dead_tup,
  round((n_live_tup * current_setting('autovacuum_vacuum_scale_factor')::float8 ) + current_setting('autovacuum_vacuum_threshold')::float8) dead_thresh,
  CASE WHEN t.n_live_tup = 0 AND t.n_dead_tup = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_dead_tup > 0 THEN 100.00 ELSE round((t.n_dead_tup::numeric / t.n_live_tup::numeric),5) END pct_dead,
  round((n_live_tup * current_setting('autovacuum_analyze_scale_factor')::float8 ) + current_setting('autovacuum_analyze_threshold')::float8) analyze_thresh,
  CASE WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze > 0 THEN 100.00 ELSE round((t.n_mod_since_analyze::numeric / t.n_live_tup::numeric),5) END pct_analyze,
  t.vacuum_count vac_cnt, t.autovacuum_count autovac_cnt, t.analyze_count ana_cnt, t.autoanalyze_count autoana_cnt, t.n_mod_since_analyze mod_since_ana, -1 n_ins_since_vacuum, c.relkind,
  GREATEST(t.last_vacuum, t.last_autovacuum)::date as last_vacuum,  GREATEST(t.last_analyze,t.last_autoanalyze)::date as last_analyze
  FROM pg_stat_user_tables t, pg_namespace n, pg_class c
  WHERE n.nspname =  t.schemaname AND n.oid = c.relnamespace AND t.relname = c.relname AND (t.n_dead_tup > 0 OR t.n_mod_since_analyze > 0) ORDER BY 1,2 LIMIT 20;
         Table            |  Sizep  |    size    | live_tup | dead_tup | dead_thresh | pct_dead | analyze_thresh | pct_analyze | vac_cnt | autovac_cnt | ana_cnt | autoana_cnt | mod_since_ana | ins_since_vac | last_vacuum | last_autovacuum
  --------------------- --+---------+------------+----------+----------+-------------+----------+----------------+-------------+---------+-------------+---------+-------------+---------------+---------------+-------------+-----------------
  public.pgbench_accounts | 1292 MB | 1354416128 | 10000035 |   111274 |      200051 |  0.01113 |         100050 |     0.01185 |       1 |           0 |       1 |           0 |        118462 |             0 | 2022-12-18  |
  public.pgbench_branches | 16 kB   |      16384 |      100 |      230 |          52 |  2.30000 |             51 |   299.36000 |       3 |           6 |       1 |           6 |         29936 |             0 | 2022-12-18  | 2022-12-18
  public.pgbench_history  | 4632 kB |    4743168 |    89979 |        0 |        1850 |  0.00000 |            950 |     0.33270 |       1 |           6 |       1 |           6 |         29936 |         29936 | 2022-12-18  | 2022-12-18
  public.pgbench_tellers  | 240 kB  |     245760 |     1000 |      715 |          70 |  0.71500 |             60 |    29.93600 |       3 |           6 |       1 |           6 |         29936 |             0 | 2022-12-18  | 2022-12-18
  '''
  if pgversion < 130000:
    # earlier versions do not have n_ins_since_vacuum column
    if schema == "":
      sql = "SELECT t.schemaname || '.\"' || t.relname || '\"' as atable, pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid)) Sizep, pg_catalog.pg_relation_size(c.oid) Size, t.n_live_tup live_tup, t.n_dead_tup dead_tup, " \
            "round((n_live_tup * current_setting('autovacuum_vacuum_scale_factor')::float8 ) + current_setting('autovacuum_vacuum_threshold')::float8) dead_thresh,  " \
            "CASE WHEN t.n_live_tup = 0 AND t.n_dead_tup = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_dead_tup > 0 THEN 100.00 ELSE round((t.n_dead_tup::numeric / t.n_live_tup::numeric),5) END pct_dead,  " \
            "round((n_live_tup * current_setting('autovacuum_analyze_scale_factor')::float8 ) + current_setting('autovacuum_analyze_threshold')::float8) analyze_thresh,  " \
            "CASE WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze > 0 THEN 100.00 ELSE round((t.n_mod_since_analyze::numeric / t.n_live_tup::numeric),5) END pct_analyze,  " \
            "t.vacuum_count vac_cnt, t.autovacuum_count autovac_cnt, t.analyze_count ana_cnt, t.autoanalyze_count autoana_cnt, t.n_mod_since_analyze mod_since_ana, -1 ins_since_vac, c.relkind, " \
            "GREATEST(t.last_vacuum, t.last_autovacuum)::date as last_vacuum,  GREATEST(t.last_analyze,t.last_autoanalyze)::date as last_analyze  " \
            "FROM pg_stat_user_tables t, pg_namespace n, pg_class c  " \
            "WHERE n.nspname =  t.schemaname AND n.oid = c.relnamespace AND t.relname = c.relname AND (t.n_dead_tup > 0 OR t.n_mod_since_analyze > 0) ORDER BY 1, 2 "
    else:
      sql = "SELECT t.schemaname || '.\"' || t.relname || '\"' as atable, pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid)) Sizep, pg_catalog.pg_relation_size(c.oid) Size, t.n_live_tup live_tup, t.n_dead_tup dead_tup, " \
            "round((n_live_tup * current_setting('autovacuum_vacuum_scale_factor')::float8 ) + current_setting('autovacuum_vacuum_threshold')::float8) dead_thresh,  " \
            "CASE WHEN t.n_live_tup = 0 AND t.n_dead_tup = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_dead_tup > 0 THEN 100.00 ELSE round((t.n_dead_tup::numeric / t.n_live_tup::numeric),5) END pct_dead,  " \
            "round((n_live_tup * current_setting('autovacuum_analyze_scale_factor')::float8 ) + current_setting('autovacuum_analyze_threshold')::float8) analyze_thresh,  " \
            "CASE WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze > 0 THEN 100.00 ELSE round((t.n_mod_since_analyze::numeric / t.n_live_tup::numeric),5) END pct_analyze,  " \
            "t.vacuum_count vac_cnt, t.autovacuum_count autovac_cnt, t.analyze_count ana_cnt, t.autoanalyze_count autoana_cnt, t.n_mod_since_analyze mod_since_ana, -1 ins_since_vac, c.relkind, " \
            "GREATEST(t.last_vacuum, t.last_autovacuum)::date as last_vacuum,  GREATEST(t.last_analyze,t.last_autoanalyze)::date as last_analyze  " \
            "FROM pg_stat_user_tables t, pg_namespace n, pg_class c  " \
            "WHERE n.nspname = '%s' AND n.nspname = t.schemaname AND n.oid = c.relnamespace AND t.relname = c.relname AND (t.n_dead_tup > 0 OR t.n_mod_since_analyze > 0) ORDER BY 1, 2 "  % (schema)
  else:
    if schema == "":
      sql = "SELECT t.schemaname || '.\"' || t.relname || '\"' as atable, pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid)) Sizep, pg_catalog.pg_relation_size(c.oid) Size, t.n_live_tup live_tup, t.n_dead_tup dead_tup, " \
            "round((n_live_tup * current_setting('autovacuum_vacuum_scale_factor')::float8 ) + current_setting('autovacuum_vacuum_threshold')::float8) dead_thresh,  " \
            "CASE WHEN t.n_live_tup = 0 AND t.n_dead_tup = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_dead_tup > 0 THEN 100.00 ELSE round((t.n_dead_tup::numeric / t.n_live_tup::numeric),5) END pct_dead,  " \
            "round((n_live_tup * current_setting('autovacuum_analyze_scale_factor')::float8 ) + current_setting('autovacuum_analyze_threshold')::float8) analyze_thresh,  " \
            "CASE WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze > 0 THEN 100.00 ELSE round((t.n_mod_since_analyze::numeric / t.n_live_tup::numeric),5) END pct_analyze,  " \
            "t.vacuum_count vac_cnt, t.autovacuum_count autovac_cnt, t.analyze_count ana_cnt, t.autoanalyze_count autoana_cnt, t.n_mod_since_analyze mod_since_ana, t.n_ins_since_vacuum  ins_since_vac, c.relkind, " \
            "GREATEST(t.last_vacuum, t.last_autovacuum)::date as last_vacuum,  GREATEST(t.last_analyze,t.last_autoanalyze)::date as last_analyze  " \
            "FROM pg_stat_user_tables t, pg_namespace n, pg_class c  " \
            "WHERE n.nspname =  t.schemaname AND n.oid = c.relnamespace AND t.relname = c.relname AND (t.n_dead_tup > 0 OR t.n_mod_since_analyze > 0) ORDER BY 1, 2 "
    else:
      sql = "SELECT t.schemaname || '.\"' || t.relname || '\"' as atable, pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid)) Sizep, pg_catalog.pg_relation_size(c.oid) Size, t.n_live_tup live_tup, t.n_dead_tup dead_tup, " \
            "round((n_live_tup * current_setting('autovacuum_vacuum_scale_factor')::float8 ) + current_setting('autovacuum_vacuum_threshold')::float8) dead_thresh,  " \
            "CASE WHEN t.n_live_tup = 0 AND t.n_dead_tup = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_dead_tup > 0 THEN 100.00 ELSE round((t.n_dead_tup::numeric / t.n_live_tup::numeric),5) END pct_dead,  " \
            "round((n_live_tup * current_setting('autovacuum_analyze_scale_factor')::float8 ) + current_setting('autovacuum_analyze_threshold')::float8) analyze_thresh,  " \
            "CASE WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze = 0 THEN 0.00 WHEN t.n_live_tup = 0 AND t.n_mod_since_analyze > 0 THEN 100.00 ELSE round((t.n_mod_since_analyze::numeric / t.n_live_tup::numeric),5) END pct_analyze,  " \
            "t.vacuum_count vac_cnt, t.autovacuum_count autovac_cnt, t.analyze_count ana_cnt, t.autoanalyze_count autoana_cnt, t.n_mod_since_analyze mod_since_ana, t.n_ins_since_vacuum  ins_since_vac, c.relkind, " \
            "GREATEST(t.last_vacuum, t.last_autovacuum)::date as last_vacuum,  GREATEST(t.last_analyze,t.last_autoanalyze)::date as last_analyze  " \
            "FROM pg_stat_user_tables t, pg_namespace n, pg_class c  " \
            "WHERE n.nspname = '%s' AND n.nspname = t.schemaname AND n.oid = c.relnamespace AND t.relname = c.relname AND (t.n_dead_tup > 0 OR t.n_mod_since_analyze > 0) ORDER BY 1, 2 "  % (schema)
  try:
       cur.execute(sql)
  except Exception as error:
      printit("Autotune Tables Exception: %s *** %s" % (type(error), error))
      conn.close()
      sys.exit (1)

  rows = cur.fetchall()
  if len(rows) == 0:
      printit ("AUTOTUNE: no threshold candidates to evaluate for vacuums/analyzes.")
  else:
      printit ("AUTOTUNE tables to be evaluated=%d.  Includes deferred ones too." % len(rows) )

  cnt = 0
  partcnt = 0
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
      table         = row[0]
      sizep         = row[1]
      size          = row[2]
      tups          = row[3]
      deadtups      = row[4]
      deadthresh    = row[5]
      pct_dead      = float(row[6])
      analthresh    = row[7]
      pct_anal      = float(row[8])
      vac_cnt       = row[9]
      autovac_cnt   = row[10]
      ana_cnt       = row[11]
      autoana_cnt   = row[12]
      mod_since_ana = row[13]
      ins_since_vac = row[14]
      relkind       = row[15]
      last_vacuum   = row[16]
      last_analyze  = row[17]

      #print (row)
      sql2 = ''
      if autotune <= pct_dead and autotune <= pct_anal:
        sql2 = 'VACUUM ANALYZE'
        action_name = 'VAC/ANALYZ'
      elif autotune <= pct_dead:
        sql2 = 'VACUUM'
        action_name = 'VACUUM'
      elif autotune <= pct_anal:
        sql2 = 'ANALYZE'
        action_name = 'ANALYZE'
      if sql2 == '':
        # nothing to do
        cnt = cnt - 1
        continue

      #if _verbose: printit('tbl=%s  sql=%s' % (table,sql2))

      if relkind == 'p' and ignoreparts:
          partcnt = partcnt + 1
          cnt = cnt - 1
          #print ("ignoring partitioned table: %s" % table)
          continue

      if size > threshold_max_size:
        # defer action
        printit ("Async %10s  %04d %-57s rows: %11d size: %10s :%13d dead: %10d analyzed: %10d  NOTICE: Skipping large table.  Do manually." \
                % (action_name, cnt, table, tups, sizep, size, deadtups, mod_since_ana))
        tables_skipped = tables_skipped + 1
        cnt = cnt - 1
        continue
      elif tups > threshold_async_rows or size > threshold_max_sync:
          if dryrun:
              if active_processes > threshold_max_processes:
                  printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                  tables_skipped = tables_skipped + 1
                  cnt = cnt - 1
                  continue
              printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %10d analyzed: %10d" % (action_name, cnt, table, tups, sizep, size, deadtups, mod_since_ana))
              tablist.append(table)
              check_maxtables()
              if len(tablist) > threshold_max_tables:
                  printit ("Max Tables Reached: %d." % len(tablist))
              active_processes = active_processes + 1
          else:
              if active_processes > threshold_max_processes:
                  printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                  tables_skipped = tables_skipped + 1
                  cnt = cnt - 1
                  continue
              tbl= table.replace('"', '\\"')
              tbl= tbl.replace('$', '\$')
              cmd = 'nohup psql -d "%s" -c "%s %s" 2>/dev/null &' % (connparms, sql2, tbl)
              time.sleep(0.5)
              asyncjobs = asyncjobs + 1
              printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %10d analyzed: %10d" % (action_name, cnt, table, tups, sizep, size, deadtups, mod_since_ana))
              rc = execute_cmd(cmd)
              tablist.append(table)
              check_maxtables()
              active_processes = active_processes + 1
      else:
          if dryrun:
              tablist.append(table)
              printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %10d analyzed: %10d" % (action_name, cnt, table, tups, sizep, size, deadtups, mod_since_ana))
          else:
              printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %10d analyzed: %10d" % (action_name, cnt, table, tups, sizep, size, deadtups, mod_since_ana))
              sql = "%s %s" % (sql2, table)
              time.sleep(0.5)
          try:
              cur.execute(sql)
          except Exception as error:
              printit("Exception: %s *** %s" % (type(error), error))
              cnt = cnt - 1
              continue
          tablist.append(table)
          check_maxtables()

  if ignoreparts:
      printit ("Partitioned tables bypassed=%d" % partcnt)
      partitioned_tables_skipped = partitioned_tables_skipped + partcnt

  printit ("Tables vacuumed/analyzed: %d" % cnt)

  if inquiry:
    rc = _inquiry(conn,cur,tablist)

  conn.close()
  printit ("End of Autotune action.  Closing the connection and exiting normally.")
  sys.exit(0)


#################################
# 5. Vacuum and Analyze query
#################################

if _verbose: printit("VERBOSE MODE: (5) Vacuum/Analyze section")

# V2.3: Fixed query date problem
#V 2.4 Fix, needed to add logic to check for null timestamps!
'''
-- all >10+
SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,
pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,
u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze,
c.relispartition, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,
to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,
CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END
FROM pg_namespace n, pg_class c, pg_stat_user_tables u where c.relnamespace = n.oid and u.schemaname = n.nspname and u.relname = c.relname AND c.relkind in ('r','m','p')
AND (u.n_dead_tup >= -1 AND now()::date - GREATEST(last_analyze, last_autoanalyze)::date > -1 AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > -1
OR (-1 = -1 AND -1 = -1 AND -1 = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL))
ORDER BY 1;
-- all < 10
SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,
pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,
u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze::bigint AS since_analyzed, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,
to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,
CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END, now()::date - GREATEST(last_analyze, last_autoanalyze)::date
FROM pg_namespace n, pg_class c, pg_stat_user_tables u where n.oid = c.relnamespace AND c.relkind in ('r','m','p') AND c.relname = u.relname and n.nspname = u.schemaname
AND (u.n_dead_tup >= -1 AND now()::date - GREATEST(last_analyze, last_autoanalyze)::date > -1 AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > -1
OR (-1 = -1 AND -1 = -1 AND -1 = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL))
ORDER BY 1;

'''
# V4.3 fix: relation size < instead of > maxsize!
# V5.0 fix: New logic regarding vacuuming/analyzing. We don't consider null vacuum/analyze timestamps anymore
if pgversion > 100000:
    if schema == "":
      sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
      "u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze::bigint, c.relispartition, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
      "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,  " \
      "CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END " \
      "FROM pg_namespace n, pg_class c, pg_stat_user_tables u where c.relnamespace = n.oid AND u.schemaname = n.nspname and u.relname = c.relname AND c.relkind in ('r','m','p') " \
      "AND (u.n_dead_tup >= %d AND now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d " \
      "OR (%d = -1 AND %d = -1 AND %d = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL)) " \
      "ORDER BY 1" % (threshold_dead_tups, threshold_max_days_analyze, threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_analyze, threshold_max_days_vacuum)
    else:
      sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
      "u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze::bigint, c.relispartition, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
      "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,  " \
      "CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END " \
      "FROM pg_namespace n, pg_class c, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = '%s' and u.schemaname = n.nspname and u.relname = c.relname AND c.relkind in ('r','m','p') " \
      "AND (u.n_dead_tup >= %d AND now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d " \
      "OR (%d = -1 AND %d = -1 AND %d = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL)) " \
      "ORDER BY 1" % (schema, threshold_dead_tups, threshold_max_days_analyze, threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_analyze, threshold_max_days_vacuum)
else:
# put version 9.x compatible query here
#CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False' ELSE 'True' END as partitioned
    if schema == "":
      sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
      "u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze::bigint, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
      "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,  " \
      "CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END " \
      "FROM pg_namespace n, pg_class c, pg_stat_user_tables u where c.relnamespace = n.oid AND u.schemaname = n.nspname and u.relname = c.relname AND c.relkind in ('r','m','p') " \
      "AND (u.n_dead_tup >= %d AND now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d " \
      "OR (%d = -1 AND %d = -1 AND %d = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL)) " \
      "ORDER BY 1" % (threshold_dead_tups, threshold_max_days_analyze, threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_analyze, threshold_max_days_vacuum)
    else:
      sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,  " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup,  " \
      "u.n_dead_tup::bigint AS dead_tup, u.n_mod_since_analyze::bigint, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,  " \
      "to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze,  " \
      "CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END " \
      "FROM pg_namespace n, pg_class c, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = '%s' and u.schemaname = n.nspname and u.relname = c.relname AND c.relkind in ('r','m','p') " \
      "AND (u.n_dead_tup >= %d AND now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d " \
      "OR (%d = -1 AND %d = -1 AND %d = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL)) " \
      "ORDER BY 1" % (schema, threshold_dead_tups, threshold_max_days_analyze, threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_analyze, threshold_max_days_vacuum)

try:
     cur.execute(sql)
except Exception as error:
    printit("Exception: %s *** %s" % (type(error), error))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No vacuum/analyze pairs to be done.")
else:
    printit ("vacuums/analyzes to be evaluated=%d.  Asterisk (*) indicates no vacuums/analyzes for table exists." % len(rows) )

cnt = 0
partcnt = 0
action_name = 'VAC/ANALYZ'
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
    table    = row[0]
    sizep    = row[1]
    size     = row[2]
    tups     = row[3]
    live     = row[4]
    dead     = row[5]
    analyzed = row[6]
    part     = row[7]
    vacs     = row[12]

    if vacs == 'NOVACS':
       ASYNC= '*Async'
       SYNC = '*Sync'
    else:
       ASYNC= ' Async'
       SYNC = ' Sync'

    if part and ignoreparts:
        partcnt = partcnt + 1
        #print ("ignoring partitioned table: %s" % table)
        continue

    # check if we already processed this table
    if skip_table(table, tablist):
        continue

    if size > threshold_max_size:
        # defer action
        if dryrun:
            printit ("%s %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d NOTICE: Skipping large table.  Do manually." % (ASYNC, action_name, cnt, table, tups, sizep, size, dead, analyzed))
            tablist.append(table)
            check_maxtables()
            tables_skipped = tables_skipped + 1
        continue
    elif tups > threshold_async_rows or size > threshold_max_sync:
    #elif (tups > threshold_async_rows or size > threshold_max_sync) and async_:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                tablist.append(table)
                check_maxtables()
                continue
            printit ("%s %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d" % (ASYNC, action_name, cnt, table, tups, sizep, size, dead, analyzed))
            total_vacuums_analyzes = total_vacuums_analyzes + 1
            tablist.append(table)
            check_maxtables()
            active_processes = active_processes + 1
        else:
            if active_processes > threshold_max_processes:
                printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tablist.append(table)
                check_maxtables()
                tables_skipped = tables_skipped + 1
                continue
            printit ("%s %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d" % (ASYNC, action_name, cnt, table, tups, sizep, size, dead, analyzed))
            asyncjobs = asyncjobs + 1

            # cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (ANALYZE, VERBOSE, %s) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, parallelstatement, table)
            # V4.1 fix, escape double quotes
            tbl= table.replace('"', '\\"')
            tbl= tbl.replace('$', '\$')
            if parallelstatement == '':
              cmd = 'nohup psql -d "%s" -c "VACUUM (ANALYZE, VERBOSE) %s" 2>/dev/null &' % (connparms, tbl)
            else:
              cmd = 'nohup psql -d "%s" -c "VACUUM (ANALYZE, VERBOSE, %s) %s" 2>/dev/null &' % (connparms, parallelstatement, tbl)
            time.sleep(0.5)
            rc = execute_cmd(cmd)
            print("rc=%d" % rc)
            total_vacuums_analyzes = total_vacuums_analyzes + 1
            tablist.append(table)
            check_maxtables()
            active_processes = active_processes + 1

    else:
        if dryrun:
            printit ("%s  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d" % (SYNC, action_name, cnt, table, tups, sizep, size, dead, analyzed))
            total_vacuums_analyzes = total_vacuums_analyzes + 1
            tablist.append(table)
            check_maxtables()
        else:
            printit ("%s  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d" % (SYNC, action_name, cnt, table, tups, sizep, size, dead, analyzed))
            if parallelstatement == '':
              sql = "VACUUM (ANALYZE, VERBOSE) %s" % (table)
            else:
              sql = "VACUUM (ANALYZE, VERBOSE, %s) %s" % (parallelstatement, table)
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except Exception as error:
                printit("Exception: %s *** %s" % (type(error), error))
                continue
            total_vacuums_analyzes = total_vacuums_analyzes + 1
            tablist.append(table)
            check_maxtables()

if ignoreparts:
    printit ("Partitioned table vacuum/analyzes bypassed=%d" % partcnt)
    partitioned_tables_skipped = partitioned_tables_skipped + partcnt

printit ("Tables vacuumed/analyzed: %d" % cnt)


#################################
# 6. Vacuum determination query #
#################################
'''
-- all

'''
# v4.0 fix: >= dead tups not just > dead tups
# v4.0 fix: also add max days for vacuum to where condition
# V4.3 fix: also add max relation size to the filter
# V4.7 fix: Use ORing condition for max days and dead tuples not ANDing
# V5.0 fix: New logic regarding vacuuming/analyzing. We don't consider null vacuum/analyze timestamps anymore
if _verbose: printit("VERBOSE MODE: (6) Vacuum query section")

if orderbydate:
    ORDERBY="GREATEST(last_vacuum, last_autovacuum) asc"
else:
    ORDERBY="1 asc"

if pgversion > 100000:
    if schema == "":
       sql = "SELECT psut.schemaname || '.\"' || psut.relname || '\"' as table, to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, " \
      "c.reltuples::bigint AS n_tup,  psut.n_dead_tup::bigint AS dead_tup, pg_size_pretty(pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname))::bigint), " \
      "pg_total_relation_size(quote_ident(psut.schemaname) || '.' ||quote_ident(psut.relname)) as size, c.relispartition, to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * c.reltuples), '9G999G999G999') AS av_threshold, CASE WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * c.reltuples) < psut.n_dead_tup THEN '*' ELSE '' END AS expect_av,  " \
      "CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END " \
      "FROM pg_stat_user_tables psut JOIN pg_class c on psut.relid = c.oid  where psut.schemaname not in ('pg_catalog', 'pg_toast', 'information_schema') AND c.relkind in ('r','m','p') " \
      "AND (now()::date - GREATEST(last_vacuum, last_autovacuum)::date >= %d AND psut.n_dead_tup >= %d " \
      "OR (%d = -1 AND %d = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL)) " \
      "ORDER BY %s;" % (threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_vacuum, threshold_dead_tups, ORDERBY)
      ####"ORDER BY 1 asc;" % (threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_vacuum, threshold_dead_tups)
    else:
       sql = "SELECT psut.schemaname || '.\"' || psut.relname || '\"' as table, to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, " \
      "c.reltuples::bigint AS n_tup,  psut.n_dead_tup::bigint AS dead_tup, pg_size_pretty(pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname))::bigint), " \
      "pg_total_relation_size(quote_ident(psut.schemaname) || '.' ||quote_ident(psut.relname)) as size, c.relispartition, to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * c.reltuples), '9G999G999G999') AS av_threshold, CASE WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * c.reltuples) < psut.n_dead_tup THEN '*' ELSE '' END AS expect_av, " \
      "CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END as vacs " \
      "FROM pg_stat_user_tables psut JOIN pg_class c on psut.relid = c.oid  where psut.schemaname = '%s' AND c.relkind in ('r','m','p') " \
      "AND (now()::date - GREATEST(last_vacuum, last_autovacuum)::date >= %d AND psut.n_dead_tup >= %d " \
      "OR (%d = -1 AND %d = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL)) " \
      "ORDER BY %s;" % (schema, threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_vacuum, threshold_dead_tups, ORDERBY)
      ####"ORDER BY 1 asc;" % (schema, threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_vacuum, threshold_dead_tups)
else:
    # put version 9.x compatible query here
    # CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False' ELSE 'True' END as partitioned
    if schema == "":
       sql = "SELECT psut.schemaname || '.\"' || psut.relname || '\"' as table, to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, " \
      "pg_class.reltuples::bigint AS n_tup,  psut.n_dead_tup::bigint AS dead_tup, pg_size_pretty(pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname))::bigint), " \
      "pg_total_relation_size(quote_ident(psut.schemaname) || '.' ||quote_ident(psut.relname)) as size, " \
      "CASE WHEN (SELECT pg_class.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=pg_class.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, " \
      "to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples), '9G999G999G999') AS av_threshold, CASE WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples) < psut.n_dead_tup THEN '*' ELSE '' END AS expect_av, " \
      "CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END as vacs " \
      "FROM pg_stat_user_tables psut JOIN pg_class on psut.relid = pg_class.oid  where psut.schemaname not in ('pg_catalog', 'pg_toast', 'information_schema') AND c.relkind in ('r','m','p') " \
      "AND (now()::date - GREATEST(last_vacuum, last_autovacuum)::date >= %d AND psut.n_dead_tup >= %d " \
      "OR (%d = -1 AND %d = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL)) " \
      "ORDER BY %s;" % (threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_vacuum, threshold_dead_tups, ORDERBY)
      ####"ORDER BY 1 asc;" % (threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_vacuum, threshold_dead_tups)
    else:
       sql = "SELECT psut.schemaname || '.\"' || psut.relname || '\"' as table, to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, " \
      "pg_class.reltuples::bigint AS n_tup,  psut.n_dead_tup::bigint AS dead_tup, pg_size_pretty(pg_total_relation_size(quote_ident(psut.schemaname) || '.' || quote_ident(psut.relname))::bigint) pretty, " \
      "pg_total_relation_size(quote_ident(psut.schemaname) || '.' ||quote_ident(psut.relname)) as size, " \
      "CASE WHEN (SELECT pg_class.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=pg_class.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, " \
      "to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples), '9G999G999G999') AS av_threshold, CASE WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint) + " \
      "(CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric) * pg_class.reltuples) < psut.n_dead_tup THEN '*' ELSE '' END AS expect_av,  " \
      "CASE WHEN last_vacuum is null and last_autovacuum is null THEN 'NOVACS' ELSE 'VACS' END as vacs " \
      "FROM pg_stat_user_tables psut JOIN pg_class on psut.relid = pg_class.oid  where psut.schemaname = '%s' AND c.relkind in ('r','m','p') " \
      "AND (now()::date - GREATEST(last_vacuum, last_autovacuum)::date >= %d AND psut.n_dead_tup >= %d " \
      "OR (%d = -1 AND %d = -1 AND last_vacuum IS NULL AND last_autovacuum IS NULL AND last_analyze IS NULL AND last_autoanalyze IS NULL)) " \
      "ORDER BY %s;" % (schema, threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_vacuum, threshold_dead_tups, ORDERBY)
      ####"ORDER BY 1 asc;" % (schema, threshold_max_days_vacuum, threshold_dead_tups, threshold_max_days_vacuum, threshold_dead_tups)
try:
     cur.execute(sql)
except Exception as error:
    printit("Exception: %s *** %s" % (type(error), error))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No vacuums to be done.")
else:
    printit ("vacuums to be evaluated=%d.  Asterisk (*) indicates no vacuums for table exists." % len(rows) )

cnt = 0
partcnt = 0
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
    table         = row[0]
    last_vacuum   = row[1]
    last_autovac  = row[2]
    tups          = row[3]
    dead          = row[4]
    sizep         = row[5]
    size          = row[6]
    part          = row[7]
    av_threshold  = row[8]
    expect_av     = row[9]
    vacs          = row[10]
    if vacs == 'NOVACS':
       ASYNC= '*Async'
       SYNC = '*Sync'
    else:
       ASYNC= ' Async'
       SYNC = ' Sync'

    if part and ignoreparts:
        partcnt = partcnt + 1
        #print ("ignoring partitioned table: %s" % table)
        cnt = cnt - 1
        continue

    # check if we already processed this table
    if skip_table(table, tablist):
        cnt = cnt - 1
        continue
    #else:
        #printit("table = %s will NOT be skipped." % table)

    if size > threshold_max_size:
        # defer action
        printit ("%s %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d NOTICE: Skipping large table.  Do manually." % (ASYNC, action_name, cnt, table, tups, sizep, size, dead))
        tables_skipped = tables_skipped + 1
        tablist.append(table)
        check_maxtables()
        cnt = cnt - 1
        continue
    elif tups > threshold_async_rows or size > threshold_max_sync:
    #elif (tups > threshold_async_rows or size > threshold_max_sync) and async_:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%s %10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (ASYNC,action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("%s %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (ASYNC,action_name, cnt, table, tups, sizep, size, dead))
            tablist.append(table)
            check_maxtables()
            total_vacuums  = total_vacuums + 1
            active_processes = active_processes + 1
        else:
            if active_processes > threshold_max_processes:
                printit ("%s %10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (ASYNC, action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                cnt = cnt - 1
                continue
            printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            asyncjobs = asyncjobs + 1

            # cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (VERBOSE, %s) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, parallelstatement, table)
            # V4.1 fix, escape double quotes
            tbl= table.replace('"', '\\"')
            tbl= tbl.replace('$', '\$')
            if parallelstatement == '':
              cmd = 'nohup psql -d "%s" -c "VACUUM (VERBOSE) %s" 2>/dev/null &' % (connparms, tbl)
            else:
              cmd = 'nohup psql -d "%s" -c "VACUUM (VERBOSE, %s) %s" 2>/dev/null &' % (connparms, parallelstatement, tbl)

            time.sleep(0.5)
            rc = execute_cmd(cmd)
            total_vacuums  = total_vacuums + 1
            active_processes = active_processes + 1
            tablist.append(table)
            check_maxtables()

    else:
        if dryrun:
            printit ("%s  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (SYNC, action_name, cnt, table, tups, sizep, size, dead))
            total_vacuums  = total_vacuums + 1
            tablist.append(table)
            check_maxtables()
        else:
            printit ("%s  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" %  (SYNC, action_name, cnt, table, tups, sizep, size, dead))
            if parallelstatement == '':
              sql = "VACUUM (VERBOSE) %s" % (table)
            else:
              sql = "VACUUM (VERBOSE, %s) %s" % (parallelstatement, table)
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except Exception as error:
                printit("Exception: %s *** %s" % (type(error), error))
                cnt = cnt - 1
                continue
            total_vacuums  = total_vacuums + 1
            tablist.append(table)
            check_maxtables()

if ignoreparts:
    printit ("Partitioned table vacuums bypassed=%d" % partcnt)
    partitioned_tables_skipped = partitioned_tables_skipped + partcnt

printit ("Tables vacuumed: %d" % cnt)

#################################
# 7. Analyze Section            #
#################################
'''
select n.nspname || '.' || c.relname as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, u.n_mod_since_analyze::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname))::bigint),
pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, c.relispartition, u.last_analyze, u.last_autoanalyze,
CASE WHEN c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and
u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 5 and
pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= 50000000 order by 1,2;

-- public schema
select n.nspname || '.' || c.relname as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, u.n_mod_since_analyze::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname))::bigint),  pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, c.relispartition, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = 'public' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > 5 and
pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= 50000000 order by 1,2;
'''

if _verbose: printit("VERBOSE MODE: (7) Analyze section")
if pgversion > 100000:
    if schema == "":
       sql = "select n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, u.n_mod_since_analyze::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' ||  " \
      "quote_ident(c.relname))::bigint), pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, c.relispartition, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 " \
      "THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, now()::date  - last_analyze::date as lastanalyzed2 " \
      "from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and " \
      "t.schemaname = n.nspname and t.tablename = c.relname and " \
      "c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d " \
      "and pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= %d order by 1,2" % (threshold_max_days_analyze, threshold_max_size)
    else:
       sql = "select n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, u.n_mod_since_analyze::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || " \
      "quote_ident(c.relname))::bigint), pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, c.relispartition, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 " \
      "THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, " \
      "now()::date  - last_analyze::date as lastanalyzed2 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname and " \
      "t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d " \
      "and pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= %d order by 1,2" % (schema, threshold_max_days_analyze, threshold_max_size)
else:
# put version 9.x compatible query here
# CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False' ELSE 'True' END as partitioned
    if schema == "":
       sql = "select n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, u.n_mod_since_analyze::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' ||  " \
      "quote_ident(c.relname))::bigint), pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i " \
      "JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 " \
      "THEN -1 ELSE round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, " \
      "now()::date  - last_analyze::date as lastanalyzed2 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname not in " \
      "('pg_catalog', 'pg_toast', 'information_schema') and t.schemaname = n.nspname and t.tablename = c.relname and " \
      "c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog') and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d " \
      "and pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= %d order by 1,2" % (threshold_max_days_analyze, threshold_max_size)
    else:
       sql = "select n.nspname || '.\"' || c.relname || '\"' as table, c.reltuples::bigint, u.n_live_tup::bigint, u.n_dead_tup::bigint, u.n_mod_since_analyze::bigint, pg_size_pretty(pg_total_relation_size(quote_ident(n.nspname) || '.' || " \
      "quote_ident(c.relname))::bigint), pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as size, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i " \
      "JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False' ELSE 'True' END as partitioned, u.last_analyze, u.last_autoanalyze, case when c.reltuples = 0 THEN -1 ELSE " \
      "round((u.n_live_tup / c.reltuples) * 100) END as tupdiff, " \
      "now()::date  - last_analyze::date as lastanalyzed2 from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname and " \
      "t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and now()::date - GREATEST(last_analyze, last_autoanalyze)::date > %d " \
      "and pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) <= %d order by 1,2" % (schema, threshold_max_days_analyze, threshold_max_size)

if debug: printit("DEBUG   MODE: (7) %s" % sql)

try:
    cur.execute(sql)
except Exception as error:
    printit("Exception: %s *** %s" % (type(error), error))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No tables require analyzes to be done.")
else:
    printit ("Table analyzes to be evaluated=%d" % len(rows) )

cnt = 0
partcnt = 0
action_name = 'ANALYZE'
for row in rows:
    cnt = cnt + 1
    table    = row[0]
    tups     = row[1]
    dead     = row[3]
    analyzed = row[4]
    sizep    = row[5]
    size     = row[6]
    part     = row[7]

    if part and ignoreparts:
        partcnt = partcnt + 1
        #print *"ignoring partitioned table: %s" % table)
        cnt = cnt - 1
        continue

    # check if we already processed this table
    if skip_table(table, tablist):
        cnt = cnt - 1
        continue

    if analyzed < minmodanalyzed:
        #print ('skipping table under threshold level: %s' % table)
        cnt = cnt - 1
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
            printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d  NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, sizep, size, dead, analyzed))
            tables_skipped = tables_skipped + 1
        else:
            printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d  NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, sizep, size, dead, analyzed))
            tables_skipped = tables_skipped + 1
        continue
    elif size > threshold_max_sync:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %-57s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d" % (action_name, cnt, table, tups, sizep, size, dead, analyzed))
            active_processes = active_processes + 1
            total_analyzes  = total_analyzes + 1
            tablist.append(table)
            check_maxtables()
        else:
            if active_processes > threshold_max_processes:
                printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %-57s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                cnt = cnt - 1
                continue
            printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d" % (action_name, cnt, table, tups, sizep, size, dead, analyzed))
            tablist.append(table)
            check_maxtables()
            asyncjobs = asyncjobs + 1

            # cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "ANALYZE VERBOSE %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, table)
            # V4.1 fix, escape double quotes
            tbl= table.replace('"', '\\"')
            tbl= tbl.replace('$', '\$')
            cmd = 'nohup psql -d "%s" -c "ANALYZE VERBOSE %s" 2>/dev/null &' % (connparms, tbl)

            time.sleep(0.5)
            rc = execute_cmd(cmd)
            active_processes = active_processes + 1
            total_analyzes  = total_analyzes + 1
    else:
        if dryrun:
            printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d" % (action_name, cnt, table, tups, sizep, size, dead, analyzed))
            total_analyzes  = total_analyzes + 1
            tablist.append(table)
            check_maxtables()
        else:
            printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d analyzed: %8d" % (action_name, cnt, table, tups, sizep, size, dead, analyzed))
            tablist.append(table)
            check_maxtables()
            sql = "ANALYZE VERBOSE %s" % table
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except Exception as error:
                printit("Exception: %s *** %s" % (type(error), error))
                cnt = cnt - 1
                continue
            total_analyzes  = total_analyzes + 1

if ignoreparts:
    printit ("Big partitioned table analyzes bypassed=%d" % partcnt)
    partitioned_tables_skipped = partitioned_tables_skipped + partcnt

printit ("Tables analyzed: %d" % cnt)


#################################
# 8. Catchall query for analyze that have not happened for over 30 days weeks.
#    Removed this section since we are trying to out think the user
#################################
if _verbose: printit("VERBOSE MODE: (8) Skipping Catchall analyze section for old analyzes > 30 days")

#################################
# 9. Catchall query for vacuums that have not happened past vacuum max days threshold
#################################
# V2.3: Introduced
'''
-- all
-- v10 or higher
SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, c.relispartition, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 5 and u.n_dead_tup >= 1000 order by 1,1;

-- v9.6 or lower
SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty,
pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup,
CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned,
to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,
to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and
t.schemaname = n.nspname  and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND
now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 30 and u.n_dead_tup >= 0 order by 1,1;


-- public schema
SELECT u.schemaname || '.' || u.relname as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, c.relispartition, to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze, to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze
FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = 'public' and t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and now()::date - GREATEST(last_vacuum, last_autovacuum)::date > 30 and u.n_dead_tup >= 0 order by 1,1;
'''
# v 4.0 fix: >= dead tups, not > only
# V4.3 fix: also add max relation size to the filter
if _verbose: printit("VERBOSE MODE: (9) Catchall vacuum query section")
if pgversion > 100000:
    if schema == "":
       sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, c.relispartition, " \
      "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,  " \
      "to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and t.schemaname = n.nspname  " \
      "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND  " \
      "now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d and u.n_dead_tup >= %d order by 1,1" % (threshold_max_days_vacuum, threshold_dead_tups)
    else:
       sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, c.relispartition, " \
      "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,  " \
      "to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname  " \
      "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname  AND  now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d and u.n_dead_tup >= %d order by 1,1" % (schema, threshold_max_days_vacuum, threshold_dead_tups)
else:
# put version 9.x compatible query here
# CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False' ELSE 'True' END as partitioned
    if schema == "":
       sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, " \
      "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,  " \
      "to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname not in ('pg_catalog', 'pg_toast', 'information_schema') and t.schemaname = n.nspname  " \
      "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname and n.nspname not in ('information_schema','pg_catalog', 'pg_toast') AND  " \
      "now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d and u.n_dead_tup >= %d order by 1,1" % (threshold_max_days_vacuum, threshold_dead_tups)
    else:
       sql = "SELECT u.schemaname || '.\"' || u.relname || '\"' as table, pg_size_pretty(pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname))::bigint) as size_pretty, " \
      "pg_total_relation_size(quote_ident(u.schemaname) || '.' || quote_ident(u.relname)) as size, c.reltuples::bigint AS n_tup, u.n_live_tup::bigint as n_live_tup, u.n_dead_tup::bigint AS dead_tup, CASE WHEN (SELECT c.relname AS child FROM pg_inherits i JOIN pg_class p ON (i.inhparent=p.oid) where i.inhrelid=c.oid) IS NULL THEN 'False'::boolean ELSE 'True'::boolean END as partitioned, " \
      "to_char(u.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum, to_char(u.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum, to_char(u.last_analyze,'YYYY-MM-DD HH24:MI') as last_analyze,  " \
      "to_char(u.last_autoanalyze,'YYYY-MM-DD HH24:MI') as last_autoanalyze FROM pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and t.schemaname = '%s' and t.schemaname = n.nspname  " \
      "and t.tablename = c.relname and c.relname = u.relname and u.schemaname = n.nspname  AND  now()::date - GREATEST(last_vacuum, last_autovacuum)::date > %d and u.n_dead_tup >= %d order by 1,1" % (schema, threshold_max_days_vacuum, threshold_dead_tups)

try:
     cur.execute(sql)
except Exception as error:
    printit("Exception: %s *** %s" % (type(error), error))
    conn.close()
    sys.exit (1)

rows = cur.fetchall()
if len(rows) == 0:
    printit ("No old vacuums to be done > maxdays=%d." % threshold_max_days_vacuum)
else:
    printit ("vacuums to be evaluated=%d greater than maxdays=%d and dead tups > %d" % (len(rows), threshold_max_days_vacuum, threshold_dead_tups))

cnt = 0
partcnt = 0
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
    part   = row[6]

    if part and ignoreparts:
        partcnt = partcnt + 1
        #print ("ignoring partitioned table: %s" % table)
        continue

    # check if we already processed this table
    if skip_table(table, tablist):
        continue

    if size > threshold_max_size:
        # defer action
        if dryrun:
            printit ("Async %10s: %04d %-57s rows: %11d  dead: %8d  size: %10s :%13d NOTICE: Skipping large table.  Do manually." % (action_name, cnt, table, tups, dead, sizep, size))
            tables_skipped = tables_skipped + 1
        continue
    elif tups > threshold_async_rows or size > threshold_max_sync:
    #elif (tups > threshold_async_rows or size > threshold_max_sync) and async_:
        if dryrun:
            if active_processes > threshold_max_processes:
                printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            total_vacuums = total_vacuums + 1
            tablist.append(table)
            check_maxtables()
            active_processes = active_processes + 1
        else:
            if active_processes > threshold_max_processes:
                printit ("%10s: Max processes reached. Skipping further Async activity for very large table, %s.  Size=%s.  Do manually." % (action_name, table, sizep))
                tables_skipped = tables_skipped + 1
                continue
            printit ("Async %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            asyncjobs = asyncjobs + 1

            # cmd = 'nohup psql -h %s -d %s -p %s -U %s -c "VACUUM (VERBOSE, %s) %s" 2>/dev/null &' % (hostname, dbname, dbport, dbuser, parallelstatement, table)
            # V4.1 fix, escape double quotes
            tbl= table.replace('"', '\\"')
            tbl= tbl.replace('$', '\$')
            if parallelstatement == '':
              cmd = 'nohup psql -d "%s" -c "VACUUM (VERBOSE) %s" 2>/dev/null &' % (connparms, tbl)
            else:
              cmd = 'nohup psql -d "%s" -c "VACUUM (VERBOSE, %s) %s" 2>/dev/null &' % (connparms, parallelstatement, tbl)

            time.sleep(0.5)
            rc = execute_cmd(cmd)
            total_vacuums = total_vacuums + 1
            tablist.append(table)
            check_maxtables()
            active_processes = active_processes + 1

    else:
        if dryrun:
            printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            tablist.append(table)
            check_maxtables()
        else:
            printit ("Sync  %10s: %04d %-57s rows: %11d size: %10s :%13d dead: %8d" % (action_name, cnt, table, tups, sizep, size, dead))
            if parallelstatement == '':
              sql = "VACUUM (VERBOSE) %s" % (table)
            else:
              sql = "VACUUM (VERBOSE, %s) %s" % (parallelstatement, table)
            time.sleep(0.5)
            try:
                cur.execute(sql)
            except Exception as error:
                printit("Exception: %s *** %s" % (type(error), error))
                continue

            total_vacuums = total_vacuums + 1
            tablist.append(table)
            check_maxtables()

if ignoreparts:
    printit ("Very old partitioned table vacuums bypassed=%d" % partcnt)
    partitioned_tables_skipped = partitioned_tables_skipped + partcnt

# wait for up to 2 hours for ongoing vacuums/analyzes to finish.
if not dryrun:
    wait_for_processes(conn,cur)

printit ("Vacuum Freeze: %d  Vacuum Analyze: %d  Total Vacuums: %d  Total Analyzes: %d  Skipped Partitioned Tables: %d  Total Skipped Tables: %d  Total Async Jobs: %d " \
         % (total_freezes, total_vacuums_analyzes, total_vacuums, total_analyzes, partitioned_tables_skipped, tables_skipped + partitioned_tables_skipped, asyncjobs))
rc = get_query_cnt(conn, cur)
if rc > 0:
    printit ("NOTE: Current vacuums/analyzes still in progress: %d" % (rc))

# v3.1 feature: show async jobs running
#ps -ef | grep 'psql -h '| grep -v '\--color'
psjobs = "ps -ef | grep 'psql -h %s'| grep -v '\--color'" % hostname
#print ("psjobs = %s" % psjobs)

if inquiry:
  _inquiry(conn,cur,tablist)

# Close communication with the database
conn.close()
printit ("Closed the connection and exiting normally.")
sys.exit(0)
