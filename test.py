# coding:utf-8

# import os
# oracle_vars = dict((a,b) for a,b in os.environ.items() if a.find('ORACLE')>=0)
# from pprint import pprint
# pprint(oracle_vars)
# import time
# import datetime
# import subprocess

# before = time.clock()
# print before
# time.sleep(1)
# sep = time.clock()-before
# print sep

# a=datetime.datetime.now()
# nowdaytime = datetime.datetime.today()
# print a
# print nowdaytime

# retcode = subprocess.call('ls -l',shell=True)
#print retcode

# output = subprocess.Popen(['ifconfig','-all'],shell=True)  
# err = output.communicate() 
# print err

import etl_global
import cx_Oracle

etl_global._init()
conn = cx_Oracle.connect(etl_global.get_value('DNS'))
cur = conn.cursor()
cur.execute("select 5 from dual")
row = cur.fetchone()
cur.close()
conn.close()
print row
# def benchmark(options):
#   params = eval(options.bind) if options.bind else {}
#   with cx_Oracle.connect(options.db) as db:
#     try:
#       cursor = db.cursor()
#       before = time.clock()
#       for i in xrange(options.requests):
#         cursor.execute(options.sql, params)
#       return (time.clock()-before)/options.requests
#     except KeyboardInterrupt:
#       pass
#     finally:
#       cursor.close()

# import os
# from subprocess import Popen, PIPE

# sqlplus = Popen("sqlplus xj_select/xj_select@202.119.86.2:22337/orclrac",shell=True, stdout=PIPE, stdin=PIPE)
# sqlplus.stdin.write("select sysdate from dual;"+os.linesep)
# sqlplus.stdin.write("select count(*) from all_objects;"+os.linesep)
# out, err = sqlplus.communicate()
# print out