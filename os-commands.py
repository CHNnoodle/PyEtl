# coding:utf-8

import os
import commands
import hdfs

os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
in_acct_day = '20170329'
oscmd='sh /root/spool/spool.sh ' + in_acct_day
(status, output) = commands.getstatusoutput(oscmd)
print status, output

client = hdfs.Client("http://192.10.86.31:50070",root="/",timeout=100,session=False)
client.upload("/user/hdfs/emp2/","/root/spool/prompt.txt") 

