#!/bin/sh

table_name=$1
in_acct_day=$2
spool_type=$3
filename=${table_name}_${in_acct_day}_${spool_type}
echo "${table_name}"
echo "${in_acct_day}"
echo "${filename}"
DB_USER=xj_select   #DB USER
DB_PWD=xj_select   #DB PASSWORD
DB_SERV=NJUST   #DB SERVICE NAME

sqlplus -s $DB_USER/$DB_PWD@$DB_SERV<<EOF

set echo off;
set feedback off;
set trimspool on
set linesize 120
set pagesize 0
set newpage none
set heading off
set term off
set trimout on
spool /root/spool/${in_acct_day}
Select ACCT_DAY || '|*|' || USERNAME || '|*|' || FULLNAME || '|*|' ||
       CERTIFICATION_NO || '|*|' || MOBILE || '|*|' || STATUS || '|*|' ||
       CREATETIME
  from STAGE_LOG.T_USERINFO where acct_day=${in_acct_day};
spool off
EOF