# coding:utf8

import cx_Oracle
import hdfs


def put_hdfs(dns, sql, filename,local_path='/root/spooldata/'):
    try:
        count = ''
        with cx_Oracle.connect(dns) as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                res = cursor.fetchall()
                count = cursor.rowcount
                print '%s:OJDBC导出%s行数据' % (filename, count)
            except Exception, e:
                print e
                raise Exception(e)

            finally:
                cursor.close()

        filepath = local_path + filename
        with open(filepath, 'wb+') as f:
            for row_data in res:
                f.write(''.join(row_data).encode('utf-8') + '\n')
            print '写入本地文件%s完成' % filepath
        retcode='success'
        retinfo =''

    except Exception, e:
        (retcode, retinfo) = ('fail', e)
        print e
        raise Exception('fail', e)

    finally:
        return count, retcode, retinfo


if __name__ == '__main__':
    os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
    sql = '''select ACCT_MONTH || '|*|' || ACCT_DAY || '|*|' || ACCTSESSIONID || '|*|' ||
       USERNAME || '|*|' || AREA_TYPE || '|*|' ||
       to_char(ACCTSTARTTIME, 'yyyymmdd hh24:mi:ss') || '|*|' ||
       to_char(ACCTSTOPTIME, 'yyyymmdd hh24:mi:ss') || '|*|' ||
       ACCTSESSIONTIME || '|*|' || ACCTINPUTOCTETS_IPV4 || '|*|' ||
       ACCTOUTPUTOCTETS_IPV4 || '|*|' || AMOUNT_IPV4 || '|*|' || number2ip(user_ipv4) ||
       '|*|' || MAC || '|*|' || number2ip(nas_ip) || '|*|' || SVLAN || '|*|' || PVLAN
  from STAGE_LOG.t_ACCT where ACCT_DAY='20170509' '''
    dns = r'xijia/dba#789@NJUST'
    (x, y, z) = put_hdfs(dns, sql, '20170509', '/root')
    print x
    print y
