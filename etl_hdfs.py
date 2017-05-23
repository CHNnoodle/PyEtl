# coding:utf8


import os
import logging
import cx_Oracle
import hdfs
import sys


def put_hdfs(dns, sql, filename, hdfs_path, local_path='/root/spooldata/'):
    try:
        reload(sys)
        sys.setdefaultencoding('utf-8') 
        count = ''
        with cx_Oracle.connect(dns) as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                res = cursor.fetchall()
                count = cursor.rowcount
                logging.info('%s:OJDBC导出%s行数据' % (filename, count))
            except Exception, e:
                logging.error(e)
                raise Exception(e)

            finally:
                cursor.close()

        filepath = local_path + filename
        with open(filepath, 'wb+') as f:
            for row_data in res:
                f.write(''.join(row_data).encode('utf-8') + '\n')
            logging.info('写入本地文件%s完成' % filepath)

        try:
            logging.info('上传数据到hdfs')
            txt_hdfs_path = hdfs_path + filename
            client = hdfs.Client("http://192.10.86.101:50070",
                                 root="/", timeout=100, session=False)
            if filename[16:22]!='v_acct':
                client.delete(hdfs_path, recursive=True)
            client.upload(txt_hdfs_path, filepath)
            logging.info('upload数据完成')
        except Exception, e:
            logging.error(e)
            raise Exception(e)

        oscmd = 'rm -f ' + filepath
        logging.info('删除本地文件')
        res = os.system(oscmd)

        (retcode, retinfo) = ('success', '')
    except Exception, e:
        (retcode, retinfo) = ('fail', e)
        logging.error(e)
        raise Exception('fail', e)

    finally:
        return count, retcode, retinfo


if __name__ == '__main__':
    sql = '''select ACCT_MONTH || '|*|' || ACCT_DAY || '|*|' || ACCTSESSIONID || '|*|' ||
       USERNAME || '|*|' || AREA_TYPE || '|*|' ||
       to_char(ACCTSTARTTIME, 'yyyymmdd hh24:mi:ss') || '|*|' ||
       to_char(ACCTSTOPTIME, 'yyyymmdd hh24:mi:ss') || '|*|' ||
       ACCTSESSIONTIME || '|*|' || ACCTINPUTOCTETS_IPV4 || '|*|' ||
       ACCTOUTPUTOCTETS_IPV4 || '|*|' || AMOUNT_IPV4 || '|*|' || number2ip(user_ipv4) ||
       '|*|' || MAC || '|*|' || number2ip(nas_ip) || '|*|' || SVLAN || '|*|' || PVLAN
  from STAGE_LOG.t_ACCT where ACCT_DAY='20170510' '''
    dns = r'xijia/dba#789@NJUST'
    (x, y, z) = put_hdfs(dns, sql, '20170510', '1', '/Users/wanggang/Downloads/')
    print x
    print y
