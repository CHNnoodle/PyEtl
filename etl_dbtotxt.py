# coding:utf8

import etl_oracle


def dbtotxt(sql,filename,hdfs_path,local_path = '/root/dbdata/'):
    try:
        conn = etl_oracle.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        filepath = local_path+filename+'.txt'
        with open(filepath, 'wb+') as f :
            for row_data in res:
                f.write(''.join(row_data).encode('utf-8')+'\n')

    except Exception, e:
        print e


if __name__ == '__main__':

    sql = '''select ACCT_DAY||'|*|'||USERNAME||'|*|'||FULLNAME||'|*|'||
    CERTIFICATION_NO||'|*|'||MOBILE||'|*|'||STATUS||'|*|'||CREATETIME from STAGE_LOG.T_USERINFO WHERE ROWNUM<=10'''
    dbtotxt(sql,'test','1','/Users/wanggang/Downloads/')

