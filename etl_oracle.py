# coding:utf8

import cx_Oracle 
import etl_hdfs

#获得oracle数据库连接
def get_conn():
    try:
        db_conn = cx_Oracle.connect('xijia','dba!@#','NJUST')
        cursor = db_conn.cursor ()
        cursor.execute ("select 5 from dual")  
        row = cursor.fetchone () 
        if row :
            return db_conn

    except Exception,e:
        print '连接异常'
        print e

#调用oracle数据库存储过程    
#procname 存储过程名称
#proctype  
def callsynproc(procname,inacctday,syntype,synmethod,synstrategy):
    try:
        db_conn = get_conn()
        cursor = db_conn.cursor ()  
        if synmethod == 1:
            out_retcode = cursor.var(cx_Oracle.STRING)
            out_retinfo = cursor.var(cx_Oracle.STRING)
            cursor.callproc(procname,[inacctday,syntype,out_retcode,out_retinfo])
            print '%s(%s): %s' %(procname,inacctday,out_retcode.getvalue())
        else:
            out_hdfs_path = cursor.var(cx_Oracle.STRING)  
            out_file_name = cursor.var(cx_Oracle.STRING)  
            out_spool_sh = cursor.var(cx_Oracle.STRING)  
            out_retcode = cursor.var(cx_Oracle.STRING)
            out_retinfo = cursor.var(cx_Oracle.STRING)
            out_proc_num = cursor.var(cx_Oracle.NUMBER)
            cursor.callproc('xijia.p_insert_log',[inacctday,procname,syntype,out_proc_num])
            in_proc_num = out_proc_num.getvalue()
            cursor.callproc('xijia.pkg_etl_model.p_table_spool_sh',[inacctday,procname,syntype,synstrategy,out_hdfs_path,out_file_name,out_spool_sh,out_retcode,out_retinfo])
            if out_retcode.getvalue() == 'success' :
                print '生成spool脚本成功'
                put_hdfs_res = etl_hdfs.put_hdfs(out_spool_sh.getvalue(),out_file_name.getvalue(),out_hdfs_path.getvalue())
                if put_hdfs_res == 'success' :
                    cursor.callproc('xijia.p_update_log',[in_proc_num,'finish',0,put_hdfs_res,''])
                    print '同步%s(%s): success' %(procname,inacctday)
                else :
                    cursor.callproc('xijia.p_update_log',[in_proc_num,'break',0,'error',put_hdfs_res])
                    print '同步%s(%s): error' %(procname,inacctday)
            else :
                cursor.callproc('xijia.p_update_log',[in_proc_num,'break',0,out_retcode.getvalue(),out_retinfo.getvalue()])
                print '同步%s(%s): break' %(procname,inacctday)
        cursor.close ()  
        db_conn.close () 
        
    except Exception,e:
        print e

if __name__=='__main__':
    print 'etl_oracle.py'

