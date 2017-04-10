# coding:utf8

import os
import cx_Oracle
import etl_hdfs
import etl_time
import etl_global


# 获得oracle数据库连接
def get_conn():
    try:
        os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
        # NJUST 192.10.86.13:1521/orclrac
        conn = cx_Oracle.connect('xijia', 'dba!@#', '192.10.86.13:1521/orclrac')
        cur = conn.cursor()
        cur.execute("select 5 from dual")
        row = cur.fetchone()
        cur.close()
        if row:
            return conn

    except Exception, e:
        print '连接异常'
        print e


def p_judge_proc(indaynum=1):
    try:
        etl_global._init()

        inacctday = etl_time.get_time(indaynum)
        jud_conn = get_conn()
        print '获得数据库连接'
        cur_jud = jud_conn.cursor()

        out_msg1 = cur_jud.var(cx_Oracle.STRING)
        out_flag2 = cur_jud.var(cx_Oracle.NUMBER)
        out_acctday3 = cur_jud.arrayvar(cx_Oracle.STRING, [None] * 30)
        out_procs4 = cur_jud.arrayvar(cx_Oracle.STRING, [None] * 30)
        out_type5 = cur_jud.arrayvar(cx_Oracle.NUMBER, [None] * 30)
        out_method6 = cur_jud.arrayvar(cx_Oracle.NUMBER, [None] * 30)
        out_strategy7 = cur_jud.arrayvar(cx_Oracle.NUMBER, [None] * 30)
        v_retcode8 = cur_jud.var(cx_Oracle.STRING)
        v_retinfo9 = cur_jud.var(cx_Oracle.STRING)

        print '调用p_judge_run开始'
        cur_jud.callproc('xijia.pkg_etl_model.p_judge_run', [inacctday,
                                                             out_msg1,
                                                             out_flag2,
                                                             out_acctday3,
                                                             out_procs4,
                                                             out_type5,
                                                             out_method6,
                                                             out_strategy7,
                                                             v_retcode8,
                                                             v_retinfo9])

        print '调用p_judge_run完成'
        cur_jud.close()
        jud_conn.close()

        etl_global.set_value('synacctday', out_acctday3.getvalue())
        etl_global.set_value('procs_strs', out_procs4.getvalue())
        etl_global.set_value('type_nums', out_type5.getvalue())
        etl_global.set_value('method_nums', out_method6.getvalue())
        etl_global.set_value('strategy_nums', out_strategy7.getvalue())
        etl_global.set_value('out_flag', out_flag2.getvalue())

        print out_msg1.getvalue()

    except Exception, e:
        print e


def callsynproc(procname, inacctday, syntype, synmethod, synstrategy):
    try:
        db_conn = get_conn()
        cursor = db_conn.cursor()
        if synmethod == 1:
            out_retcode = cursor.var(cx_Oracle.STRING)
            out_retinfo = cursor.var(cx_Oracle.STRING)
            cursor.callproc(
                procname, [inacctday, syntype, out_retcode, out_retinfo])
            print '%s(%s): %s' % (procname, inacctday, out_retcode.getvalue())
        else:
            out_hdfs_path = cursor.var(cx_Oracle.STRING)
            out_file_name = cursor.var(cx_Oracle.STRING)
            out_spool_sh = cursor.var(cx_Oracle.STRING)
            out_retcode = cursor.var(cx_Oracle.STRING)
            out_retinfo = cursor.var(cx_Oracle.STRING)
            out_proc_num = cursor.var(cx_Oracle.NUMBER)
            cursor.callproc('xijia.p_insert_log', [
                            inacctday, procname, syntype, out_proc_num])
            in_proc_num = out_proc_num.getvalue()
            cursor.callproc('xijia.pkg_etl_model.p_table_spool_sh', [
                            inacctday, procname, syntype, synstrategy, out_hdfs_path, out_file_name, out_spool_sh, out_retcode, out_retinfo])
            if out_retcode.getvalue() == 'success':
                print '生成spool脚本成功'
                put_hdfs_res = etl_hdfs.put_hdfs(out_spool_sh.getvalue(
                ), out_file_name.getvalue(), out_hdfs_path.getvalue())
                if put_hdfs_res == 'success':
                    cursor.callproc('xijia.p_update_log', [
                                    in_proc_num, 'finish', 0, put_hdfs_res, ''])
                    print '同步%s(%s): success' % (procname, inacctday)
                else:
                    cursor.callproc('xijia.p_update_log', [
                                    in_proc_num, 'break', 0, 'error', put_hdfs_res])
                    print '同步%s(%s): error' % (procname, inacctday)
            else:
                cursor.callproc('xijia.p_update_log', [
                                in_proc_num, 'break', 0, out_retcode.getvalue(), out_retinfo.getvalue()])
                print '同步%s(%s): break' % (procname, inacctday)
        cursor.close()
        db_conn.close()

    except Exception, e:
        print e

if __name__ == '__main__':
    print 'etl_oracle.py'
    x = get_conn()
    x.close()
    p_judge_proc()
