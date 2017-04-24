# coding:utf8

import os
import cx_Oracle
import logging
import etl_global
import etl_hdfs


# 插入日志
def p_insert_log(dns, procname, inacctday, syntype):
    with cx_Oracle.connect(dns) as db_conn:
        try:
            cursor = db_conn.cursor()
            out_proc_num = cursor.var(cx_Oracle.NUMBER)
            cursor.callproc('xijia.p_insert_log', [
                inacctday, procname, syntype, out_proc_num])
            return out_proc_num.getvalue()
        except Exception, e:
            logging.error(e)
            raise Exception(e)
        finally:
            cursor.close()


def p_update_log(dns, in_proc_num, finish_flag, rowcount, retcode, retinfo):
    '''更新日志'''
    with cx_Oracle.connect(dns) as db_conn:
        try:
            cursor = db_conn.cursor()
            cursor.callproc('xijia.p_update_log', [
                in_proc_num, finish_flag, rowcount, retcode, retinfo])
        except Exception, e:
            logging.error(e)
            raise Exception(e)
        finally:
            cursor.close()


def p_judge_proc(dns, inacctday):
    try:
        with cx_Oracle.connect(dns) as jud_conn:
            try:
                logging.info('调度日期--%s' % inacctday)
                logging.info('获得数据库连接')
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

                logging.info('调用p_judge_run开始')
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

                logging.info('调用p_judge_run完成')

                etl_global.set_value('synacctday', out_acctday3.getvalue())
                etl_global.set_value('procs_strs', out_procs4.getvalue())
                etl_global.set_value('type_nums', out_type5.getvalue())
                etl_global.set_value('method_nums', out_method6.getvalue())
                etl_global.set_value('strategy_nums', out_strategy7.getvalue())
                etl_global.set_value('out_flag', out_flag2.getvalue())
                for line in out_msg1.getvalue().splitlines():
                    logging.info(line)

            except Exception, e:
                logging.error(e)
                raise Exception(e)

            finally:
                cur_jud.close()
    except Exception, e:
        raise Exception('fail', e)


def callsynproc(dns, procname, inacctday, syntype, synmethod, synstrategy):
    try:
        with cx_Oracle.connect(dns) as db_conn:
            try:
                cursor = db_conn.cursor()
                if synmethod == 1:
                    out_retcode = cursor.var(cx_Oracle.STRING)
                    out_retinfo = cursor.var(cx_Oracle.STRING)
                    cursor.callproc(
                        procname, [inacctday, syntype, out_retcode, out_retinfo])
                    logging.info('%s(%s): %s' %
                                 (procname, inacctday, out_retcode.getvalue()))
                else:
                    out_hdfs_path = cursor.var(cx_Oracle.STRING)
                    out_file_name = cursor.var(cx_Oracle.STRING)
                    out_select_sql = cursor.var(cx_Oracle.STRING)
                    out_retcode = cursor.var(cx_Oracle.STRING)
                    out_retinfo = cursor.var(cx_Oracle.STRING)

                    in_proc_num = p_insert_log(
                        dns, procname, inacctday, syntype)
                    cursor.callproc('xijia.pkg_etl_model.p_table_select_sql', [
                                    inacctday, procname, syntype, synstrategy, out_hdfs_path, out_file_name, out_select_sql, out_retcode, out_retinfo])

                    if out_retcode.getvalue() == 'success':
                        logging.info('生成查询脚本成功')
                        (rowcount, hdfs_retcode, hdfs_retinfo) = etl_hdfs.put_hdfs(dns, out_select_sql.getvalue(
                        ), out_file_name.getvalue(), out_hdfs_path.getvalue())

                        if hdfs_retcode == 'success':
                            p_update_log(dns, in_proc_num, 'finish',
                                         rowcount, hdfs_retcode, hdfs_retinfo)
                            logging.info('同步%s(%s): success' %
                                         (procname, inacctday))
                        else:
                            p_update_log(dns, in_proc_num, 'break',
                                         rowcount, hdfs_retcode, hdfs_retinfo)
                            logging.info('同步%s(%s): error' %
                                         (procname, inacctday))
                    else:
                        p_update_log(dns, in_proc_num, 'break',
                                     0, out_retcode.getvalue(), out_retinfo.getvalue())
                        logging.info('同步%s(%s): break' % (procname, inacctday))

            except Exception, e:
                logging.error(e)
                raise Exception(e)

            finally:
                cursor.close()
    except Exception, e:
        raise Exception('fail', e)


if __name__ == '__main__':
    os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
    indns = r'xijia/dba#789@NJUST'
    etl_global._init()
    print 'etl_oracle.py'
    p_judge_proc(indns, '20170424')
