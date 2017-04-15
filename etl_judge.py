# coding:utf8


import sys
import os
import logging
import threading
import time
import datetime
import cx_Oracle
import hdfs

# 设置日志系统


def init_logger():
    try:
        # 创建logger
        # 创建handler
        # 定义formatter
        # 给handler添加formatter
        # 给logger添加handler

        # 生成一个日志对象
        logger = logging.getLogger(__name__)

        # 格式化字符串，此时跟logger没有关系
        LogFile = r'/root/PyEtl/etl.log'
        format_str = '[%(asctime)s] thread-%(thread)-8d [line:%(lineno)-3d]: %(levelname)-6s: %(message)s'
        datefmt_str = '%Y-%m-%d %H:%M:%S'

        # 通过logging.basicConfig函数对日志的输出做相关配置，这里指定输出level为INFO，默认不输出INFO
        logging.basicConfig(level=logging.INFO)
        # 生成一个格式器，用于规范日志的输出格式
        formatter = logging.Formatter(format_str, datefmt_str)

        # 生成一个时间分割日志处理器,一天一分割
        flog = logging.handlers.TimedRotatingFileHandler(
            LogFile, when='D', interval=1, backupCount=30)
        flog.suffix = "%Y%m%d.log"
        # 设置处理器级别
        flog.setLevel(logging.INFO)
        # 设置处理器格式
        flog.setFormatter(formatter)
        # 将处理器加到日志对象上
        logger.addHandler(flog)

        logger.info('初始化logging')
        return logger
    except Exception, e:
        logger.error(e)
        raise Exception('fail', e)


# 获取时间


def get_time(daynum, daytpye=1):
    try:
        nowdaytime = datetime.datetime.today()
        indaytime = datetime.datetime.today() - datetime.timedelta(days=daynum)  # 默认昨天
        inacctday = indaytime.strftime("%Y%m%d")
        nowhour = nowdaytime.strftime("%H")
        if daytpye == 1:
            return inacctday

        else:
            return nowhour
    except Exception, e:
        print e
        raise Exception('fail', e)


def _init():  # 初始化
    global _global_dict
    _global_dict = {}


def set_value(key, value):
    _global_dict[key] = value


def get_value(key, defValue=None):
    try:
        return _global_dict[key]
    except KeyError:
        return defValue


def p_judge_proc(dns, inacctday):
    try:
        with cx_Oracle.connect(dns) as jud_conn:
            try:
                logger.info('调度日期--%s' % inacctday)
                logger.info('获得数据库连接')
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

                logger.info('调用p_judge_run开始')
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

                logger.info('调用p_judge_run完成')

                set_value('synacctday', out_acctday3.getvalue())
                set_value('procs_strs', out_procs4.getvalue())
                set_value('type_nums', out_type5.getvalue())
                set_value('method_nums', out_method6.getvalue())
                set_value('strategy_nums', out_strategy7.getvalue())
                set_value('out_flag', out_flag2.getvalue())

                logger.info(out_msg1.getvalue())

            except Exception, e:
                logger.error(e)
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
                    logger.info('%s(%s): %s' %
                                (procname, inacctday, out_retcode.getvalue()))
                else:
                    out_hdfs_path = cursor.var(cx_Oracle.STRING)
                    out_file_name = cursor.var(cx_Oracle.STRING)
                    out_select_sql = cursor.var(cx_Oracle.STRING)
                    out_retcode = cursor.var(cx_Oracle.STRING)
                    out_retinfo = cursor.var(cx_Oracle.STRING)
                    out_proc_num = cursor.var(cx_Oracle.NUMBER)

                    cursor.callproc('xijia.p_insert_log', [
                                    inacctday, procname, syntype, out_proc_num])
                    in_proc_num = out_proc_num.getvalue()
                    cursor.callproc('xijia.pkg_etl_model.p_table_select_sql', [
                                    inacctday, procname, syntype, synstrategy, out_hdfs_path, out_file_name, out_select_sql, out_retcode, out_retinfo])

                    if out_retcode.getvalue() == 'success':
                        logger.info('生成查询脚本成功')
                        (rowcount, hdfs_retcode, hdfs_retinfo) = put_hdfs(dns, out_select_sql.getvalue(
                        ), out_file_name.getvalue(), out_hdfs_path.getvalue())

                        if hdfs_retcode == 'success':
                            cursor.callproc('xijia.p_update_log', [
                                            in_proc_num, 'finish', rowcount, hdfs_retcode, hdfs_retinfo])
                            logger.info('同步%s(%s): success' %
                                        (procname, inacctday))
                        else:
                            cursor.callproc('xijia.p_update_log', [
                                            in_proc_num, 'break', rowcount, hdfs_retcode, hdfs_retinfo])
                            logger.info('同步%s(%s): error' %
                                        (procname, inacctday))
                    else:
                        cursor.callproc('xijia.p_update_log', [
                                        in_proc_num, 'break', 0, out_retcode.getvalue(), out_retinfo.getvalue()])
                        logger.info('同步%s(%s): break' % (procname, inacctday))

            except Exception, e:
                logger.error(e)
                raise Exception(e)

            finally:
                cursor.close()
    except Exception, e:
        raise Exception('fail', e)


def main_control(dns, inacctday):
    try:
        logger.info('主进程开始调度')
        t = threading.Thread(target=p_judge_proc,
                             args=(dns, inacctday,))
        t.start()
        t.join()

        synacctday = get_value('synacctday')
        procs_strs = get_value('procs_strs')
        type_nums = get_value('type_nums')
        method_nums = get_value('method_nums')
        strategy_nums = get_value('strategy_nums')
        out_flag = get_value('out_flag')

        logger.info('判断是否有待执行过程')
        if procs_strs:
            logger.info('发现待执行过程,循环并发调用过程')
            for index in range(len(procs_strs)):
                t = threading.Thread(target=callsynproc, args=(dns, procs_strs[index], synacctday[
                    index], type_nums[index], method_nums[index], strategy_nums[index]))
                logger.info('执行过程 %s' % procs_strs[index])
                t.start()

        if out_flag == 1:
            inc = 600
            logger.info('当日过程已执行完成,主进程进入长等待,十分钟')
            time.sleep(inc)
        else:
            inc = 30
            logger.info('仍有过程未执行完成，主进程进入30秒等待')
            time.sleep(inc)
    except Exception, e:
        logger.error(e)
        raise Exception('fail', e)


def put_hdfs(dns, sql, filename, hdfs_path, local_path='/root/spooldata/'):
    try:
        count = ''
        with cx_Oracle.connect(dns) as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                res = cursor.fetchall()
                count = cursor.rowcount
                logger.info('OJDBC导出%s行数据' % count)
            except Exception, e:
                logger.error(e)
                raise Exception(e)

            finally:
                cursor.close()

        filepath = local_path + filename
        with open(filepath, 'wb+') as f:
            for row_data in res:
                f.write(''.join(row_data).encode('utf-8') + '\n')
            logger.info('写入本地文件%s完成' % filepath)

        try:
            logger.info('上传数据到hdfs')
            txt_hdfs_path = hdfs_path + filename
            client = hdfs.Client("http://192.10.86.31:50070",
                                 root="/", timeout=100, session=False)
            client.delete(hdfs_path, recursive=True)
            client.upload(txt_hdfs_path, filepath)
            logger.info('upload数据完成')
        except Exception, e:
            logger.error(e)
            raise Exception(e)

        oscmd = 'rm -f ' + filepath
        logger.info('删除本地文件')
        res = os.system(oscmd)

        (retcode, retinfo) = ('success', '')
    except Exception, e:
        (retcode, retinfo) = ('fail', e)
        raise Exception('fail', e)

    finally:
        return count, retcode, retinfo


if __name__ == '__main__':
    logger = init_logger()
    _init()
    try:
        os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
        dns = r'xijia/dba#789@NJUST'

        # 判断是否有入参
        if len(sys.argv) < 2:
            daynum = 1
        else:
            daynum = int(sys.argv[1])

        inacctday = get_time(daynum)
        nowhour = 3
        while (nowhour >= 3):

            main_control(dns, inacctday)
            nowhour = int(get_time(1, 2))

        logger.info('当前%s点' % nowhour)
        logger.info('未到调度时间，终止调度')

    except Exception, e:
        logger.error(e)
