# coding:utf8


import sys
import os
import time
import datetime
import logging
import threading
import cx_Oracle

from sqlalchemy import *
from PyLog import init_logger
from PyLog import getdns
from PySynProc import syn_proc


def main_control(d_info):
    try:
        logging.info('主进程开始调度')
        logging.info('检查过程同步情况')
        meta_db = create_engine(d_info.get('meta_dns'))
        meta_conn = meta_db.raw_connection()
        meta_cursor = meta_conn.cursor()
        out_msg = meta_cursor.var(cx_Oracle.STRING)
        out_flag = meta_cursor.var(cx_Oracle.NUMBER)
        out_procs = meta_cursor.arrayvar(cx_Oracle.STRING, [None] * 30)
        retcode = meta_cursor.var(cx_Oracle.STRING)
        retinfo = meta_cursor.var(cx_Oracle.STRING)
        args = [d_info.get('inacctday'), out_msg, out_flag, out_procs, retcode, retinfo]
        logging.info('调用p_judge_run开始')
        meta_cursor.callproc("xijia.pkg_py_etl.p_judge_run", args)

        if retcode.getvalue() == 'success':
            logging.info('调用p_judge_run完成')

            for line in out_msg.getvalue().splitlines():
                logging.info(line)

            logging.info('判断是否有待执行过程')
            procs_strs = out_procs.getvalue()
            if procs_strs:
                logging.info('发现待执行过程,循环并发调用过程')
                for index in range(len(procs_strs)):
                    print d_info
                    syn_args = (d_info, procs_strs[index])
                    t = threading.Thread(target=syn_proc, args=syn_args)
                    t.start()

            flag = out_flag.getvalue()
            if flag == 1:
                inc = 600
                logging.info('当日过程已执行完成,主进程进入长等待,十分钟')
                time.sleep(inc)
            else:
                inc = 60
                logging.info('仍有过程未执行完成，主进程进入60秒等待')
                time.sleep(inc)
        else:
            raise Exception('fail', '调用xijia.pkg_py_etl.p_judge_run失败，需要检查！')

    except Exception, etp:
        raise Exception('fail', etp)


if __name__ == '__main__':
    d_syninfo = {'localpath': '/Users/wanggang/'}
    logger = init_logger(d_syninfo)
    try:
        os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
        d_syninfo['meta_dns'] = getdns('META')

        # 判断是否有入参
        if len(sys.argv) < 2:
            daynum = 1
        else:
            daynum = int(sys.argv[1])

        # 初始化入参 同步日期
        inacctday = (datetime.datetime.today() - datetime.timedelta(days=daynum)).strftime("%Y%m%d")
        logging.info('日同步日期: %s' % inacctday)
        while True:
            d_syninfo['inacctday'] = inacctday
            main_control(d_syninfo)
            inacctday = (datetime.datetime.today() - datetime.timedelta(days=daynum)).strftime("%Y%m%d")

    except Exception, e:
        logging.error(e)
        logging.info('异常，终止调度')
    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt')
