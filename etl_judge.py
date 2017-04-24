# coding:utf8


import sys
import os
import time
import logging
import threading
import etl_time
import etl_ftp
import etl_global
import etl_logger
import etl_oracle


def main_control(dns, inacctday):
    try:
        logging.info('主进程开始调度')

        logging.info('检查URL日志文件')
        filenames = etl_ftp.get_filenames()
        res = 0
        if filenames:
            for infilename in filenames:
                logging.info('上传%s' % infilename)
                res = etl_ftp.put_file(dns, infilename)
                if res == 1:
                    break

        if res == 0:
            logging.info('上传URL日志文件完成')
        else:
            logging.info('上传URL日志文件失败')

        logging.info('检查过程同步情况')
        t = threading.Thread(target=etl_oracle.p_judge_proc,
                             args=(dns, inacctday,))
        t.start()
        t.join()

        synacctday = etl_global.get_value('synacctday')
        procs_strs = etl_global.get_value('procs_strs')
        type_nums = etl_global.get_value('type_nums')
        method_nums = etl_global.get_value('method_nums')
        strategy_nums = etl_global.get_value('strategy_nums')
        out_flag = etl_global.get_value('out_flag')

        logging.info('判断是否有待执行过程')
        if procs_strs:
            logging.info('发现待执行过程,循环并发调用过程')
            for index in range(len(procs_strs)):
                t = threading.Thread(target=etl_oracle.callsynproc, args=(dns, procs_strs[index], synacctday[
                    index], type_nums[index], method_nums[index], strategy_nums[index]))
                logging.info('执行过程 %s' % procs_strs[index])
                t.start()

        if out_flag == 1:
            inc = 600
            logging.info('当日过程已执行完成,主进程进入长等待,十分钟')
            time.sleep(inc)
        else:
            inc = 60
            logging.info('仍有过程未执行完成，主进程进入30秒等待')
            time.sleep(inc)
    except Exception, e:
        logging.error(e)
        raise Exception('fail', e)

if __name__ == '__main__':
    logger = etl_logger.init_logger()
    etl_global._init()
    try:
        os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
        dns = r'xijia/dba#789@NJUST'

        # 判断是否有入参
        if len(sys.argv) < 2:
            daynum = 1
        else:
            daynum = int(sys.argv[1])

        inacctday = etl_time.get_time(daynum)

        while (inacctday > 0):

            main_control(dns, inacctday)
            inacctday = etl_time.get_time(daynum)

    except Exception, e:
        logger.error(e)
        logger.info('异常，终止调度')
    except KeyboardInterrupt:
        logger.info('KeyboardInterrupt')
