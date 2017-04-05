# coding:utf8

import cx_Oracle 
import datetime
import threading
import time
import logging
import sched

#设置日志系统
def init_logger():
    try:
        #获取时间，用于构建日志文件名称
        curDate = datetime.date.today()
        LogName =  r'C:\log\log_%s.log' %curDate
        format_str='[%(asctime)s] %(filename)s - %(name)s [line:%(lineno)-4d] %(levelname)-8s: %(message)s'
        datefmt_str = '%Y-%m-%d %H:%M:%S'
        
        logger = logging.getLogger(__name__)
        #通过logging.basicConfig函数对日志的输出格式及方式做相关配置
        logging.basicConfig(level=logging.DEBUG,
                        format=format_str,
                        datefmt=datefmt_str,
                        filename=LogName,
                        filemode='a')#w是覆盖 a是追加
        #定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console_format_str='[%(asctime)s] %(name)s - [line:%(lineno)-4d] %(levelname)-8s: %(message)s'
        console_datefmt_str = '%Y-%m-%d %H:%M:%S'    
        formatter = logging.Formatter(console_format_str,console_datefmt_str)
        console.setFormatter(formatter)
        logger.addHandler(console)
        
        logger.debug('This is debug message')
        logger.info('This is info message')
        logger.warning('This is warning message')
        return logger
    except Exception,e:
        print 
        logger.error(e) 