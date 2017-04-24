# coding:utf8

import logging
import logging.handlers
import os

import etl_oracle
import etl_global


# 设置日志系统
def init_logger(logpath='/root/PyEtl/'):
    try:
        # 创建logger
        # 创建handler
        # 定义formatter
        # 给handler添加formatter
        # 给logger添加handler

        # 生成一个日志对象  这里如果指定logging.getLogger(__name__)那么仅仅收集主文件的日志
        logger = logging.getLogger()

        # 格式化字符串，此时跟logger没有关系
        try:
            os.chdir(logpath)
        except OSError:
            os.makedirs(logpath)

        LogFile = logpath + 'etl.log'
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


if __name__ == '__main__':
    try:
        etl_global._init()
        logger = init_logger('/Users/wanggang/')
        logger.info('This is info message')
        os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
        indns = r'xijia/dba#789@NJUST'
        print 'etl_oracle.py'
        etl_oracle.p_judge_proc(indns, '20170424')

    except Exception, e:
        print e
