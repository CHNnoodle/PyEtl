# coding:utf8

import logging
import logging.handlers
import os
import ConfigParser
from urllib import quote_plus as urlquote


def getdns(section):
    try:
        config = ConfigParser.ConfigParser()
        config.readfp(open('/Users/wanggang/git/PyYarn/config.ini'))
        try:
            s = config.get(section, 'dns')
            p = config.get(section, 'passwd')
            dns = s % urlquote(p)
            return dns





        except Exception, ept:
            raise Exception(ept)
    except Exception, ept:
        print ept
        logging.error(ept)
        raise Exception(ept)


# 设置日志系统
def init_logger(d_info):




    try:
        # 创建logger
        # 创建handler
        # 定义formatter
        # 给handler添加formatter
        # 给logger添加handler

        # 生成一个日志对象  这里如果指定logging.getLogger(__name__)那么仅仅收集主文件的日志

        logpath = d_info.get('localpath', '/root/PyEtl/')

        logger = logging.getLogger()

        # 格式化字符串，此时跟logger没有关系
        try:
            os.chdir(logpath)
        except OSError:
            os.makedirs(logpath)

        localfile = logpath + 'etl.log'
        format_str = '[%(asctime)s] thread-%(thread)-8d [line:%(lineno)-3d]: %(levelname)-6s: %(message)s'
        datefmt_str = '%Y-%m-%d %H:%M:%S'

        # 通过logging.basicConfig函数对日志的输出做相关配置，这里指定输出level为INFO，默认不输出INFO
        logging.basicConfig(level=logging.INFO)
        # 生成一个格式器，用于规范日志的输出格式
        formatter = logging.Formatter(format_str, datefmt_str)

        # 生成一个时间分割日志处理器,一天一分割
        flog = logging.handlers.TimedRotatingFileHandler(
            localfile, when='D', interval=1, backupCount=30)
        flog.suffix = "%Y%m%d.log"
        # 设置处理器级别
        flog.setLevel(logging.INFO)
        # 设置处理器格式
        flog.setFormatter(formatter)
        # 将处理器加到日志对象上
        logger.addHandler(flog)

        logging.info('初始化logging')
        return logger
    except Exception, etp:
        logging.error(etp)
        raise Exception('fail', etp)


if __name__ == '__main__':
    try:
        d_syninfo = {'localpath': '/Users/wanggang/'}
        init_logger(d_syninfo)
        logging.info('logging test')
        dnss = getdns('HDFS')
        print dnss
    except Exception, e:
        print e
