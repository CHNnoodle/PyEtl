# coding:utf8

import datetime
import logging


# 设置日志系统
def init_logger(inday):
    try:
        # 获取时间，用于构建日志文件名称
        LogName = r'/Users/wanggang/Downloads/log_%s.log' % inday
        format_str = '[%(asctime)s] %(filename)s [line:%(lineno)-3d]: %(levelname)-6s: %(message)s'
        datefmt_str = '%Y-%m-%d %H:%M:%S'

        logger = logging.getLogger(__name__)
        # 通过logging.basicConfig函数对日志的输出格式及方式做相关配置
        logging.basicConfig(level=logging.DEBUG,
                            format=format_str,
                            datefmt=datefmt_str,
                            filename=LogName,
                            filemode='a')  # w是覆盖 a是追加
        # 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter(format_str, datefmt_str)
        console.setFormatter(formatter)
        logger.addHandler(console)

        logger.info('初始化logging')
        return logger
    except Exception, e:
        logger.error(e)


if __name__ == '__main__':
    try:
        logger = init_logger('20170414')
        logger.info('This is info message')
        print 1 / 0
    except Exception, e:
        logger.error(e)
