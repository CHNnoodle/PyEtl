# coding:utf8

import sys
import os
import commands
import logging
import hdfs


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
        try:
            os.chdir("/data/log")
        except OSError:
            os.makedirs("/data/log")

        LogFile = r'/data/log/etl.log'
        format_str = '[%(asctime)s] thread-%(thread)-8d [line:%(lineno)-3d]: %(levelname)-6s: %(message)s'
        datefmt_str = '%Y-%m-%d %H:%M:%S'

        # 通过logging.basicConfig函数对日志的输出做相关配置，这里指定输出level为INFO，默认不输出INFO
        logging.basicConfig(level=logging.INFO,
                            format=format_str,
                            datefmt=datefmt_str,
                            filename=LogFile,
                            filemode='a')  # w是覆盖 a是追加

        logger.info('初始化logging')
        return logger

    except Exception, e:
        logger.error(e)
        raise Exception(e)


def put_hdfs(filename, hdfs_path='/user/hdfs/url/', local_path='/data/ftp/'):
    try:
        acctday = filename[6:14] + '/'
        hdfs_filepath = hdfs_path + acctday + filename
        local_filepath = local_path + filename
        logger.info('本地文件：%s' % local_filepath)
        logger.info('HDFS文件：%s' % hdfs_filepath)
        logger.info('开始上传数据到hdfs')
        client = hdfs.Client("http://192.10.86.31:50070",
                             root="/", timeout=100, session=False)
        client.upload(hdfs_filepath, local_filepath)
        logger.info('upload数据完成')
        oscmd = 'mv ' + local_filepath + ' /data/url/'
        # oscmd = 'ls'
        logger.info('转移本地文件')
        logger.info(oscmd)
        (status, output) = commands.getstatusoutput(oscmd)
        if status == 0:
            (retcode, retinfo) = ('success', '')
            logger.info(retcode + retinfo)
        else:
            (retcode, retinfo) = ('fail', 'mv file fail')
            logger.info(retcode + ' : ' + retinfo)

    except Exception, e:
        logger.error(e)
        raise Exception(e)

if __name__ == '__main__':
    logger = init_logger()
    if len(sys.argv) < 2:
        raise Exception('没有入参')
    else:
        infilename = sys.argv[1]
    # put_hdfs('3_url_20170315155330_00058.txt')
    put_hdfs(infilename,'/user/hdfs/','/root/')
