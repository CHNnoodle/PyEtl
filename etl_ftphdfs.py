# coding:utf8

import sys
import os
import commands
import logging
import hdfs
import paramiko


def init_logger(logpath='/root/PyEtl/'):
    try:
        # 创建logger
        # 创建handler
        # 定义formatter
        # 给handler添加formatter
        # 给logger添加handler

        # 生成一个日志对象
        logger = logging.getLogger()

        # 格式化字符串，此时跟logger没有关系
        try:
            os.chdir(logpath)
        except OSError:
            os.makedirs(logpath)

        LogFile = rLogFile = logpath + 'etl.log'
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
        if not os.path.isfile(local_filepath) or filename[-4:] == 'temp':
            raise EOFError("file is not a txt file.")
        logging.info('本地文件：%s' % local_filepath)
        logging.info('HDFS文件：%s' % hdfs_filepath)
        logging.info('开始上传数据到hdfs')
        client = hdfs.Client("http://192.10.86.31:50070",
                             root="/", timeout=100, session=False)
        client.upload(hdfs_filepath, local_filepath, overwrite=True)
        logging.info('upload数据完成')
        newpath = '/data/url/' + acctday
        try:
            os.chdir(newpath)
        except OSError:
            os.makedirs(newpath)
        oscmd = 'mv ' + local_filepath + ' ' + newpath
        logging.info('转移本地文件')
        logging.info(oscmd)
        (status, output) = commands.getstatusoutput(oscmd)
        if status == 0:
            (retcode, retinfo) = ('success', '')
            logging.info(retcode + retinfo)
        else:
            (retcode, retinfo) = ('fail', 'mv file fail')
            logging.info(retcode + ' : ' + retinfo)

    except EOFError, e:
        logging.info(e)
    except Exception, e:
        logging.error(e)
        raise Exception(e)

if __name__ == '__main__':
    logger = init_logger()
    if len(sys.argv) < 2:
        (status, output) = commands.getstatusoutput('ls /data/ftp')
        for infilename in output.split():
            print infilename
            put_hdfs(infilename)
    else:
        infilename = sys.argv[1]
        put_hdfs(infilename)
