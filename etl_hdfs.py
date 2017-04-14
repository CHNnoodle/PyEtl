# coding:utf-8

import os
import commands
import hdfs
import time
import subprocess

# shcontext spool文件的内容
# filename 对应生成文件的前缀
# hdfs_path hdfs文件路径


def put_hdfs(shcontext, filename, hdfs_path):
    try:
        sh_path = '/root/spoolsh/' + filename + '.sh'
        txt_local_path = '/root/spooldata/' + filename + '.txt'

        with open(sh_path, 'wb+') as f:
            f.write(shcontext)

        print '写入pool脚本'
        oscmd1 = 'chmod +x ' + sh_path
        (res_status1, res_output1) = commands.getstatusoutput(oscmd1)
        print sh_path
        oscmd2 = 'sh ' + sh_path

        output = subprocess.Popen(oscmd2, stdout=subprocess.PIPE, shell=True)
        out, err = output.communicate()
        print out
        print err
        if not err:
            print 'spool数据成功'

        print '上传数据到hdfs'
        txt_hdfs_path = hdfs_path + filename + '.txt'
        client = hdfs.Client("http://192.10.86.31:50070",
                             root="/", timeout=100, session=False)
        client.delete(hdfs_path, recursive=True)
        client.upload(txt_hdfs_path, txt_local_path)

        # oscmd3 = 'rm -f ' + txt_local_path
        print '删除本地文件'
        res3 = os.system(oscmd3)
        return 'success'
    except Exception, e:
        print e
        return e

if __name__ == '__main__':
    print 'etl_hdfs'
