# coding:utf-8

import os
import commands
import datetime

if __name__=='__main__':
        print 'etl_monitor'
        print '当前时间 ' + datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        oscmd = 'ps -ef | grep python | grep etl_judge.py | grep -v grep'
        (res_status,res_output) = commands.getstatusoutput(oscmd)
        if res_output :
                print res_output
                print 'etl调度正常，进程无需重启'
        else :
                print 'etl调度中断，重启调度程序'
                os.chdir("/root/PyEtl/")
                pycmd = 'nohup python etl_judge.py 1>/dev/null 2>&1 &'
                osres = os.popen(pycmd)
                (os_status,os_output) = commands.getstatusoutput(oscmd)
                if os_output :
                        print os_output
                        print 'etl调度启动成功'
                else :
                        print 'etl进程启动失败'
        print '退出etl_monitor'