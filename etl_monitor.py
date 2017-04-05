# coding:utf-8

import os
import commands

if __name__=='__main__':
        print 'etl_monitor'
        oscmd = 'ps -ef | grep python | grep etl_judge.py | grep -v grep'
        (res_status,res_output) = commands.getstatusoutput(oscmd)
        if res_output :
                print res_output
                print 'etl调度正常，进程无需重启'
        else :
                os.chdir("/root/etl/")
                pycmd = 'nohup stdbuf -oL python etl_judge.py >>etl.log 2>>$(date +%Y%m%d-%H-%M).err &'
                osres = os.popen(pycmd)
                (os_status,os_output) = commands.getstatusoutput(oscmd)
                if os_output :
                        print os_output
                        print 'etl调度已启动'
                else :
                        print 'etl进程启动失败'
        print '退出etl_monitor'