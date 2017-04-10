# coding:utf8

import sys
import etl_time
import time
import threading
import etl_oracle
import etl_global


def p_judge_run(indaynum):
    try:
        t = threading.Thread(target=etl_oracle.p_judge_proc, args=(indaynum,))
        t.start()
        t.join()

        synacctday = etl_global.get_value('synacctday')
        procs_strs = etl_global.get_value('procs_strs')
        type_nums = etl_global.get_value('type_nums')
        method_nums = etl_global.get_value('method_nums')
        strategy_nums = etl_global.get_value('strategy_nums')
        out_flag = etl_global.get_value('out_flag')

        print '判断是否有待执行存储过程'
        if procs_strs:
            print '发现待执行存储过程,循环并发调用存储过程'
            for index in range(len(procs_strs)):
                t = threading.Thread(target=etl_oracle.callsynproc, args=(procs_strs[index], synacctday[
                    index], type_nums[index], method_nums[index], strategy_nums[index]))
                print '执行存储过程 %s' % procs_strs[index]
                t.start()

        if out_flag == 1:
            inc = 600
            print '当日过程已执行完成,主进程进入长等待,十分钟'
            time.sleep(inc)
        else:
            inc = 30
            print '仍有未执行过程，主进程进入等待时间,默认30秒'
            time.sleep(inc)
    except Exception, e:
        print e

if __name__ == '__main__':
    if len(sys.argv) < 2:
        daynum = 1
    else:
        daynum = int(sys.argv[1])
    nowhour = int(etl_time.get_time(1, 2))
    if nowhour < 3:
        print '当前%s点' % nowhour
        print '未到调度时间，终止调度'
    while (nowhour >= 3):
        p_judge_run(daynum)
        nowhour = int(etl_time.get_time(1, 2))
    print '终止调度'
