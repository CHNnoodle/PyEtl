# coding:utf8

import sys
import etl_time
import time
import threading
import sched
import etl_oracle
import cx_Oracle


def p_judge_run(indaynum):
    try:
        print '主进程开始调度'
        inacctday = etl_time.get_time(indaynum)
        db_conn = etl_oracle.get_conn()
        print '获得数据库连接'
        cursor = db_conn.cursor()

        out_all_num1 = cursor.var(cx_Oracle.NUMBER)
        out_day_valid_num2 = cursor.var(cx_Oracle.NUMBER)
        out_now_valid_num3 = cursor.var(cx_Oracle.NUMBER)
        out_day_done_num4 = cursor.var(cx_Oracle.NUMBER)
        out_now_done_num5 = cursor.var(cx_Oracle.NUMBER)
        out_time_max_str6 = cursor.var(cx_Oracle.STRING)
        out_acctday_strs7 = cursor.arrayvar(cx_Oracle.STRING, [None] * 30)
        out_procs_strs8 = cursor.arrayvar(cx_Oracle.STRING, [None] * 30)
        out_type_nums9 = cursor.arrayvar(cx_Oracle.NUMBER, [None] * 30)
        out_method_nums10 = cursor.arrayvar(cx_Oracle.NUMBER, [None] * 30)
        out_strategy_nums11 = cursor.arrayvar(cx_Oracle.NUMBER, [None] * 30)
        out_retcode_str12 = cursor.var(cx_Oracle.STRING)
        out_retinfo_str13 = cursor.var(cx_Oracle.STRING)
        print '调用p_judge_run开始'
        print '判断符合执行条件存储过程'
        cursor.callproc('xijia.pkg_etl_model.p_judge_run', [inacctday, out_all_num1, out_day_valid_num2, out_now_valid_num3, out_day_done_num4,
                                                            out_now_done_num5, out_time_max_str6, out_acctday_strs7, out_procs_strs8,
                                                            out_type_nums9, out_method_nums10, out_strategy_nums11, out_retcode_str12, out_retinfo_str13])

        print '调用p_judge_run完成'
        cursor.close()
        db_conn.close()

        out_all = out_all_num1.getvalue()
        out_valid_all = out_day_valid_num2.getvalue() + out_now_valid_num3.getvalue()
        out_done_all = out_day_done_num4.getvalue() + out_now_done_num5.getvalue()
        print '待同步过程总数%d个' % out_all
        print '待同步有效过程总数%d个' % out_valid_all
        print '已执行有效过程总数%d个' % out_done_all
        
        print '日同步有效过程共%d个' % out_day_valid_num2.getvalue()
        print '已执行日同步过程共%d个' % out_day_done_num4.getvalue()
        print '实时有效过程共%d个' % out_now_valid_num3.getvalue()
        print '已执行实时过程共%d个' % out_now_done_num5.getvalue()


        synacctday = out_acctday_strs7.getvalue()
        procs_strs = out_procs_strs8.getvalue()
        type_nums = out_type_nums9.getvalue()
        method_nums = out_method_nums10.getvalue()
        strategy_nums = out_strategy_nums11.getvalue()
        # for index in range(len(procs_strs)):
        #     print procs_strs[index]

        print '判断是否有待执行存储过程'
        if procs_strs:
            print '发现待执行存储过程,循环并发调用存储过程'
            for index in range(len(procs_strs)):
                t = threading.Thread(target=etl_oracle.callsynproc, args=(procs_strs[index], synacctday[
                                     index], type_nums[index], method_nums[index], strategy_nums[index]))
                print '执行存储过程 %s' % procs_strs[index]
                t.start()

        # out_all = out_all_num1.getvalue()
        # out_done_all = out_day_done_num4.getvalue() + out_now_done_num5.getvalue()

        if out_all == out_done_all:
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
