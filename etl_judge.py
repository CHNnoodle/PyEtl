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
        cursor = db_conn.cursor () 

        out_all_num1 = cursor.var(cx_Oracle.NUMBER) 
        out_valid_num2 = cursor.var(cx_Oracle.NUMBER)
        out_day_done_num3 = cursor.var(cx_Oracle.NUMBER)
        out_time_max_str4 = cursor.var(cx_Oracle.STRING)
        out_acctday_strs5 = cursor.arrayvar(cx_Oracle.STRING,[None]*30)
        out_procs_strs6 = cursor.arrayvar(cx_Oracle.STRING,[None]*30)
        out_type_nums7 = cursor.arrayvar(cx_Oracle.NUMBER,[None]*30)
        out_method_nums8 = cursor.arrayvar(cx_Oracle.NUMBER,[None]*30)
        out_strategy_nums9 = cursor.arrayvar(cx_Oracle.NUMBER,[None]*30) 
        out_retcode_str10 = cursor.var(cx_Oracle.STRING)
        out_retinfo_str11 = cursor.var(cx_Oracle.STRING)   
        print '调用p_judge_run开始'
        print '判断符合执行条件存储过程'
        cursor.callproc('xijia.pkg_etl_model.p_judge_run',[inacctday,out_all_num1,out_valid_num2,out_day_done_num3,out_time_max_str4,
                                                           out_acctday_strs5,out_procs_strs6,out_type_nums7,out_method_nums8,
                                                           out_strategy_nums9,out_retcode_str10,out_retinfo_str11])

        print '调用p_judge_run完成'
        cursor.close ()  
        db_conn.close () 

        print '有效存储过程共%d个' %out_valid_num2.getvalue()
        print '日同步待执行存储过程共%d个' %out_all_num1.getvalue()
        print '目前已执行完成存储过程%d个' %out_day_done_num3.getvalue()

        synacctday = out_acctday_strs5.getvalue()
        procs_strs = out_procs_strs6.getvalue()
        type_nums = out_type_nums7.getvalue()
        method_nums = out_method_nums8.getvalue()
        strategy_nums = out_strategy_nums9.getvalue()
        # for index in range(len(procs_strs)):
        #     print procs_strs[index]



        print '判断是否有待执行存储过程'
        if procs_strs :
            print '发现待执行存储过程,循环并发调用存储过程'
            for index in range(len(procs_strs)):
                t = threading.Thread(target=etl_oracle.callsynproc,args=(procs_strs[index],synacctday[index],type_nums[index],method_nums[index],strategy_nums[index]))
                print '执行存储过程 %s'%procs_strs[index]  
                t.start()

        if out_all_num1.getvalue()==out_day_done_num3.getvalue():
            inc = 600
            print '当日过程已执行完成,主进程进入长等待,十分钟'
            time.sleep(inc)
        else :
            inc = 30
            print '仍有未执行过程，主进程进入等待时间,默认30秒'
            time.sleep(inc)
    except Exception,e:
        print e      
    
if __name__=='__main__':
    if len(sys.argv) < 2: 
        daynum = 1
    else :
        daynum = int(sys.argv[1])
    nowhour = int(etl_time.get_time(1,2))
    if nowhour<3 :
        print '未到调度时间，终止调度'
    while (nowhour >=3) :
        p_judge_run(daynum)
        nowhour = int(etl_time.get_time(1,2))
    print '终止调度'
            
