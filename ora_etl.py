# coding:utf8

import cx_Oracle 
import datetime
import threading
import time
import logging
import sched


#设置日志系统
def init_logger():
    try:
        #获取时间，用于构建日志文件名称
        curDate = datetime.date.today()
        LogName =  r'C:\log\log_%s.log' %curDate
        format_str='[%(asctime)s] %(filename)s - %(name)s [line:%(lineno)-4d] %(levelname)-8s: %(message)s'
        datefmt_str = '%Y-%m-%d %H:%M:%S'
        
        logger = logging.getLogger(__name__)
        #通过logging.basicConfig函数对日志的输出格式及方式做相关配置
        logging.basicConfig(level=logging.DEBUG,
                        format=format_str,
                        datefmt=datefmt_str,
                        filename=LogName,
                        filemode='a')#w是覆盖 a是追加
        #定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console_format_str='[%(asctime)s] %(name)s - [line:%(lineno)-4d] %(levelname)-8s: %(message)s'
        console_datefmt_str = '%Y-%m-%d %H:%M:%S'    
        formatter = logging.Formatter(console_format_str,console_datefmt_str)
        console.setFormatter(formatter)
        logger.addHandler(console)
        
        logger.debug('This is debug message')
        logger.info('This is info message')
        logger.warning('This is warning message')
        return logger
    except Exception,e:
        print 
        logger.error(e) 

#获得oracle数据库连接
def get_conn():
    try:
        db_conn = cx_Oracle.connect('xijia','dba!@#','ORCLRAC')
        cursor = db_conn.cursor ()
        cursor.execute ("select 5 from dual")  
        row = cursor.fetchone () 
        if row :
            return db_conn
    except Exception,e:
        logger.error(e) 
        

#调用oracle数据库存储过程     
def callproc(procname,inacctday):
    try:
        db_conn = get_conn()
        cursor = db_conn.cursor ()  
        
        #yesterday = datetime.datetime.today()-datetime.timedelta(days=5)
        #str1 = yesterday.strftime("%Y%m%d")
        str2 = cursor.var(cx_Oracle.STRING)
        str3 = cursor.var(cx_Oracle.STRING)
        cursor.callproc(procname,[inacctday,str2,str3])
        logger.info('%s(%s): %s' %(procname,inacctday,str2.getvalue())) 
        cursor.close ()  
        db_conn.close () 
        
    except Exception,e:
        logger.error(e) 
    
def p_judge_run(inc = 30): 
    try:
        inacctdaytime = datetime.datetime.today()-datetime.timedelta(days=1) #默认昨天
        inacctday=inacctdaytime.strftime("%Y%m%d")
        logger.info('调度日期-- %s'%inacctday) 
        db_conn = get_conn()
        logger.info('主进程获得数据库连接')
        cursor = db_conn.cursor () 
        str_out = cursor.arrayvar(cx_Oracle.STRING,[None]*30)
        str2 = cursor.var(cx_Oracle.NUMBER) 
        str3 = cursor.var(cx_Oracle.NUMBER)  
        str6 = cursor.var(cx_Oracle.NUMBER)    
        str4 = cursor.var(cx_Oracle.STRING)
        str5 = cursor.var(cx_Oracle.STRING)   
        logger.info('调用p_judge_run开始') 
        logger.info('判断符合执行条件存储过程') 
        cursor.callproc('xijia.pkg_etl_model.p_judge_run',[inacctday,str_out,str2,str3,str6,str4,str5])
        logger.info('调用p_judge_run完成') 
        cursor.close ()  
        db_conn.close ()  
        logger.info('待执行存储过程共%d个' %str3.getvalue())
        logger.info('有效存储过程共%d个' %str6.getvalue())
        logger.info('目前已执行完成存储过程%d个' %str2.getvalue())  
        thread_cnt = 0 
        logger.info('判断是否有待执行存储过程')
        if str_out.getvalue() :
            logger.info('发现待执行存储过程,循环并发调用存储过程') 
            for now_thread in str_out.getvalue():
                t = threading.Thread(target=callproc,args=(now_thread,inacctday,))
                logger.info('执行存储过程 %s'%now_thread)  
                t.start()
                thread_cnt+=1
            logger.info('本次循环并发子线程个数%d' %thread_cnt)
        inc = 30
        logger.info('主进程进入等待时间,默认30秒') 
        if str3.getvalue()==str2.getvalue():
            inc = 600
            logger.info('当日过程已执行完成,主进程进入长等待,十分钟') 
        schedule.enter(inc, 0, p_judge_run, ()) 
    except Exception,e:
        logger.error(e)       
    
def timing_exe(time_inc=5): 
    # enter用来安排某事件的发生时间，从现在起第n秒开始启动   
    schedule.enter(time_inc, 0, p_judge_run, ())  
    logger.info('主进程开始: 5s') 
    # 持续运行，直到计划时间队列变成空为止   
    schedule.run()  
    
if __name__=='__main__':
    logger = init_logger()   
    logger.info('初始化调度任务')
    schedule = sched.scheduler(time.time, time.sleep)  
    timing_exe()
    
            
