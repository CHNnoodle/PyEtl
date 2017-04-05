# coding:utf8

import datetime

def get_time(daynum,daytpye=1): 
    try:
        nowdaytime = datetime.datetime.today()
        indaytime = datetime.datetime.today()-datetime.timedelta(days=daynum) #默认昨天
        inacctday = indaytime.strftime("%Y%m%d")
        nowacctday = nowdaytime.strftime("%Y%m%d-%H:%M")
        nowhour = nowdaytime.strftime("%H")
        if daytpye == 1 :
            print '当前时间-- %s'%nowacctday
            print '同步数据日期-- %s'%inacctday
            return inacctday
        else :
            return nowhour
    except Exception,e:
        print e
        return e

if __name__=='__main__':
    print 'etl_time'
    x = get_time(1,2)

