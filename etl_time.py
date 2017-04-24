# coding:utf8

import datetime


def get_time(daynum, daytpye=1):
    try:
        nowdaytime = datetime.datetime.today()
        indaytime = datetime.datetime.today() - datetime.timedelta(days=daynum)  # 默认昨天
        inacctday = indaytime.strftime("%Y%m%d")
        nowhour = nowdaytime.strftime("%H")
        if daytpye == 1:
            return inacctday
        else:
            return nowhour
    except Exception, e:
        print e
        raise Exception('fail', e)

if __name__ == '__main__':
    x = get_time(1)
    print x

    y = get_time(1, 2)
    print y
