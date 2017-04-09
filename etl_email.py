# coding:utf-8

import smtplib
from email.header import Header
from email.mime.text import MIMEText
import etl_oracle
import etl_time
import cx_Oracle
import os
import commands

inacctday = etl_time.get_time(1)

procname = 'xijia.pkg_etl_model.p_day_execute_situation'
str = '1'

try:
    db_conn = etl_oracle.get_conn()
    cursor = db_conn.cursor()
    num1 = cursor.var(cx_Oracle.NUMBER)
    num2 = cursor.var(cx_Oracle.NUMBER)
    str = cursor.var(cx_Oracle.STRING)
    cursor.callproc(procname, [inacctday, num1, num2, str])
    print '数据库同步情况：'
    print str.getvalue()
    cursor.close()
    db_conn.close()
except Exception, e:
    print e

if str != '1':
    oscmd = 'ps -ef | grep python | grep etl_judge.py | grep -v grep'
    (res_status, res_output) = commands.getstatusoutput(oscmd)
    if res_output:
        res_str = 'etl调度状态正常。\n'
        print res_str
    else:
        res_str = 'etl调度进程已停止，需要检查！\n'
        print res_str

    # 第三方 SMTP 服务
    mail_host = "smtp.126.com"  # 设置服务器
    mail_user = "wanggang_123@126.com"  # 用户名
    mail_pass = "njust789"  # 口令

    sender = 'wanggang_123@126.com'
    receivers = 'wanggang@xjgreat.com'  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    cc = 'chensongqiao@xjgreat.com,1049484426@qq.com'
    message_str = res_str + str.getvalue()

    # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
    message = MIMEText(message_str, 'plain', 'utf-8')
    message['From'] = sender
    message['To'] = receivers
    message['Cc'] = cc

    subject = '南理工数据调度情况'
    message['Subject'] = Header(subject, 'utf-8')

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, [receivers, cc], message.as_string())
        smtpObj.close()
        print "邮件发送成功"
    except smtplib.SMTPException:
        print "Error: 无法发送邮件"
else:
    print '数据库连接失败，邮件发送失败'
