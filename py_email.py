# coding:utf-8

import smtplib
from email.header import Header
from email.mime.text import MIMEText

# 第三方 SMTP 服务
mail_host="smtp.126.com"  #设置服务器
mail_user="wanggang_123@126.com"    #用户名
mail_pass="njust789"   #口令 

sender = 'wanggang_123@126.com'
receivers = 'wanggang@xjgreat.com'  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

# 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
message = MIMEText('Python 邮件发送测试...', 'plain', 'utf-8')
message['From'] = 'wanggang_123@126.com'
message['To'] =  'wanggang@xjgreat.com'

subject = 'Python SMTP 邮件测试1212'
message['Subject'] = Header(subject, 'utf-8')

try:
    smtpObj = smtplib.SMTP() 
    smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
    smtpObj.login(mail_user,mail_pass)  
    smtpObj.sendmail(sender, receivers, message.as_string())
    print "邮件发送成功"
except smtplib.SMTPException:
    print "Error: 无法发送邮件"

