# coding:utf8



x ='jiajian_authlog_v_acct_20170511-02-33_incre.txt'
print x[16:22]

# import redis

# r = redis.Redis(host='202.119.86.2', port=6379,
#                 db=0, password='xjcztest')

# r.lpush("list_name",2)
# print(r.get('name'))  # 获取
# import os
# import logging
# import cx_Oracle
# import hdfs


# def put_hdfs(dns, sql, filename, hdfs_path, local_path='/root/spooldata/'):
#     try:
#         count = ''
#         with cx_Oracle.connect(dns) as conn:
#             try:
#                 cursor = conn.cursor()
#                 cursor.execute(sql)
#                 res = cursor.fetchall()
#                 count = cursor.rowcount
#                 logging.info('%s:OJDBC导出%s行数据' % (filename, count))
#             except Exception, e:
#                 logging.error(e)
#                 raise Exception(e)

#             finally:
#                 cursor.close()

#         filepath=local_path + filename
#         with open(filepath, 'wb+') as f:
#             for row_data in res:
#                 f.write(''.join(row_data).encode('utf-8') + '\n')
#             logging.info('写入本地文件%s完成' % filepath)

#         try:
#             logging.info('上传数据到hdfs')
#             txt_hdfs_path=hdfs_path + filename
#             client=hdfs.Client("http://192.10.86.31:50070",
#                                  root="/", timeout=100, session=False)
#             client.delete(hdfs_path, recursive=True)
#             client.upload(txt_hdfs_path, filepath)
#             logging.info('upload数据完成')
#         except Exception, e:
#             logging.error(e)
#             raise Exception(e)

#         oscmd='rm -f ' + filepath
#         logging.info('删除本地文件')
#         res=os.system(oscmd)

#         (retcode, retinfo)=('success', '')
#     except Exception, e:
#         (retcode, retinfo)=('fail', e)
#         logging.error(e)
#         raise Exception('fail', e)

#     finally:
#         return count, retcode, retinfo


# if __name__ == '__main__':
#     sql='''select ACCT_DAY||'|*|'||USERNAME||'|*|'||FULLNAME||'|*|'||
#     CERTIFICATION_NO||'|*|'||MOBILE||'|*|'||STATUS||'|*|'||CREATETIME from STAGE_LOG.T_USERINFO WHERE ROWNUM<=10'''
#     dns=r'xijia/dba#789@NJUST'
#     (x, y, z)=put_hdfs(dns, sql, 'test', '1', '/Users/wanggang/Downloads/')
#     print x
#     print y


# import os
# import commands


# def putfile(infilename):
# 	acctday = infilename[6:14] + '/'
# 	newpath = '/data/url/'+acctday
# 	try:
# 	    os.chdir(newpath)
# 	except OSError:
# 		os.makedirs(newpath)

# 	oscmd = 'mv /data/url/' + infilename + ' '+ newpath
# 	print oscmd
# 	(status, output) = commands.getstatusoutput(oscmd)
# 	print status

# (status, output) = commands.getstatusoutput('ls /data/url')
# for infilename in output.split():
#     print infilename
#     putfile(infilename)
