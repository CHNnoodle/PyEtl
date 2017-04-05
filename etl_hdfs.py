# coding:utf-8

import os
import commands
import hdfs

#shcontext spool文件的内容
#filename 对应生成文件的前缀
#hdfs_path hdfs文件路径

def put_hdfs(shcontext,filename,hdfs_path):
	try:
		os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8' #设置环境变量，避免乱码
		sh_path = '/root/spoolsh/'+filename+'.sh'
		txt_local_path = '/root/spooldata/'+filename+'.txt'
		f = open(sh_path,'wb+')
		f.write(shcontext)
		f.close()
		print '写入pool脚本'
		oscmd1 = 'chmod +x '+sh_path
		(res_status1,res_output1) = commands.getstatusoutput(oscmd1)
		oscmd2 = 'sh '+sh_path
		(res_status2,res_output2) = commands.getstatusoutput(oscmd2)
		print 'spool数据到本地'
		txt_hdfs_path = hdfs_path+filename+'.txt'
		client = hdfs.Client("http://192.10.86.31:50070",root="/",timeout=100,session=False)
		client.delete(hdfs_path,recursive=True)
		client.upload(txt_hdfs_path,txt_local_path)
		print '上传数据到hdfs'
		oscmd3 = 'rm -f '+txt_local_path
		print '删除本地文件'
		res3 = os.system(oscmd3)
		return 'success'
	except Exception,e:
		print e
		return e

if __name__=='__main__':	
	print 'etl_hdfs'


