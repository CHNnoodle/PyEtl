# coding:utf8

import threading
import time

def text(x,i):
	global xyz
	time.sleep(i)
	print x*i
	xyz.append(x*i)

if __name__ == '__main__':
	print 'test'
	global xyz
	xyz = []
	while True :
		for i in range(3):
			t = threading.Thread(target=text, args=('a',i))
			t.start()

		time.sleep(3)
		print xyz 


