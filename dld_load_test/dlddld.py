#!/bin/env python
# coding: utf-8

import M6
import threading
import socket
import time
import M6.Common.Default as Default
import M6.Common.Protocol.Socket as Socket
from M6.Common.Protocol.DLDClient import Client
import datetime
import sys
from multiprocessing import Process

def find_node(table, key, partition):
	c = Client(Default.M6_MASTER_IP_ADDRESS, Default.PORT["DLD"])
	try:
		c.Connect()
	except:
		return []
	try:
		nodeList = c.FIND_NODE(table, key, partition)
	finally:
		try:
			c.Close()
		except:
			pass
	return nodeList

def find_node_end(table, key, partition):
	result = []
	c = Client(Default.M6_MASTER_IP_ADDRESS, Default.PORT["DLD"])
	try:
		c.Connect()
	except:
		return result
	
	result = c.FIND_NODE_END(table, key, partition)
	
	if not c.isSuccess:
		result = []
	
	try: c.Close()
	except: pass

	return result


def delete(table, key, partition, node_id):
	result = []
	c = Client(Default.M6_MASTER_IP_ADDRESS, Default.PORT["DLD"])
	try:
		c.Connect()
	except:
		return result
	
	result = c.DEL(table, key, partition, node_id)
	
	if not c.isSuccess:
		result = []
	
	try: c.Close()
	except: pass

	return result


def find_last_partition_per_key(table):
	result = []
	c = Client(Default.M6_MASTER_IP_ADDRESS, Default.PORT["DLD"])
	try:
		c.Connect()
	except:
		return result
	
	result = c.FIND_LAST_PARTITION_PER_KEY(table)
	
	if not c.isSuccess:
		result = []
	
	try: c.Close()
	except: pass

	return result

def dld_connect(table_name, table_key, table_partition):
	dld_sock = Socket.Socket()
	nodeList = []
	if not dld_sock.isConnect:
		dld_sock.Connect(Default.M6_MASTER_IP_ADDRESS, Default.PORT["DLD"])
		msg = dld_sock.Readline()
		print msg
		if msg[0] == "-":
			return msg

	DLD_WHERE_QUERY = "TABLE_KEY = '%s' and TABLE_PARTITION = '%s'" % (table_key, table_partition)
	#time.sleep(10)
	try:
		dld_sock.SendMessage("FIND_NODE_WHERE %s,%s\r\n" % (table_name, DLD_WHERE_QUERY))
		msg = dld_sock.Readline()
	except Exception, e:
		print str(e)
		return "-ERR %s\r\n" % str(e)

	while True:
		msg = dld_sock.Readline().strip()
		if msg[0] == "." :
			break
		nodeList.append(msg.split(","))
	dld_sock.close()
	print nodeList

	return msg

def load_func(table, partition, str_before_ten_days):

	for i in range(500):
		# 조회 
		if find_last_partition_per_key(table) == []:
			print "-err select empty"
		
		table_key = "k%s" % i
		
		#dld_lock.acquire()

		# 삽입
		find_node(table, table_key, partition)
			#print "-err empty"

		find_node_end(table, table_key, partition)
			#print "-err end empty"

                # 삭제
		for node_id in range(1,3):
			delete(table, table_key, str_before_ten_days, node_id)
			#	print "-err delete empty"
		
		#dld_lock.release()

# function test
if __name__ == '__main_':
	table_id = 'T26'
	table_key = 'k3'
	table_partition = '20110616000000'

	print find_node(table_id, table_key, table_partition)
	print find_node_end(table_id, table_key, table_partition)
	print delete(table_id, table_key, table_partition, 2)
	print find_last_partition_per_key(table_id)

if __name__ == '__main__':
	process_cnt = sys.argv[1]
	run_time = sys.argv[2]
	
	partition_start = '201801111000000'
	if len(sys.argv) <= 3:
		print "{process_cnt} {run_time} {partition_start}"
	elif len(sys.argv[3]) != 14:
		print "err argv length is not 14"
	else:
		partition_start = str(sys.argv[3])
	

	print "process_cnt: ", process_cnt
	print "partition_start: ", partition_start
	print "run_time : ", run_time

	# 백그라운드로 여러개 돌릴때 시작 파티션 다르게 수정하여 실행
	next_date_time = datetime.datetime.strptime(partition_start, '%Y%m%d%H%M%S')
	
	table_name = 'T26'
	
	count = 1
	gap = 0
	# process list
	p_list = []

	#dld_lock = threading.Lock()
	time.sleep(0.1)
	st_time = time.time()
	while True:
		if float(gap) > float(run_time):
			break	
		next_date_time = next_date_time + datetime.timedelta(seconds=60)
		str_next_date_time = datetime.datetime.strftime(next_date_time, '%Y%m%d%H%M%S')
		before_ten_days = next_date_time - datetime.timedelta(days=10)
		str_before_ten_days = datetime.datetime.strftime(before_ten_days, '%Y%m%d%H%M%S')
		#print str_next_date_time
		#print str_before_ten_days
		for i in range(int(process_cnt)):
			p_list.append(Process(target=load_func, args=(table_name, str_next_date_time, str_before_ten_days)))
		for p in p_list:
			p.start()
		for p in p_list:
			p.join()
		p_list = []
		ed_time = time.time()
		gap = ed_time - st_time
		count += 1
	
	time.sleep(0.1)
	print '======RUN TIME %s======' % run_time
	print 'total run time : ', ed_time - st_time
	print 'total cycle count : ',count
	print 'count per sec: ', (float(count * 500.0) * float(process_cnt)) / float(ed_time - st_time)
	print '======================='

	print
