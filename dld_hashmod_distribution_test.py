#!/bin/env python
# coding: utf-8

import sys
import os
import M6.Common.Default as Default
from M6.Common.DB import Backend
import API.M6 as M6

SYS_TABLE_INFO = Default.M6_MASTER_DATA_DIR + "/SYS_TABLE_INFO.DAT"

# API로 쿼리 수행
def m6_execute(query):
    conn = M6.Connection('127.0.0.1:5050', 'test', 'test')
    c = conn.Cursor()
    c.SetFieldSep('|^|')
    c.SetRecordSep('|^-^|')
    print
    print c.Execute2(query)
    c.Close()
    conn.close()

# 테이블 생성
def create_table(db_dot_table):
    query = '''
    CREATE TABLE %s (
       k         TEXT,
       p         TEXT,
       a         TEXT
    )
    datascope       LOCAL
    ramexpire       30
    diskexpire      34200
    partitionkey    k
    partitiondate   p
    partitionrange  10
    ;
    ''' % db_dot_table
    m6_execute(query)

# 데이터 입력
def insert_data(db_dot_table, key, partition, value):
    query = "INSERT INTO %s (k, p, a) VALUES ('%s', '%s', '%s');" % (db_dot_table, key, partition,  value)
    m6_execute(query)

# DELETE TABLE
def delete_table(db_dot_table):
    query = "DELETE FROM %s;" % db_dot_table
    m6_execute(query)

# 초기화 DROP TABLE
def drop_table(db_dot_table):
    query = "DROP TABLE %s;" % db_dot_table
    m6_execute(query)

# 마스터 노드에서 파일 위치 메타데이터가 저장된 디렉터리 조회
def check_dld_dir(table_id):
    file_path = "%s/SYS_TABLE_LOCATION/%s" % (Default.M6_MASTER_DATA_DIR, table_id)
    print
    print "## dld dir path: ", file_path
    is_dir = os.path.isdir(file_path)
    print "## is dir : ", is_dir 
    if is_dir:
        print "## list  dir: ", os.listdir(file_path)

# DLD 파일에 직접 쿼리를 수행하는 함수
def execute_query(file_list, query):
    try:
        backend = Backend(file_list)
        cursor = backend.GetConnection().cursor()
        cursor.execute(query)
        data = cursor.fetchall()
    except Exception,e:
        print e
        return [["ERR: " + str(e)]]
    finally:
        try: cursor.close()
        except: pass
        try: backend.Disconnect()
        except: pass
    return data

# DLD 파일에 직접 select 쿼리 수행
def select_dld(file_path):
    file_list = [file_path]
    query = "select * from sys_table_location"
    data = execute_query(file_list, query)
    return data

# 테이블 아이디를 구하는 함수 
def get_table_id(db_dot_table):
    tmp_list = db_dot_table.upper().split(".")
    db_name = tmp_list[0]
    table_name = tmp_list[1]
    query = "SELECT TABLE_NAME FROM SYS_TABLE_INFO WHERE TABLE_REALNAME = '%s' AND DB_REALNAME = '%s'" % (table_name, db_name)
    query_result = execute_query([SYS_TABLE_INFO],query)
    if len(query_result) == 0:
        query_result = [["ERR: there is no table name - %s" % table_name]]
    return query_result[0][0]

def select_total_hash_mod(table_id):
    print "$$$"
    for i in range(Default.HASH_MOD_VALUE):
        dld_file_path = "%s/SYS_TABLE_LOCATION/%s/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_id, i)
        data = select_dld(dld_file_path)
        print " %d.DAT " % i
        for item in data:
            print " - ", item
    print "$$$"


def Main():
    db_dot_table = "test.dld_test_table"
    key = "k"
    partition = "20201212000000" 
    value = "1"
    
    print "## create table : %s" % db_dot_table
    create_table(db_dot_table)
    table_id = get_table_id(db_dot_table)
    print "## table_id : %s" % table_id

    for i in  range(5):
        print
        print "*======================*"
        
        key_loop = key + str(i)
        print "## insert a row of data / key : %s" % key_loop
        insert_data(db_dot_table, key_loop, partition, value)
        check_dld_dir(table_id)
        hash_value = hash(table_id + key_loop + partition) % Default.HASH_MOD_VALUE
        print "## hash_value : hash('table_id(%s)' + 'key'(%s) + 'partition'(%s)) %% Default.HASH_MOD_VALUE(%d) ==> %d" % (table_id, 
        key_loop, partition, Default.HASH_MOD_VALUE, hash_value)
        
        dld_file_path = "%s/SYS_TABLE_LOCATION/%s/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_id, hash_value)
        print "## dld file path : ", dld_file_path
        data = select_dld(dld_file_path)
        print "## dld file path data : "
        for item in data:
            print item
        print "========================"
        print

    print "## select total hash_mod"
    select_total_hash_mod(table_id)
    
    print 
    print "## delete table"
    delete_table(db_dot_table)

    print "## select total hash_mod"
    select_total_hash_mod(table_id)
    
    print "## drop table"
    drop_table(db_dot_table)
    exit(0)
    

if __name__ == "__main__":
    Main()
