# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 18:39:07 2021

@author: 26345
"""

"""
Oracle SQL 文件批量插入到 MySQL 数据库

1、将SQL文件放到 linux 系统下；
2、使用 sed 命令对函数替换 sed -e 's/to_date\(/str_to_date(/g' -e 's/dd-mm-yyyy/\%d-\%m-\%Y/g' -e 's/hh24:mi:ss/\%H:\%i:\%s/g' sql文件名 > 目标文件名
3、使用 python 按行读取，执行sql
4、1000条执行时间 22 秒
"""
import mysql.connector
import time

mydb = mysql.connector.connect(
    host = "***.***.***.***",
    user="root",
    passwd = "*****"
    )

file = open ("c:\\sql\\tmp.sql",'r')
#newFile = open ("c:\\sql\TB_ZSSP_STK_HIST.new.sql",'w')

cursor = mydb.cursor()

cursor.execute("use KMMS")


i=0
cnt = 0
n = 0
tmpStr = ""
batchstr = ""

start_time = time.time()

while True:
    line = file.readline() 
   
    i+=1
    if not line:
        break
    if line == "":
        break
    elif i == 1:
        tmpStr = line
    elif i == 2:
        tmpStr = tmpStr + line
    elif i%3  == 1 and i>3:
        #line = line.replace('to_date(','str_to_date(').replace('dd-mm-yyyy','%d-%m-%Y').replace('dd-mm-yyyy hh24:mi:ss','%d-%m-%Y %H:%i:%s')
        tmpStr = line
    elif i%3 == 2 and i > 2:
        #line = line.replace('to_date(','str_to_date(').replace('dd-mm-yyyy','%d-%m-%Y').replace('dd-mm-yyyy hh24:mi:ss','%d-%m-%Y %H:%i:%s')
        tmpStr = tmpStr + line
    else:    
        #print("sql " + tmpStr)
        #newFile.write(tmpStr+"\n")
        #print("execute" + tmpStr + "\n")
        #n += 1
        cursor.execute(tmpStr)
        tmpStr = ""
        """
        batchstr = batchstr + tmpStr
        if n == 20:            
            cursor.executemany(batchstr)
            mydb.commit()
            n = 0
            batchstr = ""
            """
            
        cnt += 1
        if cnt % 10000 == 0 and cnt > 10000:
            mydb.commit()
            end_time = time.time()        
            print("cost" , end_time - start_time)
            start_time = time.time()
        elif cnt == 10000:
            mydb.commit()
            end_time = time.time()
            print("cost" , end_time - start_time)
            start_time = time.time()
            
end_time = time.time()
print("cost" , end_time - start_time)

cursor.execute(tmpStr)        
mydb.commit()
        
mydb.close()   
file.close()
