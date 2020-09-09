# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 11:50:19 2018

@author: wushansj
"""

"""
[MySQL]
Description     = ODBC for MySQL
Driver          = /usr/lib/libmyodbc5.so
Setup           = /usr/lib/libodbcmyS.so
Driver64        = /usr/lib64/libmyodbc5.so
Setup64         = /usr/lib64/libodbcmyS.so
FileUsage       = 1


pyodbc.pooling = False
pyodbc.autocommit = True
con = pyodbc.connect("DSN=ODBC for MySQL", autocommit=True)
#con = pyodbc.connect("DSN=HIVEDSN", autocommit=True)
"""


"""
import mysql.connector
from mysql.connector import Error


connection = mysql.connector.connect(
        host='10.124.2.27',          # 主機名稱
        database='analysisdb', # 資料庫名稱
        user='tableau_tw',        # 帳號
        password='pchome1234')  # 密碼

cursor = connection.cursor()
cursor.execute("select count(days)  FROM youtube  where days like '2020-04-10'")
    
for date in cursor:
     print(date)

    
""" 






import paramiko


PORT = 22
HOST = "linxpd-itbigd01"
USER = "oracle"
PSWD = "cont3xt"

transport  = paramiko.Transport((HOST,PORT))
transport.connect(username = USER, password = PSWD)
sftp = paramiko.SFTPClient.from_transport(transport)
#your data 
localpath='/home/oracle/test.sh'
#linux server directory and assign data name
remotepath='/home/oracle/sq_scripts/test.sh'
#UPLOAD
sftp.put(localpath,remotepath)
#DOWNLOAD
#sftp.get(remotepath, localpath)
transport.close()


#localpath='D://rate/daily/airflowtest/exchangerate_generate_ctl_cmd.py'
#remotepath='/home/oracle/rate/exchangerate_generate_ctl_cmd.py'
#localpath='D://rate/daily/airflowtest/dag_rate.py'
#remotepath='/home/oracle/anaconda3/envs/airflow-tutorials/dags/dag_rate.py'

"""測試server
#10.124.2.55 (linxtd-mys01)/ oracle / cont3xt (CentOS Linux release 7.5.1804 (Core))
#10.124.2.151 (linxdd-twany00) / oracle / abc123  (Red Hat Enterprise Linux Server release 7.4 (Maipo))
"""
"""正式server
#10.124.3.22 (linxpa-myp01)/ ad account (CentOS Linux release 7.5.1804 (Core))

"""



"""MARIADB
#10.124.2.70 / LINXTD-MARIAU01(測試環境) /  root / abc123  => DB name analysisdb (tableau_tw / pchome1234)(shan/shan)
#10.124.2.27 / LINXPD-MARIAU01(正式環境) / oracle / cont3xt => DB name analysisdb (tableau_tw / pchome1234)
"""

"""ORACLE
TOPEKA seleapps garm1n, GARMIN garm1n
CLNE_ORBDEV GARMIN garm1n
ORANY twbidw 密碼忘了
CLNE_ORBTST GARMIN garm1n
"""

 

"""
MONGODB
#test: LINXTD-ITBIGD02:27017  admin admin #tableau的port 2207 
#prod: LINXPD-ITBIGN01:27017 帳密admin/admin123  #tableau的port 2207 
DBAA        
"""


"""
TABLEAU
test
linwta-birpt01.ad.garmin.com
DBA GARM1N

prod
jhowpa-birpt00.ad.garmin.com 
AP garm1n

"""

#先按d 再按 G 可全刪vi
"""
linxpa-dremio
31010
garmin
garmin123
"""



"""
linxta-elk01:5601
elastic elastic
"""

"""

Elasticsearch("slack:slack_user@linxta-elk01:9200/") ---CRAWLER LOG
Elasticsearch("http://10.128.1.165:9200/")  ---VPN

"""


"""
Hive
linxpd-itbigd03 
tw_tableau
"""