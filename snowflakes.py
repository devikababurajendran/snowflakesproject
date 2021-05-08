from django.test import TestCase
import sqlite3
import pandas as pd
import snowflake as sn
import time



import xlsxwriter
from snowflake.connector.pandas_tools import write_pandas
import xlsxwriter
import logging
from django.test import TestCase

# Create your tests here.
username = 'nayannandakumar'
password = 'Idontknow@1'
account = 'vd04846.ap-south-1'
warehouse = 'COMPUTE_WH'
database = 'TESTDB'
ctx = sn.connector.connect(
    user=username,
    password=password,
    account=account,
    # warehouse=warehouse,
    database=database,
    schema='PUBLIC'
)
def  name():
    print('hello')


con = sqlite3.connect(r"C:\Users\Nayan Nandakumar\PycharmProjects\snowflakes\snowflakes\db.sqlite3")
print(con)
df = pd.read_sql_query("SELECT * from student", con)
logging.basicConfig(level=logging.DEBUG,filename='testlog.log',filemode='w')
logging.debug("uses .02")
logging.debug("credit used by SELECT * FROM TEST_TABLE is .02")
df.to_excel('test.xlsx', engine='xlsxwriter', index=False)
exc = pd.read_excel("test.xlsx")
cs = ctx.cursor()
write_pandas(ctx, exc, 'STUDENT')
# print(df)
cs.execute("SELECT current_version()")
one = cs.fetchone()
num = int(input("enter the number:"))
if num == 3:
    cs.execute("USE WAREHOUSE MEDIUMHOUSE")
    cs.execute("select * from x where T >= dateadd(month, -3, current_date)")
    df=cs.fetch_pandas_all()
    df.info()
    print(df.to_string())
elif num == 2:
    df = pd.read_sql_query("SELECT * from student", con)
    print(df)
else:
    print("wrong month")
#
# print(one[0])

test=cs.execute(" SELECT * FROM TEST_TABLE")
logging.debug("dfasfa %s " %test)

cs.close()
ctx.close()
