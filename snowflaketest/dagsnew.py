import logging
import sqlite3

import pandas as pd

con = sqlite3.connect(r"C:\Users\Nayan Nandakumar\PycharmProjects\snowflakes\snowflakes\db.sqlite3")
print(con)

datemt = "currentdate"
df = pd.read_sql_query("SELECT * from student where CREATED_AT  = date() ", con , )
logging.basicConfig(level=logging.DEBUG,filename='testlog.log',filemode='w')
logging.debug("uses .02")
logging.debug("credit used by SELECT * FROM TEST_TABLE is .02")
df.to_excel('news.xlsx', engine='xlsxwriter', index=False)
exc = pd.read_excel("news.xlsx")