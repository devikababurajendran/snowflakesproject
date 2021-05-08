import logging
import sqlite3

import pandas as pd

con = sqlite3.connect("C:/Users/Nayan Nandakumar/PycharmProjects/snowflakes/snowflakes/db.sqlite3")
print(con)
df = pd.read_sql_query("SELECT * from student", con)
logging.basicConfig(level=logging.DEBUG, filename='testlog.log', filemode='w')
logging.debug("uses .02")
logging.debug("credit used by SELECT * FROM TEST_TABLE is .02")
df.to_excel('test.xlsx', engine='xlsxwriter', index=False)
exc = pd.read_excel("test.xlsx")