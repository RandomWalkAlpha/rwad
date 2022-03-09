import datetime
import json

from rwad.data.collector.interface import DataCollector
import time
import pandas as pd

codes = ['SZ300631', 'SZ300867', 'SZ300692', 'SZ300187', 'SH603177', 'SZ002645', 'SZ000920', 'SH688565', 'SZ002266',
         'SZ002573', 'SZ301126', 'SH688621', 'SZ002173', 'SH603127', 'SH600896', 'SZ000504', 'SZ301033', 'SZ200028',
         'SZ002821', 'SH688246', 'SH600155', 'SH601066', 'SH601162', 'SZ000166', 'SZ000712', 'SH600999', 'SH600864',
         'SH600621', 'SZ002736', 'SH601901', 'SH600809', 'SZ000568', 'SH603589', 'SH600199', 'SH600559', 'SH600365',
         'SH600059', 'SZ002568', 'SZ000869', 'SH600543', 'SH688008', 'SH688233', 'SZ300706', 'SH688234', 'SH603160',
         'SH688123', 'SZ003026', 'SH688368', 'SH688711', 'SH688200', 'SZ300118', 'SH600438', 'SH601012', 'SZ002610',
         'SZ300393', 'SZ002459', 'SZ300274', 'SZ002506', 'SH688599', 'SH688680', 'SH601899', 'SH600766', 'SH600807',
         'SH600311', 'SZ000975', 'SZ002237', 'SZ000506', 'SZ002716', 'SH600547', 'SH600988']

collector = DataCollector()
start_date = '2020-01-25'
end_date = '2021-01-26'
'''
#df = collector.clean(collector.get_real_time_data("SH600519"))
df = collector.get_historical_data("SH600519", start_date, end_date)
#df = collector.get_minute_data("SH600519")
print(df.to_string())
'''

'''
collector.provider = 'N'
df = collector.get_batch_historical_data(codes, start_date, end_date)
for key, val in df.items():
    print(key)
    print(val)
'''

'''
while True:
    print(collector.get_real_time_data("SH600519"))
    time.sleep(5)
'''
print(time.time())