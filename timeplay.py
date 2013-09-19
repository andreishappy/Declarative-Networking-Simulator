import datetime


timestamp = datetime.datetime.strptime('2013-09-18 17:03:41.534','%Y-%m-%d %H:%M:%S.%f')
timestamp = int(float(timestamp.strftime('%s.%f'))*1000)
print timestamp
