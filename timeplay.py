import datetime


timestamp = datetime.datetime.strptime('2013-09-17 15:17:40.688','%Y-%m-%d %H:%M:%S.%f')
timestamp = int(float(timestamp.strftime('%s.%f'))*1000)
print timestamp
