import csv
import sys
csv.field_size_limit(sys.maxsize)

v=open('m.csv','wb')
w=csv.writer(v,delimiter=',')
with open('songs.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        w.writerow([int(row[0]),int(row[1]),float(row[2]),1])
