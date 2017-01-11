import csv
import sys
csv.field_size_limit(sys.maxsize)

sid={}
with open('songs.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        sid.update({int(row[0]):row[1]})

v=open('s.csv','wb')
w=csv.writer(v,delimiter=',')
for key in sid:
    w.writerow([key,sid.get(key)])
