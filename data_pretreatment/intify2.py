import csv
import sys
csv.field_size_limit(sys.maxsize)

v=open('s.csv','wb')
w=csv.writer(v,delimiter=',')
with open('songs.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
	if row[0]!='' and row[1]!='':
            w.writerow([int(row[0]),row[1]])
