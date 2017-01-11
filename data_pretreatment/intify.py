import csv
import sys
csv.field_size_limit(sys.maxsize)

v=open('m.csv','wb')
w=csv.writer(v,delimiter=',')
with open('ratings.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
	if row[0]!='' and row[1]!='' and row[2]!='':
            w.writerow([int(row[0]),int(row[1]),float(row[2])])
