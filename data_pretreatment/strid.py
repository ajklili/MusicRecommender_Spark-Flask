import csv
import sys
csv.field_size_limit(sys.maxsize)

column1=[]
column2=[]
with open('data.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        column1.append(row[0])
        column2.append(row[1])

uid={}
for u in column1:
    uid.setdefault(u,len(uid))

sid={}
for s in column2:
    sid.setdefault(s,len(sid))

v=open('dataid.csv','wb')
w=csv.writer(v,delimiter=',')
with open('data.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        w.writerow([uid.get(row[0]),sid.get(row[1]),row[2],row[3]])
