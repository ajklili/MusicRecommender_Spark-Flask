import csv
import sys
csv.field_size_limit(sys.maxsize)

ratings={}
user=-1
with open('data_id.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        if row[0]!=user:
            user=row[0]
            ratings[user]=float(row[3])

v=open('data_ratings.csv','wb')
w=csv.writer(v,delimiter=',')
with open('data_id.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        w.writerow([row[0],row[1],row[2],"{:.2f}".format(10*float(row[3])/ratings.get(row[0]))])
