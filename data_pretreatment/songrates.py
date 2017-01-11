import csv
import sys
csv.field_size_limit(sys.maxsize)

sid=[[0,0] for i in range(160000)]
with open('ratings.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        index=int(row[1])
        (sid[index])[0]=index
        (sid[index])[1]+=1
        
sid=sorted(sid,key=lambda s:s[1],reverse=True)
v=open('s.csv','wb')
w=csv.writer(v,delimiter=',')
for key in sid:
    w.writerow(key)
