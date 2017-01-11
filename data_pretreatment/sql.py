import MySQLdb
import csv

db=MySQLdb.connect("localhost","root","root","mr")
cursor=db.cursor()
with open('songs.csv','rb') as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        aid=int(row[0])
        name=row[1]
        name=name.replace('"','\\"')
        print aid
        sql='insert into artists (aid,name) values ("%d","%s")' % (aid,name)
        cursor.execute(sql)
db.commit()
db.close()
