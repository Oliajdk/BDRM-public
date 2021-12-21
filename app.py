import psycopg2
import pandas as pd
import csv
from test2 import *
import sys
from operator import itemgetter
import time
from flask import Flask,render_template,request,url_for,redirect


app = Flask(__name__)




DB_HOST1 = ""
DB_NAME1 = ""
DB_USER1 = ""
DB_PASS1 = ""

DB_HOST2 = ""
DB_NAME2 = ""
DB_USER2 = ""
DB_PASS2 = ""

DB_HOST = ""
DB_NAME = ""
DB_USER = ""
DB_PASS = ""

def conn1():
    conn = psycopg2.connect(
        host=DB_HOST1,
        database=DB_NAME1,
        user=DB_USER1,
        password=DB_PASS1,
        port="5432",
        sslmode='require')
    return conn

def conn2():
    conn = psycopg2.connect(
        host=DB_HOST2,
        database=DB_NAME2,
        user=DB_USER2,
        password=DB_PASS2,
        port="5432",
        sslmode='require')
    return conn
def connlocal():
    
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port="5432",
        sslmode='require')
    return conn

def insertlocalcsv():

    
    conn = connlocal()
    cursor = conn.cursor()
                                                
    

    with open('data.csv', 'r') as f:
        reader = csv.reader(f)
        next(f) # Skip the header row.
        cursor.copy_from(f,'ouvrage',sep=';')
    
    conn.commit()
    print("test2")
    
    conn.close()
        
    
    print("PostgreSQL connection is closed")
def reconstruire():


    c1=conn1()
    c2=conn2()
    cursor1=c1.cursor()
    cursor2=c2.cursor()
    cursor1.execute('''(SELECT o0.codouvrage,titreouvrage,domaineprincipal,maisondedition
            FROM ouvrage00 as o0,ouvrage01 as o1,ouvrage02 as o2,ouvrage03 as o3
            WHERE o0.codouvrage=o1.codouvrage
            and o0.codouvrage=o2.codouvrage
            and o0.codouvrage=o3.codouvrage
            UNION
            SELECT o0.codouvrage,titreouvrage,domaineprincipal,maisondedition
            FROM ouvrage20 as o0,ouvrage21 as o1,ouvrage22 as o2,ouvrage23 as o3
            WHERE o0.codouvrage=o1.codouvrage
            and o0.codouvrage=o2.codouvrage
            and o0.codouvrage=o3.codouvrage
            ORDER BY codouvrage)
            ''')
    cursor2.execute('''SELECT o1.codouvrage,titreouvrage,domaineprincipal,maisondedition
            FROM ouvrage10 as o0,ouvrage11 as o1,ouvrage12 as o2,ouvrage13 as o3
            WHERE o0.codouvrage=o1.codouvrage
            and o0.codouvrage=o2.codouvrage
            and o0.codouvrage=o3.codouvrage
            UNION
            SELECT o0.codouvrage,titreouvrage,domaineprincipal,maisondedition
            FROM ouvrage30 as o0,ouvrage31 as o1,ouvrage32 as o2,ouvrage33 as o3
            WHERE o0.codouvrage=o1.codouvrage
            and o0.codouvrage=o2.codouvrage
            and o0.codouvrage=o3.codouvrage
            ORDER BY codouvrage''')            
    records = cursor1.fetchall()
    records1 = cursor2.fetchall()
    c=tuple(set(records).union(records1))
    
    e=sorted(c,key=itemgetter(0))
    print("codouvrage | titreouvrage | domaineprincipal | maisondedition")
    for row in e:
        print(row[0],"  ",row[1],"   ",row[2],"   ",row[3],"\n" )
    c1.close()
    c2.close()    
    return e    
            
def fragmenterH():
    c1 = conn1() 
    c2 = conn2()
    c3 = connlocal()
    cursor1 = c1.cursor()
    cursor2 = c2.cursor()
    cursor3 = c3.cursor()       
    g=prepared()
    for i in range(0,4):
        if (i%2==0):

            for j in range(0,4):
                if len(g.fragmentsV[j])>16:
                    t=g.fragmentsV[j].split(",")
                    cursor1.execute('''CREATE TABLE IF NOT EXISTS ouvrage{}{}
                    (codouvrage integer not null PRIMARY KEY,
                    {} character varying(255));
                    '''.format(i,j,t[1]))
                    c1.commit()
                    cursor3.execute('''SELECT {} from ouvrage WHERE {} '''.format(g.fragmentsV[j],g.fragmentsH[i]))
                    records = cursor3.fetchall()
                    for arg in records:
        
                        insert_query = '''INSERT INTO ouvrage{}{} (codouvrage,{}) values ({},'{}')'''.format(i,j,t[1],arg[0],arg[1])
                        cursor1.execute(insert_query)
                        c1.commit()


                else:
                    cursor1.execute('''CREATE TABLE IF NOT EXISTS ouvrage{}{}
                    (codouvrage integer not null PRIMARY KEY);
                    '''.format(i,j))     
                    c1.commit()   
                    cursor3.execute('''SELECT {} from ouvrage WHERE {} '''.format(g.fragmentsV[j],g.fragmentsH[i]))
                    records = cursor3.fetchall()
                    for arg in records:
        
                        insert_query = '''INSERT INTO ouvrage{}{} (codouvrage) values ({})'''.format(i,j,arg[0])
                        cursor1.execute(insert_query)
                        c1.commit()                                                        
        else:
               
            for j in range(0,4):
                if len(g.fragmentsV[j])>16:
                    t=g.fragmentsV[j].split(",")
                    cursor2.execute('''CREATE TABLE IF NOT EXISTS ouvrage{}{}
                    (codouvrage integer not null PRIMARY KEY,
                    {} character varying(255));
                    '''.format(i,j,t[1]))
                    c2.commit()
                    cursor3.execute('''SELECT {} from ouvrage WHERE {} '''.format(g.fragmentsV[j],g.fragmentsH[i]))
                    records = cursor3.fetchall()
                    for arg in records:
        
                        insert_query = '''INSERT INTO ouvrage{}{} (codouvrage,{}) values ({},'{}')'''.format(i,j,t[1],arg[0],arg[1])
                        cursor2.execute(insert_query)
                        c2.commit()                    
                else:
                    cursor2.execute('''CREATE TABLE IF NOT EXISTS ouvrage{}{}
                    (codouvrage integer not null PRIMARY KEY);
                    '''.format(i,j))     
                    c2.commit()    
                    cursor3.execute('''SELECT {} from ouvrage WHERE {} '''.format(g.fragmentsV[j],g.fragmentsH[i]))
                    records = cursor3.fetchall()
                    for arg in records:
        
                        insert_query = '''INSERT INTO ouvrage{}{} (codouvrage) values ({})'''.format(i,j,arg[0])
                        cursor2.execute(insert_query)
                        c2.commit()     
    c1.close()
    c2.close()
    c3.close()                                            

def deleteall():

    
    c1 = conn1()
    c1.set_isolation_level(0)
    cursor1= c1.cursor()
    c2=conn2()
    c2.set_isolation_level(0)
    cursor2=c2.cursor
                                                
    
    try:
        c1 = conn1()
        c1.set_isolation_level(0)
        cursor1= c1.cursor()
        c2=conn2()
        c2.set_isolation_level(0)
        cursor2=c2.cursor()
    except:
        print ("Unable to connect to the database.")

        

    try:
        cursor1.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
        rows = cursor1.fetchall()
        for row in rows:
            print ("dropping table: ", row[1])
            cursor1.execute("drop table " + row[1] + " cascade")
            c1.commit()
            
        cursor1.close()
        c1.close()
        cursor2.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
        rows = cursor2.fetchall()
        for row in rows:
            print ("dropping table: ", row[1])
            cursor2.execute("drop table " + row[1] + " cascade")
            c2.commit()
        cursor2.close()
        c2.close()        
    except:
        print ("Error: ", sys.exc_info()[1])
    c1.close()
    c2.close()
        

   
#insertlocalcsv() 
#fragmenterH()
#reconstruire()
#deleteall()

@app.route('/')
def index():
    #e=reconstruire()
    return render_template('index.html')

@app.route('/reconstruction',methods=["GET"])
def reconstruction():
    c1=conn1()
    cursor=c1.cursor()
    cursor.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
    rows = cursor.fetchall()
    print(rows.__len__())
    print(rows)
    c1.close()
    if(rows.__len__()==0):
        return render_template('index1vide.html')
    else:
        e=reconstruire()
        return render_template('index1.html',data=e)

@app.route('/SomeFunction')
def SomeFunction():
    deleteall()
    return "Nothing "
@app.route('/SomeFunction1')
def SomeFunction1():
    fragmenterH()
    return "Nothing "    



@app.route('/start')
def start():
    #insertlocalcsv()
    #deleteall()
    start = time.time()
    deleteall()
    end = time.time()
    a=str(end - start)
    start = time.time()
    fragmenterH()
    #reconstruire()
    end = time.time()
    b=str(end - start)
    
    return a+"|"+b
    #return render_template("index.html")

if __name__ == "__main__":
    app.debug=True
    app.run()
        
