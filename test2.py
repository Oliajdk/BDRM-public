import psycopg2

def prepared():
    t1=Predicat("titreouvrage","=","'affinites des donnees'")
    t2=Predicat("maisondedition","=","'ME1'")
    t3=Predicat("maisondedition","=","'ME2'")
    p0=["codouvrage","titreouvrage","domaineprincipal","maisondedition"]
    p1=["domaineprincipal"]
    p2=["titreouvrage","domaineprincipal"]
    p3=["codouvrage"]
    g=Generateur(t1,t2,t3)
    g.ajouterVert(p0,p1,p2,p3)
    g.prepareHorizontale() #préparer minterms
    g.printResult()
    g.ajouterVert(p0,p1,p2,p3)
    g.prepareVerticale()
    return g

def connect():

    try:
        conn = psycopg2.connect("dbname=bdd user=postgres password=root")
        cursor = conn.cursor()
        postgreSQL_select_Query = "select * from ouvrage"

        cursor.execute(postgreSQL_select_Query)
        print("Select * FROM ouvrage")
        ouvrage_records = cursor.fetchall()
        

        print("Print each row and it's columns values")
        for row in ouvrage_records:
            print("codouvrage = ", row[0], )
            print("titreouvrage = ", row[1])
            print("domaineprincipal  = ", row[2])
            print("maisondedition = ", row[3],"\n" )

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

class Predicat:
        def __init__(self,col,op,val):
            self.op=op    
            self.col=col
            self.val=val
        def case(self,argument):
            switcher = {
                "<": ">=",
                ">": "<=",
                "<=": ">",
                ">=":"<",
                "=" :"!=",
                "!=":"=",
            } 
            return switcher.get(argument,"nothing")
        def negation(self):
            return Predicat(self.col,self.case(self.op),self.val)    
        def __str__(self):
            return "{} {} {}".format(self.col,self.op,self.val)    
            
class Generateur:
    def __init__(self,*args):
        self.liste=[]
        self.hori=[]
        self.fragsH=[]
        self.fragsV=[]
        self.fragsM=[]
        self.vert=[]
        self.fragmentsV=[]
        self.fragmentsH=[]
        for arg in args:
            
            self.liste.append(arg)
            self.liste.append(arg.negation())
    def ajouterVert(self,all,*args):
        for arg in args:
            self.vert.append(arg)
            self.vert.append(self.diff(all,arg))
    def prepareVerticale(self):
        for i in range(0,2):
            for j in range (2,4):
                for k in range (4,6):
                    #print(i,j,k)
                    intersection_set = set.intersection(set(self.vert[i]), set(self.vert[j]),set(self.vert[k]))
                    intersection_list = list(intersection_set)
                    #print(self.vert[i],self.vert[j],self.vert[k])
                    #print(intersection_list)
                    if (not intersection_list):
                        pass
                    else:
                        self.fragsV.append(intersection_list)
        for i in self.fragsV:
            if("codouvrage" in i):
                pass
            else:
                i.insert(0,"codouvrage")    
        for j in range(0,self.fragsV.__len__()):
            k=k+1
            temp=",".join(self.fragsV[j])        
            #self.fragmentsV.append("SELECT {} from ouvrage;".format(temp))
            self.fragmentsV.append(temp)        
                     
                    
    def diff(self,li1, li2):
        li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
        return li_dif    
    def print(self):
        for t in self.liste:
            print(t)
    def prepareHorizontale(self):
        for i in range(0,2):
            for j in range (2,4):
                for k in range (4,6):
                    #print(self.list[i]," ∧ ",self.list[j]," ∧ ",self.list[k])
                    self.hori.append(self.liste[i])
                    self.hori.append(self.liste[j])
                    self.hori.append(self.liste[k])      
    def printHorizontale(self):
        for i in range(0,24,3):
            print(self.hori[i]," ∧ ",self.hori[i+1]," ∧ ",self.hori[i+2]) 
    def printResult(self):
        for i in range(0,24,3):
            if (self.hori[i+1].col==self.hori[i+2].col and self.hori[i+1].op==self.hori[i+2].op and self.hori[i+1].val!=self.hori[i+2].val):
                pass
            else:

                temp=(self.hori[i].__str__()+" and "+self.hori[i+1].__str__()+" and "+self.hori[i+2].__str__())          
                self.fragsH.append(temp)
                #print(temp)
        for i in range(0,self.fragsH.__len__()):
            temp=self.fragsH[i]
            #self.fragmentsH.append("SELECT * from ouvrage WHERE {};".format(temp))
            self.fragmentsH.append(temp)
            

                


#g=prepared()
#print(g.fragmentsH)
#print(g.fragmentsV)
#

#connect()
""""
from itertools import combinations
ta=Predicat("titreouvrage","=","'data'")
tb=Predicat("maisondedition","=","'ME1'")
tc=Predicat("maisondedition","=","'ME2'")



L = [ta.__str__(),tb.__str__(),tc.__str__()]
test=[" and ".join(map(str, comb)) for comb in combinations(L, 3)]
print(test)
print(test.__len__())


print("-------")



print("-------")
for i in g.fragsH:
    print(i)
print("-------")
for i in g.fragsV:
    print(i)
print("-------")

for i in range(0,g.fragsH.__len__()):
    temp=g.fragsH[i]
    print("SELECT * from ouvrage\nWHERE {};".format(temp))
    print("\n")


k=0
for i in range(0,g.fragsH.__len__()):
    for j in range(0,g.fragsV.__len__()):
        print(k)
        k=k+1
        temp=g.fragsH[i]
        temp1=",".join(g.fragsV[j])
        
        
        print("SELECT {} from ouvrage\nWHERE {}".format(temp1,temp))
        print("\n")




g=Generateur(t1,t2,t3)
g.prepareHorizontale() #préparer minterms
g.printHorizontale()   #afficher minterms
print("------------------------------")
g.printResult()      #stocker afficher fragments horizontales

for i in range(0,g.fragsH.__len__()):

    print("SELECT from table\nWHERE {}".format(g.fragsH[i]))
"""








