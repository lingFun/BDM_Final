# Ling Fang
# BDM_FinalChallenge_Fang.py

import sys
from pyspark import SpartContext

def extractBoroughCode(county):
    if county.upper() in ['MAN','MH','MN','NEWY','NEW Y','NY','MANHATTAN']:
        return '1'
    elif county.upper() in ['BRONX','BX', 'BRONX']:
        return '2'
    elif county.upper() in ['BK','K','KING','KINGS','BROOKLYN']:
        return '3'
    elif county.upper() in ['Q','QN','QNS','QU','QUEEN','QUEENS']:
        return '4'
    elif county.upper() in ['R','RICHMOND','STATEN ISLAND']:
        return '5'

#2015 0
#2016 1
#2017 2
#2018 3
#2019 4

def extractViolations2015(partId, records):
    if partId == 0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if(row[21] and row[23] and row[24]):
            if(row[23].isdigit()):
                if(int(row[23])%2 == 0):
                    yield (row[21],row[24],0,(int(row[23]),0)),(1,0)
                else:
                    yield (row[21],row[24],1,(int(row[23]),0)),(1,0)
            if('-' in row[23]):
                tu = row[23].split('-')
                if(len(tu) == 2 and tu[0].isdigit() and tu[1].isdigit()):
                    if(int(tu[1])%2 == 0):
                        yield (row[21],row[24],0,(int(tu[0]),int(tu[1]))),(1,0)
                    else:
                        yield (row[21],row[24],1,(int(tu[0]),int(tu[1]))),(1,0)

def extractViolations2016(partId, records):
    if partId == 0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if(row[21] and row[23] and row[24]):
            if(row[23].isdigit()):
                if(int(row[23])%2 == 0):
                    yield (row[21],row[24],0,(int(row[23]),0)),(1,1)
                else:
                    yield (row[21],row[24],1,(int(row[23]),0)),(1,1)
            if('-' in row[23]):
                tu = row[23].split('-')
                if(len(tu) == 2 and tu[0].isdigit() and tu[1].isdigit()):
                    if(int(tu[1])%2 == 0):
                        yield (row[21],row[24],0,(int(tu[0]),int(tu[1]))),(1,1)
                    else:
                        yield (row[21],row[24],1,(int(tu[0]),int(tu[1]))),(1,1)
                        
def extractViolations2017(partId, records):
    if partId == 0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if(row[21] and row[23] and row[24]):
            if(row[23].isdigit()):
                if(int(row[23])%2 == 0):
                    yield (row[21],row[24],0,(int(row[23]),0)),(1,2)
                else:
                    yield (row[21],row[24],1,(int(row[23]),0)),(1,2)
            if('-' in row[23]):
                tu = row[23].split('-')
                if(len(tu) == 2 and tu[0].isdigit() and tu[1].isdigit()):
                    if(int(tu[1])%2 == 0):
                        yield (row[21],row[24],0,(int(tu[0]),int(tu[1]))),(1,2)
                    else:
                        yield (row[21],row[24],1,(int(tu[0]),int(tu[1]))),(1,2)
                        
def extractViolations2018(partId, records):
    if partId == 0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if(row[21] and row[23] and row[24]):
            if(row[23].isdigit()):
                if(int(row[23])%2 == 0):
                    yield (row[21],row[24],0,(int(row[23]),0)),(1,3)
                else:
                    yield (row[21],row[24],1,(int(row[23]),0)),(1,3)
            if('-' in row[23]):
                tu = row[23].split('-')
                if(len(tu) == 2 and tu[0].isdigit() and tu[1].isdigit()):
                    if(int(tu[1])%2 == 0):
                        yield (row[21],row[24],0,(int(tu[0]),int(tu[1]))),(1,3)
                    else:
                        yield (row[21],row[24],1,(int(tu[0]),int(tu[1]))),(1,3)                        

def extractViolations2019(partId, records):
    if partId == 0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        if(row[21] and row[23] and row[24]):
            if(row[23].isdigit()):
                if(int(row[23])%2 == 0):
                    yield (row[21],row[24],0,(int(row[23]),0)),(1,4)
                else:
                    yield (row[21],row[24],1,(int(row[23]),0)),(1,4)
            if('-' in row[23]):
                tu = row[23].split('-')
                if(len(tu) == 2 and tu[0].isdigit() and tu[1].isdigit()):
                    if(int(tu[1])%2 == 0):
                        yield (row[21],row[24],0,(int(tu[0]),int(tu[1]))),(1,4)
                    else:
                        yield (row[21],row[24],1,(int(tu[0]),int(tu[1]))),(1,4)  

def extractCenterline(partId, records):
    if partId == 0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        #at least one of street name or street label are not empty
        if(row[10] or row[28]):
            #house number has -, set as tuple
            if('-' in row[2] and '-' in row[3] and '-' in row[4] and '-' in row[5] and row[13]):
                temp1 = row[2].split('-')
                l_low = tuple(map(int,temp1))
                temp2 = row[3].split('-')
                l_high = tuple(map(int,temp2))
                temp3 = row[4].split('-')
                r_low = tuple(map(int,temp3))
                temp4 = row[5].split('-')
                r_high = tuple(map(int,temp4))
                #street label and full street are same, both not empty
                if(row[28] == row[10] and row[28]):
                        yield (row[13],row[28],1,l_low),(row[0],'low')
                        yield (row[13],row[28],1,l_high),(row[0],'high')
                        yield (row[13],row[28],0,r_low),(row[0],'low')
                        yield (row[13],row[28],0,r_high),(row[0],'high')
                #street label and full street are diff
                elif row[28] and row[10]:
                        yield (row[13],row[28],1,l_low),(row[0],'low')
                        yield (row[13],row[28],1,l_high),(row[0],'high')
                        yield (row[13],row[10],1,l_low),(row[0],'low')
                        yield (row[13],row[10],1,l_high),(row[0],'high')
                        yield (row[13],row[28],0,r_low),(row[0],'low')
                        yield (row[13],row[28],0,r_high),(row[0],'high')
                        yield (row[13],row[10],0,r_low),(row[0],'low')
                        yield (row[13],row[10],0,r_high),(row[0],'high')
                #only full street, street label is empty 
                elif row[28]:
                        yield (row[13],row[28],1,l_low),(row[0],'low')
                        yield (row[13],row[28],1,l_high),(row[0],'high')
                        yield (row[13],row[28],0,r_low),(row[0],'low')
                        yield (row[13],row[28],0,r_high),(row[0],'high')
                #only street label, full street is empty
                elif row[10]:
                        yield (row[13],row[10],1,l_low),(row[0],'low')
                        yield (row[13],row[10],1,l_high),(row[0],'high')
                        yield (row[13],row[10],0,r_low),(row[0],'low')
                        yield (row[13],row[10],0,r_high),(row[0],'high')
            #house number is int, add 0 to set as tuple 
            elif row[2].isdigit() and row[3].isdigit() and row[4].isdigit() and row[5].isdigit():
                #street label and full street are same, both not empty
                if(row[28] == row[10] and row[28]):
                    #both left and right high are greater than 0
                    if(int(row[3]) > 0 and int(row[5]) > 0):
                        yield (row[13],row[28],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[28],1,(int(row[3]),0)),(row[0],'high')
                        yield (row[13],row[28],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[28],0,(int(row[5]),0)),(row[0],'high')
                    #only left high greater than 0
                    elif(int(row[3]) > 0):
                        yield (row[13],row[28],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[28],1,(int(row[3]),0)),(row[0],'high')
                    #only right high greater than0
                    elif(int(row[5]) > 0):
                        yield (row[13],row[28],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[28],0,(int(row[5]),0)),(row[0],'high')
                    #both low and high are not greater than 0
                    else:
                        yield ('0','',-1,(0,0)),row[0]
                #street label and full street are diff, both not empty
                elif row[28] and row[10]:
                    #both left and right high are greater than 0
                    if(int(row[3]) > 0 and int(row[5]) > 0):
                        yield (row[13],row[28],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[10],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[10],1,(int(row[3]),0)),(row[0],'high')
                        yield (row[13],row[28],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[28],0,(int(row[5]),0)),(row[0],'high')
                        yield (row[13],row[10],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[10],0,(int(row[5]),0)),(row[0],'high')
                    #only left high greater than 0
                    elif(int(row[3]) > 0):
                        yield (row[13],row[28],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[28],1,(int(row[3]),0)),(row[0],'high')
                        yield (row[13],row[10],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[10],1,(int(row[3]),0)),(row[0],'high')
                    #only right high greater than0
                    elif(int(row[5]) > 0):
                        yield (row[13],row[28],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[28],0,(int(row[5]),0)),(row[0],'high')
                        yield (row[13],row[10],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[10],0,(int(row[5]),0)),(row[0],'high')
                    #both low and high are not greater than 0
                    else:
                        yield ('0','',-1,(0,0)),row[0]
                #only full street, street label is empty 
                elif row[28]:
                    #both left and right high are greater than 0
                    if(int(row[3]) > 0 and int(row[5]) > 0):
                        yield (row[13],row[28],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[28],1,(int(row[3]),0)),(row[0],'high')
                        yield (row[13],row[28],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[28],0,(int(row[5]),0)),(row[0],'high')
                    #only left high greater than 0
                    elif(int(row[3]) > 0):
                        yield (row[13],row[28],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[28],1,(int(row[3]),0)),(row[0],'high')
                    #only right high greater than0
                    elif(int(row[5]) > 0):
                        yield (row[13],row[28],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[28],0,(int(row[5]),0)),(row[0],'high')
                    #both low and high are not greater than 0
                    else:
                        yield ('0','',-1,(0,0)),row[0]
                #only street label, full street is empty
                elif row[10]:
                    #both left and right high are greater than 0
                    if(int(row[3]) > 0 and int(row[5]) > 0):
                        yield (row[13],row[10],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[10],1,(int(row[3]),0)),(row[0],'high')
                        yield (row[13],row[10],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[10],0,(int(row[5]),0)),(row[0],'high')
                    #only left high greater than 0
                    elif(int(row[3]) > 0):
                        yield (row[13],row[10],1,(int(row[2]),0)),(row[0],'low')
                        yield (row[13],row[10],1,(int(row[3]),0)),(row[0],'high')
                    #only right high greater than0
                    elif(int(row[5]) > 0):
                        yield (row[13],row[28],0,(int(row[4]),0)),(row[0],'low')
                        yield (row[13],row[28],0,(int(row[5]),0)),(row[0],'high')
                    #both low and high are not greater than 0
                    else:
                        yield ('0','',-1,(0,0)),row[0]
            # left low and high are not empty, right low and high are empty
#             elif row[2].isdigit() and row[3].isdigit():
            #right low and high are not empty, left are empty
#             elif row[4].isdigit() and row[5].isdigit():
            #all four low and high are empty, or one in the pair are empty
            else:
                yield ('0','',-1,(0,0)),row[0]
        #physical id with both street name and street label are empty, set borough code = 0            
        else:
            yield ('0','',-1,(0,0)),row[0]


def getCount(_, records):
    #record each pid and its correctsponding count
    leftdic = {}
    rightdic = {}
    
    #if multi pid has same interval, keeping track all pid
    leftcur = [] 
    rightcur = []
    for row in records:
        bcode = row[0][0]
#         if(bcode == '0'):
#             yield (row[1],[0,0,0,0,0])
        pid = row[1][0]
        mode = row[0][2]
        yearIdx = row[1][1]
        if(mode == 0):
            if(pid == 1):
                for p in leftcur:
                    #violation occur in between low and high
                    try: 
                        leftdic[p][yearIdx] += 1
                    #if violation occur before low
                    except:
                        continue
            elif(row[1][1] == 'low'):
                leftdic[pid] = [0,0,0,0,0]
                leftcur.append(pid)
            elif(row[1][1] == 'high'):
                try:
                    leftcur.remove(pid)
                    yield (pid, leftdic[pid])
                #if high occur before low
                except:
                    yield (pid,[0,0,0,0,0])
        elif(mode == 1):
            if(pid == 1):
                for p in rightcur:
                    try: 
                        rightdic[p][yearIdx] += 1
                    except:
                        continue
            elif(row[1][1] == 'low'):
                rightdic[pid] = [0,0,0,0,0]
                rightcur.append(pid)
            elif(row[1][1] == 'high'):
                try:
                    yield (pid, rightdic[pid])
                    rightcur.remove(pid)
                except:
                    yield (pid,[0,0,0,0,0])


if __name__ == "__mian__":
    sc = SpartContext()
    
    Violations2015 = sc.textFile("../../data/share/bdm/nyc_parking_violation/2015.csv", use_unicode=False).cache()\
        .mapPartitionsWithIndex(extractViolations2015)\
        .map(lambda x:((extractBoroughCode(x[0][0]),x[0][1],x[0][2],x[0][3]),x[1]))

    Violations2016 = sc.textFile("../../data/share/bdm/nyc_parking_violation/2015.csv", use_unicode=False).cache()\
        .mapPartitionsWithIndex(extractViolations2016)\
        .map(lambda x:((extractBoroughCode(x[0][0]),x[0][1],x[0][2],x[0][3]),x[1]))

    Violations2017 = sc.textFile("../../data/share/bdm/nyc_parking_violation/2015.csv", use_unicode=False).cache()\
        .mapPartitionsWithIndex(extractViolations2017)\
        .map(lambda x:((extractBoroughCode(x[0][0]),x[0][1],x[0][2],x[0][3]),x[1]))

    Violations2018 = sc.textFile("../../data/share/bdm/nyc_parking_violation/2015.csv", use_unicode=False).cache()\
        .mapPartitionsWithIndex(extractViolations2018)\
        .map(lambda x:((extractBoroughCode(x[0][0]),x[0][1],x[0][2],x[0][3]),x[1]))

    Violations2019 = sc.textFile("../../data/share/bdm/nyc_parking_violation/2015.csv", use_unicode=False).cache()\
        .mapPartitionsWithIndex(extractViolations2019)\
        .map(lambda x:((extractBoroughCode(x[0][0]),x[0][1],x[0][2],x[0][3]),x[1]))

    Centerline = sc.textFile("../../data/share/bdm/nyc_cscl.csv", use_unicode=False).cache()\
        .mapPartitionsWithIndex(extractCenterline).distinct()

    Sorted = (Centerline+Violations2015+Violations2016+Violations2017+Violations2018+Violations2019).sortByKey()
    Result = Sorted.mapPartitionsWithIndex(getCount).sortByKey()\
        .saveAsTextFile("final_output1")
    #.groupByKey().mapValues(lambda x: (sum[x[0],sumx[1],sum[2],sum[3],sum[4]]))