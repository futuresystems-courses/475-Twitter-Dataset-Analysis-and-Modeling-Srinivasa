
#Import Packages
import pymongo
import csv
from pymongo import MongoClient
import json
from datetime import datetime
rows = []
srow = {}
client = MongoClient('localhost', 27017)
db = client.twitter

#1)  All twitter users
# add all users. each user as one document.
########################
########################
with open('/users/srao/downloads/indiana/users.txt','r') as tw:
    reader=csv.reader(tw,delimiter='\t')
    for row in reader:
        try:
            srow =  json.loads('{ "_id" : ' + row[0]  + ', "name" : "' + row[1].replace('"','').replace('\'','') +'" , "friendcount" : ' + row[2]  + ', "followercount" : ' + row[3] + ', "statuscount" : ' + row[4]  + ', "favoritecount" : ' + row[5]  + ', "joineddate" : "' + row[6] +'", "userlocation" : "' + row[7].replace('"','').replace('\\','')  + '"' + '}')
            rows.append(srow)
            result = db.twitteruser.insert_one(srow)
        except ValueError:
            print (row[1])
            print(row[7])




#2)
#Adding followers from networks file to twitter users as sub document.
###############
##############
with open('/users/srao/downloads/indiana/network.txt','r') as tw:
    reader=csv.reader(tw,delimiter='\t')
    userid = 0
    f =[]
    for row in reader:
        if userid == row[0]:
            f.append(row[1])
        else:
            srow1 =  json.loads('{ "_id" : ' + str(userid) + '}')
            srow2 =  json.loads('{ "$set" : {"followers" : [' + ','.join(f) + ']}}')
            if f != []:
                db.twitteruser.update(srow1,srow2)
                f= []
            f.append(row[1])
            userid = row[0]
        #except ValueError:
            print('Error')
            print(f)
  #3)Model 1 
  ##Add all Tweets to users 
  #########################
  ##########################     
recordstart = 0
jdoc = ''
for user in os.listdir('/users/srao/downloads/indiana/tweets/'):
    filename = '/users/srao/downloads/indiana/tweets/' + user 
    with open(filename,'r') as tw:
        for line in tw:
            if recordstart == 0 and line == '***\n':
                recordstart = 1
                jdoc = jdoc + '{'
            elif recordstart == 1 and line == '***\n':
                recordstart = 0
                jdoc = jdoc[:-1]
                jdoc = jdoc + '}'
                try:
                    srow1 =  json.loads('{ "_id" : ' + str(user) + '}')
                    #srow2 = json_decode('{ "$push" : {"tweets" : ' + jdoc + '}}')
                    srow2 =  '{ "$push" : {"tweets" : ' + jdoc + '}}'
                    try:
                        srow3 = json.loads(srow2,strict=False)
                    except:
                         srow3 =  ''
                    if srow3 != '':     
                        result = db.twitteruser.update_one(srow1,srow3)
                    #print(jdoc)
                    jdoc = ''
                    print('Success')
                except Exception as e:
                    print e       
            elif line != '***\n':    
                keyvalue = line.split(':',1)
                if "Text" in keyvalue[0]:
                    srow =   '"' + keyvalue[0].replace('\\','').replace('\'','').replace('"','').replace('""','').replace('.','').replace('$','').replace('\n','').replace('{','').replace('}','').strip().replace('\t','').replace(chr(99),'').replace(chr(97),'').replace(chr(92),'').replace(chr(52),'').replace("\'",'') + '"' + ' : "' + keyvalue[-1].replace('\n','').replace('\\','').replace('\'','').replace('"','').replace('""','').replace('.','').replace('$','').replace('\n','').replace('{','').replace('}','').strip().replace('\t','').replace(chr(99),'').replace(chr(97),'').replace(chr(92),'').replace(chr(52),'').replace(':','').replace("\'",'') + '",'
                    jdoc = jdoc + srow

################################