##############
#### MODEL 2
####Keep all tweets in separate table. 
#one tweet as one document.
#which will have user.

import pymongo
import csv
from pymongo import MongoClient
import json
from datetime import datetime
import os
rows = []
srow = {}
client = MongoClient('localhost', 27017)
db = client.twitter

recordstart = 0
addrecord = 0
jdocfinal =''
i = 0
jdoc = {}
for filename in os.listdir('/users/srao/downloads/indiana/tweets/'):
    userid = filename
    filename = '/users/srao/downloads/indiana/tweets/' + filename 
    with open(filename,'r') as tw:
        for line in tw:
            if recordstart == 0 and line == '***\n':
                recordstart = 1
                jdoc = '{ "userid" : ' + userid + ','
            elif recordstart == 1 and line == '***\n':
                recordstart = 0
                jdoc = jdoc[:-1]
                jdoc = jdoc + '}'
                try:
                    #print (jdoc)
                    db.tweets.insert_one(json.loads(jdoc))
                except ValueError:
                    i =+ 1
                    print(jdoc)
            elif line != '***\n':    
                keyvalue = line.split(':',1)
                if keyvalue[0].replace('\\','').replace('\'','').replace('"','').replace('""','').replace('.','').replace('$','').replace('\n','').replace('{','').replace('}','').strip().replace('\t',''):
                    srow =   '"' + keyvalue[0].replace('\\','').replace('\'','').replace('"','').replace('""','').replace('.','').replace('$','').replace('\n','').replace('{','').replace('}','').strip().replace('\t','') + '"' + ' : "' + keyvalue[-1].replace('\n','').replace('\\','').replace('\'','').replace('"','').replace('""','').replace('.','').replace('$','').replace('\n','').replace('{','').replace('}','').strip().replace('\t','') + '",'
                    jdoc = jdoc + srow


            
        
            