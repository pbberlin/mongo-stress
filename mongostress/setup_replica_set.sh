#!/bin/bash

cd ~/mongo/program/bin;
rm    start.js
touch start.js


echo "function init(){
conf = 
{
    _id : 'rs1',
     members : [
         {_id : 0, host : 'localhost:27001' },
         {_id : 1, host : 'localhost:27002' }
     ]
} ;

rs.initiate(conf) ;
// add the last rs member as an arbiter
rs.addArb('localhost:27003') ;
rs.status() ;
db.serverStatus()

}" >> start.js


# use local
# newsize = 2 * 1024 * 1024 * 1024
# "size is " +Math.round(newsize/1000000/1000) + "GB"
# db.oplog.rs.drop()
# db.runCommand( { create : "oplog.rs", capped : true, size : newsize } )



# make a test replica set

# if you need to start over try:
#  rm -rf data_rs1/*
#  (careful that deletes that everything under data_rs1/ recursively!)
#

# we expect nothing to be running.  you might have a mongo shell running which is ok...but 
# no mongod or mongos
echo "Already running mongo* processes (this is fyi, should be none probably):"
ps -A | grep mongo
echo


echo make / reset dirs
cd ~/mongo
mkdir ~/mongo/data_rs1
mkdir ~/mongo/data_rs1/node1  
mkdir ~/mongo/data_rs1/node2 
mkdir ~/mongo/data_rs1/node3

echo
echo running mongod processes...

small_files=" --smallfiles "
small_files=""

cd ~/mongo/program/bin; ./mongod --replSet rs1 --port=27001  --dbpath=/home/peter.buchmann/mongo/data_rs1/node1 --oplogSize 2000 $small_files   --logpath /home/peter.buchmann/mongo/data_rs1/node1/main.log --logappend --fork --rest --master
cd ~/mongo/program/bin; ./mongod --replSet rs1 --port=27002  --dbpath=/home/peter.buchmann/mongo/data_rs1/node2 --oplogSize 2000 $small_files   --logpath /home/peter.buchmann/mongo/data_rs1/node2/main.log --logappend --fork --rest
ycd ~/mongo/program/bin; ./mongod --replSet rs1 --port=27003  --dbpath=/home/peter.buchmann/mongo/data_rs1/node3 --oplogSize 50    --smallfiles  --logpath /home/peter.buchmann/mongo/data_rs1/node3/main.log --logappend --fork --rest


echo giving them time to start. note this might not be enough time!
sleep 2
ps -A | grep mongod


echo ~/mongo/program/bin/mongo --shell --port 27001 ~/mongo/program/bin/start.js
