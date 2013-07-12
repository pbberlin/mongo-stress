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

cd ~/mongo/program/bin; ./mongod --replSet rs1 --port=27001  --dbpath=/home/peter.buchmann/mongo/data_rs1/node1 --oplogSize 50 --smallfiles --logpath /home/peter.buchmann/mongo/data_rs1/node1/main.log --logappend --fork --rest
cd ~/mongo/program/bin; ./mongod --replSet rs1 --port=27002  --dbpath=/home/peter.buchmann/mongo/data_rs1/node2 --oplogSize 50 --smallfiles --logpath /home/peter.buchmann/mongo/data_rs1/node2/main.log --logappend --fork --rest
cd ~/mongo/program/bin; ./mongod --replSet rs1 --port=27003  --dbpath=/home/peter.buchmann/mongo/data_rs1/node3 --oplogSize 50 --smallfiles --logpath /home/peter.buchmann/mongo/data_rs1/node3/main.log --logappend --fork --rest


echo giving them time to start. note this might not be enough time!
sleep 2
ps -A | grep mongo

echo
echo
echo "Now run:"
echo "  ~/mongo/program/bin/mongo --shell --port 27003 start.js"
echo
echo conf = 
echo {
echo     _id : rs1,
echo      members : [
echo          {_id : 0, host : localhost:27001 },
echo          //{_id : 1, host : localhost:27002 },
echo          {_id : 2, host : localhost:27003 },
echo      ]
echo }
echo
echo # add an arbiter
echo rs.addArb("localhost:27002")
