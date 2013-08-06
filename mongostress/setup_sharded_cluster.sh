#!/bin/bash

# sftp://root@46.16.76.100:22/root

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/debian-sysvinit dist 10gen' | sudo tee /etc/apt/sources.list.d/10gen.list
sudo apt-get update
sudo apt-get install mongodb-10gen
sudo apt-get install sysstat


# x=[1,2,3...]
export SHARDNUMBER=x  

# now
vi /etc/mongodb.conf
cp /etc/mongodb.conf   /etc/mongodb.install.conf

rm    /etc/mongodb.conf
touch /etc/mongodb.conf

touch  /etc/mongos.conf
# do not use...
/etc/init.d/mongodb start
/etc/init.d/mongodb stop


killall mongod
killall mongos
cd /data
rm -rf *
mkdir -p /data/mongo/configdb
mkdir -p /data/mongo/db/mongodb
mkdir -p /data/mongo/db/mongodb/repair
mkdir -p /data/mongo/arb1
chown -R mongodb:mongodb /data/mongo


# start mongod
mongod --replSet rset$SHARDNUMBER --config /etc/mongodb.conf
#mongod --replSet rset1 --port=27021  --dbpath=/data/mongo/arb1 --logpath /data/mongo/arb1/arb.log  --oplogSize 50  --smallfiles  --logappend --fork --rest


# set up replicaset on EACH of the following:
mongo --host 46.16.76.100 --port 27020
mongo --host 46.16.78.17  --port 27020
mongo --host 46.16.78.151 --port 27020

# change rset[x]
conf = 
{
    _id : 'rset[x]',
     members : [
         //{_id : 0, host : '46.16.76.100:27020' },
         //{_id : 0, host : '46.16.78.17:27020' },
         //{_id : 0, host : '46.16.78.151:27020' },
     ]
} ;
rs.initiate(conf) ;
// add the last rs member as an arbiter
// rs.addArb('localhost:27021') ;
rs.status() ;
// db.serverStatus()



# start config server with default port 27019
mongod --configsvr --fork --dbpath /data/mongo/configdb --logpath /data/mongo/configsrv.log 
# start at least one mongos
mongos --config /etc/mongos.conf


# now connect to mongos and set up sharding
mongo --host 46.16.76.100 --port 27017

db.adminCommand( { listShards: 1 } )
sh.addShard( "rset1/46.16.76.100:27020" )
sh.addShard( "rset2/46.16.78.17:27020" )
sh.addShard( "rset3/46.16.78.151:27020" )


sh.enableSharding("offer-db.offers.test" , {_id:"hashed"} ) 


cd /root/ws_go/src/github.com/pbberlin/g1/mongostress/