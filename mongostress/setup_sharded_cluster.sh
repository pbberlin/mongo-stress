#!/bin/bash


echo ~/mongo/program/bin/mongo b30.lvl.bln/admin -u admin -p mdbpw4US
echo ~/mongo/program/bin/mongo b30.lvl.bln/offerStore_operation_scale -u scale_tester -p 32168
echo ~/mongo/program/bin/mongo b30.lvl.bln:27018/admin  -u admin -p mdbpw4US

echo "db.currentOp()" | ~/mongo/program/bin/mongo b30.lvl.bln/offerStore_operation_scale -u scale_tester -p 32168 >> currentOp.txt
echo db.currentOp().inprog.forEach(    function(d){      if(   d.op != "read"  && d.op != "getmore"  )         printjson(d)      })


sudo apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/debian-sysvinit dist 10gen' | sudo tee /etc/apt/sources.list.d/10gen.list
sudo apt-get update
#sudo apt-get install mongodb-10gen
sudo apt-get install mongodb-10gen=2.4.5
sudo apt-get install sysstat


# x=[1,2,3...]
export SHARDNUMBER=x
echo "export SHARDNUMBER=$SHARDNUMBER" >> /root/.profile

/etc/init.d/mongodb stop

# now

cp /etc/mongodb.conf   /etc/mongodb.install.conf
rm    /etc/mongodb.conf
touch /etc/mongodb.conf

touch  /etc/mongos.conf
# do not use...
# /etc/init.d/mongodb start
# /etc/init.d/mongodb stop



killall mongod
killall mongos
cd /data
rm -rf *
mkdir -p /data/mongo/configdb1
mkdir -p /data/mongo/configdb2
mkdir -p /data/mongo/configdb3
mkdir -p /data/mongo/db/mongodb
mkdir -p /data/mongo/db/mongodb/repair
mkdir -p /data/mongo/mmslog
mkdir -p /data/mongo/arb1
chown -R mongodb:mongodb /data/mongo

ulimit -f unlimited
ulimit -t unlimited
ulimit -v unlimited
ulimit -n 64000
ulimit -m unlimited
ulimit -u 32000


# copy configs to each
46.16.77.246
46.16.76.226
46.16.78.17
46.16.78.55
46.16.78.151
scp /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress/shard_start_scripts/*  root@46.16.76.226:/etc/


# start mongod
mongod --replSet rset$SHARDNUMBER --config /etc/mongodb.conf
#mongod --replSet rset1 --port=27021  --dbpath=/data/mongo/arb1 --logpath /data/mongo/arb1/arb.log  --oplogSize 50  --smallfiles  --logappend --fork --rest


# set up replicaset on EACH of the following:
mongo --host 46.16.78.17 --port 27020
conf = {  _id : 'rset1', members : [{_id : 0, host : '46.16.78.17:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;
mongo --host 46.16.78.55  --port 27020
conf = {  _id : 'rset2', members : [{_id : 0, host : '46.16.78.55:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;
mongo --host 46.16.78.151 --port 27020
conf = {  _id : 'rset3', members : [{_id : 0, host : '46.16.78.151:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;


// add the last rs member as an arbiter
// rs.addArb('localhost:27021') ;
// db.serverStatus()



# start three config servers 
mongod --configsvr --fork --dbpath /data/mongo/configdb1 --logpath /data/mongo/configsrv1.log --port 27030
mongod --configsvr --fork --dbpath /data/mongo/configdb2 --logpath /data/mongo/configsrv2.log --port 27031
mongod --configsvr --fork --dbpath /data/mongo/configdb3 --logpath /data/mongo/configsrv3.log --port 27032
# start at least one mongos
mongos --config /etc/mongos.conf

ps aux | grep mongo

# now connect to mongos and set up sharding
mongo --host 46.16.76.226 --port 27017
db.adminCommand( { listShards: 1 } )
sh._adminCommand( { addShard:"rset1/46.16.78.17:27020", maxSize:0, name:"sh1"} , true )
sh._adminCommand( { addShard:"rset2/46.16.78.55:27020" , maxSize:0, name:"sh2"} , true )
sh._adminCommand( { addShard:"rset3/46.16.78.151:27020", maxSize:0, name:"sh3"} , true )


sh.enableSharding("offer-db") 
//sh.shardCollection("offer-db.offers.test" , {_id: "hashed"} ) 
sh.shardCollection("offer-db.offers.test"         , {_id: 1} ) 
sh.shardCollection("offer-db.offersByShop"        , {_id: 1} ) 
sh.shardCollection("offer-db.offersByLastUpdated" , {_id: 1} ) 




cd ~
wget https://mms.10gen.com/settings/mmsAgent/90222bce0616c829f72a68f4eb1e3a9c/10gen-mms-agent-idealo.tar.gz
tar xvzf ./10gen-mms-agent-idealo.tar.gz
cd mms-agent
wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O - | python
#python ez_setup.py
easy_install pymongo
ps aux | grep "python agent.py"
killall "python"

cd ~/mms-agent
nohup python agent.py > /data/mongo/mmslog/agent.log 2>&1 &
less /data/mongo/mmslog/agent.log


# ===========================================================
mkdir -p /root/ws_go/src/github.com/pbberlin/g1/mongostress/
cd /root/ws_go/src/github.com/pbberlin/g1/mongostress/
scp -r  /home/peter.buchmann/ws_go/src  root@46.16.77.246:/root/ws_go/


sftp://root@46.16.77.246:22/root/ws_go/src/

apt-get install golang
export GOPATH="/root/ws_go/"
echo "/root/ws_go/" >> /root/.profile

cd /root/ws_go/src/github.com/pbberlin/g1/mongostress/
go run mongostress.go

ps aux | grep a.out

http://46.16.77.246:8080/tpl/sss

ssh root@46.16.77.246