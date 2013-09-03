#!/bin/bash

PATH=/home/peter.buchmann/mongo/program/bin/:$PATH



sudo apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/debian-sysvinit dist 10gen' | sudo tee /etc/apt/sources.list.d/10gen.list
sudo apt-get update
sudo apt-get remove mongodb-10gen
#sudo apt-get install mongodb-10gen
#sudo apt-get install mongodb-10gen=2.4.5
sudo apt-get install mongodb-10gen=2.2.3
sudo apt-get install sysstat



# x=[1,2,3...]
export SHARDNUMBER=x
echo "export SHARDNUMBER=$SHARDNUMBER" >> /root/.profile

/etc/init.d/mongodb stop

# now

cp    /etc/mongodb.conf   /etc/mongodb.install.conf
rm    /etc/mongodb.conf
touch /etc/mongodb.conf
touch /etc/mongos.conf
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
# (use scp_over_list.sh script for this)
# LOCALHOST
sudo su
vi /etc/hosts
# add hosts copied


# start three config servers 
mongod --configsvr --fork --dbpath /data/mongo/configdb1 --logpath /data/mongo/configsrv1.log --port 27030
mongod --configsvr --fork --dbpath /data/mongo/configdb2 --logpath /data/mongo/configsrv2.log --port 27031
mongod --configsvr --fork --dbpath /data/mongo/configdb3 --logpath /data/mongo/configsrv3.log --port 27032


# start mongod 
rm /data/mongo/db/mongodb/mongod.lock
mongod --replSet rset$SHARDNUMBER --config /etc/mongodb.conf
#mongod --replSet rset1 --port=27021  --dbpath=/data/mongo/arb1 --logpath /data/mongo/arb1/arb.log  --oplogSize 50  --smallfiles  --logappend --fork --rest



# set up replicaset on EACH of the following:
mongo --host mgod01 --port 27020
conf = {  _id : 'rset1', members : [{_id : 0, host : 'mgod01:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;
xexit
mongo --host mgod02  --port 27020
conf = {  _id : 'rset2', members : [{_id : 0, host : 'mgod02:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;
xexit
mongo --host mgod03 --port 27020
conf = {  _id : 'rset3', members : [{_id : 0, host : 'mgod03:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;
xexit
mongo --host mgod04 --port 27020
conf = {  _id : 'rset4', members : [{_id : 0, host : 'mgod04:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;
xexit
mongo --host mgod05 --port 27020
conf = {  _id : 'rset5', members : [{_id : 0, host : 'mgod05:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;
xexit
mongo --host mgod06 --port 27020
conf = {  _id : 'rset6', members : [{_id : 0, host : 'mgod06:27020' }, ] } ;
rs.initiate(conf) ;
rs.status() ;
xexit



// add the last rs member as an arbiter
// rs.addArb('localhost:27021') ;
// db.serverStatus()

mongo --port 27020
use admin
db.adminCommand({shutdown : 1, force : true})


# start at least one mongos
mongos --config /etc/mongos.conf


ps aux | grep mongo

# now connect to mongos and set up sharding
mongo --host mgos01 --port 27017
db.adminCommand( { listShards: 1 } )
sh._adminCommand( { addShard:"rset1/mgod01:27020" , maxSize:0, name:"sh1"} , true )
sh._adminCommand( { addShard:"rset2/mgod02:27020" , maxSize:0, name:"sh2"} , true )
sh._adminCommand( { addShard:"rset3/mgod03:27020" , maxSize:0, name:"sh3"} , true )
sh._adminCommand( { addShard:"rset4/mgod04:27020" , maxSize:0, name:"sh4"} , true )
sh._adminCommand( { addShard:"rset5/mgod05:27020" , maxSize:0, name:"sh5"} , true )
sh._adminCommand( { addShard:"rset6/mgod06:27020" , maxSize:0, name:"sh6"} , true )






sh.enableSharding("offer-db") 
//sh.shardCollection("offer-db.offers.test"       , {_id: "hashed"} ) 
sh.shardCollection("offer-db.offers.test"         , {_id: 1} ) 
sh.shardCollection("offer-db.offersByShop"        , {_id: 1} ) 
sh.shardCollection("offer-db.offersByLastUpdated" , {_id: 1} ) 




cd ~
wget https://mms.10gen.com/settings/mmsAgent/8c2f08ab15ce0716402aa105fffdd44d/10gen-mms-agent-idealo_test.tar.gz
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
scp -r  /home/peter.buchmann/ws_go/src  root@stress01:/root/ws_go/
scp -r  /home/peter.buchmann/ws_go/src  root@stress02:/root/ws_go/

scp -r  /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress/mongostress.go  root@stress01:/root/ws_go/src/github.com/pbberlin/g1/mongostress/mongostress.go
scp -r  /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress/mongostress.go  root@stress02:/root/ws_go/src/github.com/pbberlin/g1/mongostress/mongostress.go


scp -r  /usr/bin/go     root@stress02:/usr/bin/go
scp -r  /usr/bin/godoc  root@stress02:/usr/bin/godoc
scp -r  /usr/bin/gofmt  root@stress02:/usr/bin/gofmt
scp -r  /usr/lib/go     root@stress02:/usr/lib/go


sftp://root@stress02:22/root/ws_go/src/

apt-get install golang
# or wget http://go.googlecode.com/files/go1.1.2.linux-amd64.tar.gz
# dpkg -i ~/go[tab]

export GOPATH="/root/ws_go/"
echo "/root/ws_go/" >> /root/.profile
cd /root/ws_go/src/github.com/pbberlin/g1/mongostress/
go run mongostress.go

ps aux | grep a.out

http://stress02:8080/tpl/sss

ssh root@stress02


echo "" > /data/mongo/db/mongodb.log 
df -h
iostat -dmx 2 1
free -g
ps aux | grep mongo







test single shard with ssd
===================
4.200 inserts/sec
4.200 updates/sec
400.000 reads/sec
mongod iostat < 5%
mongos cpu > 80%



test two shards with ssd
four mongos, all locally together with stress and mongod
b30-b33
===================
~8.000 inserts/sec (3.750, 4.200)
iostat ~12% +-10
mongod cpu ~100% 
mongos cpu ~ 40%
stress cpu ~ 50%



~14.350 updates/sec (8.000, 6.350)
iostat ~20%  - peaks of 95% - 140 MB/s write, 33.000 writes/pers (IOPS)
mongod cpu ~140% 
mongos cpu ~ 70%
stress cpu ~ 60%





up to 40 million offers, 1 kB each, 50 GB (incl. indexes), equally distributed via salted hash of id, 

insert test 1
===================
3 shards
2 mongos, 1 GB RAM
data size < phys. RAM
2 stresser each with < 16 stress threads
< 50 Percent per Shard CPU 
< 50 Percent per Shard IOStat Utilization, avg 10 percent
< 2000 Inserts per Shard


insert test 2
===================
6 shards
3 mongos, 1/2/4 GB RAM - identical performance
3 stresser each with <= 8 stress threads
data size < phys. RAM
< 25 Percent per Shard CPU 
~  5 Percent per Shard IOStat Utilization, avg +-20 
< 950 Inserts per Shard, ~ 5700 whole cluster

MMS background flush  < 1 Sec
MMS page faults, network, lock percent - all tiny


update test
===================
6 shards
3 mongos, 1/2/4 GB RAM - identical performance
3 stresser with 24 stress threads each
40 mio offers, 1 kB each, 50 GB storage incl. indexes, equally distributed via salted hash of offerid 
data size < phys. RAM (sum of shards)
~ 1600 Updates/Sec per Shard,  ~ 9500 whole cluster
~ 60 Percent CPU load per shard (primary)
~ 50 Percent IOStat Utilization per shard, avg +-50 
~ 40 Percent CPU load per mongos

MMS page faults, network, lock percent - all tiny
MMS background flush  ~5 Sec		=> IO subsystem on the verge


secondary data structures
===================
mongos> db.offersByLastUpdated.find({},{_id:-1})
		{ "_id" : "1377094490::9c2dc1b1a73168afef077bd5::mgod05" }
		{ "_id" : "1377094490::4e9eb0e4c1dab1c886e3d181::mgod03" }
		{ "_id" : "1377094490::45ec4b999192ab50eb8aaa72::mgod03" }
		{ "_id" : "1377094490::6b17dee4a1df216b8b7538e8::mgod04" }

mongos> db.offersByLastUpdated.find({},{_id:0,op:0}).sort({lastUpdated:-1})
 		{ "lastUpdated" : 1377096871, "fk_id" : ObjectId("6b17dee4a1df216b8b7538e8") }
 
mongos> db.offers.test.find({  "_id" : ObjectId("6b17dee4a1df216b8b7538e8") },{_id:1,lastUpdated:1,countUpdates:1,lastSeen:1})
		{ "_id" : ObjectId("6b17dee4a1df216b8b7538e8"), "countUpdates" : 4, "lastSeen" : 1377018050, "lastUpdated" : 1377096871 }



go run mongostress.go 

cd /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress/
go build  mongostress.go
go install 
/home/peter.buchmann/ws_go/bin/mongostress --cpuprofile p.prof
go tool pprof mongostress p.prof


export hnx="b33"
scp -r  /home/peter.buchmann/ws_go/bin/mongostress	  operations@$hnx:/home/operations/stress/
scp -r  /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress/*.html    operations@$hnx:/home/operations/stress/
scp -r  /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress/*.ini     operations@$hnx:/home/operations/stress/


#cd ~/stress; nohup ./mongostress 2>&1  | tee output.log | ftee fifolog &
#cd ~/stress; nohup ./mongostress 2>&1  | tee output.log 
cd ~/stress; nohup ./mongostress & 
exit
ps aux | grep mongostress


db.adminCommand("writeBacksQueued") 
db.runCommand({writeBacksQueued: 1})
db.adminCommand({writeBacksQueued: 1})
writebacklisten

lvl3_repl01/b30:27018
mongo b30.lvl.bln/admin   -u admin -p mdbpw4US
mongo b30.lvl.bln:27018   -u admin -p mdbpw4US 

After chunk migrations in a sharded cluster, there is a small window when writes could go to the wrong shard.
The writeback listener catches (?) such misdirected writes and "relays [them] back" (=sends them back?) from the wrong mongod/shard to the right mongod/shard (via a mongos?).
A mongos instance "lazily updates" chunk configuration changes. Possibly only after a write back?

Nach chunk migrations + chunk configuration updates gibt's unter Umständen heimtückische kleine Übergangseffekte. Arbeitshypothese: nach Neustart des writing mongos geht's wieder.





The writeback listener is a process that opens a long poll to relay writes back from a mongod or mongos after migrations to make sure they have not gone to the wrong server. The writeback listener sends writes back to the correct server if necessary.