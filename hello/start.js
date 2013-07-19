conf = 
{
    _id : 'rs1',
     members : [
         {_id : 0, host : 'localhost:27001' },
         {_id : 1, host : 'localhost:27002' }
     ]
}
rs.initiate(conf)
// add the last rs member as an arbiter
rs.addArb('localhost:27003')
rs.status()
