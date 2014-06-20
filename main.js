
var Couch       = require("./lib/couchbaseDBC.js");
var zmq         = require('m2nodehandler')
var openiLogger = require('openi-logger')
logger          = null;

var db = null;
//var db = new Couch({host: 'localhost:11211', bucket: 'default'});


var getMongrelSink = function(mongrel_sink){

   return (
      function(mongrel_sink){

         if (!mongrel_handlers[mongrel_sink.spec]){
            mongrel_handlers[mongrel_sink.spec] = zmq.sender(mongrel_sink)
         }

         return mongrel_handlers[mongrel_sink.spec]

      }(mongrel_sink, mongrel_handlers)
   )
}


var dao = function(config) {

   logger = openiLogger(config.logger_params)

   var subpush = zmq.sender(config.sub_sink)

   zmq.receiver(config.dao_sink, null, function(msg) {

      console.log(0)
      console.log('notify')
      console.log(msg)

      //var mongSink = getMongrelSink(msg.mongrel_sink)

//      helper.evaluateMessage(msg, function(err, results){
//
//         if ( undefined === msg.mongrel_resp.data ){
//            msg.mongrel_resp.data = {}
//         }
//
//         msg.mongrel_resp.data.dao_out = results
//
//         var response = zmq.Response(zmq.status.OK_200, zmq.header_json, msg.mongrel_resp)
//
//         for (var i=0; i < msg.clients.length; i++){
//            var client = msg.clients[i]
//            mongSink.send(client.uuid, client.connId, response)
//         }
//      });
   })
}



module.exports = dao
