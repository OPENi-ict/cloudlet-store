/**
 * Created by dmccarthy on 10/06/2014.
 */

'use strict';

var zmq    = require('m2nodehandler')


var push = zmq.sender({spec:'tcp://127.0.0.1:49999', bind:false, id:'test', type:'push' })


setTimeout(function(){

   console.log({test:"1234"})

   push.send({test:"1234"})
   push.send({test:"1234"})
   push.send({test:"1234"})

}, 2000)


console.log(2)