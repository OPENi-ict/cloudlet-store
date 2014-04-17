var Lazy = require('lazy.js');
var async = require('async');
var uuid = require('uuid');
var scrypt = require("scrypt");
var couchbase = require('couchbase');//.Mock;
var norm = require('./jsonnorm.js');
var evolution = require('./evolution.js');
var conf = require('./config.js');

// TYPES

function checkType(db, type, cb)
{
	// TODO checks
	cb(undefined, type);
}

function _readType(db, id, cb)
{
	db.get(id, function(err, res)
	{
		if(err)
			cb(err); //new Error('Type not found'));
		else
			cb(undefined, res.value);
	});
}

function _createMap(db, tokenID, oid, tid, cb)
{
	var n = norm.norm({'token': tokenID, 'object': oid, 'type':  tid});
	var id = norm.hash(n);
	//var id = uuid.v4();
	db.add(conf.omap.prefix + id, JSON.parse(n), function(err, res)
	{
		if(err && err.code != 18)
			cb(err); // fixme roll back
		else
			cb(undefined, id);
	});
}

function createMap(db, tokenID, oid, tid, cb)
{
	_createMap(db, conf.token.prefix + tokenID, conf.object.prefix + oid, conf.type.prefix + tid, cb);
}

function _readMap(db, id, cb)
{
	db.get(id, function(err, res)
	{
		if (err)
			cb(err);
		else
			cb(undefined, res.value);
	});
}

function readMap(db, id, cb)
{
	_readMap(db, conf.omap.prefix + id, cb);
}


function _readAllByMap(db, id, cb)
{
	_readMap(db, id, function(err, map)
	{
		if(err)
			cb(err, undefined);
		else
			_readType(db, map.type, function(err, type)
			{

				if(err)
					cb(err, map);
				else
					_readObject(db, map.object, function(err, object)
					{
						if(err)
							cb(err, map, type);
						else
							cb(undefined, map, type, object);
					});
			});
	});
}

function readAllByMap(db, id, cb)
{
	_readAllByMap(db, conf.omap.prefix + id, cb);
}

// OBJECTS

function checkObject(db, obj, type, cb)
{
	// TODO checks
	cb(undefined, obj);
}

function _readObject(db, id, cb)
{
	db.get(id, function(err, res)
	{
		if(err)
			cb(err); //new Error('Type not found'));
		else
			cb(undefined, res.value);
	});
}

function removePrefix(str)
{
	//return str.replace(/(?:.*)-(.*)/, '#');
	return Lazy(str.split('-')).shift().toArray().join('-');
}

function addPrefix(pre, str)
{
	return pre + '-' + str;
}

// --- CouchbaseDBC

function CouchbaseDBC(args)
{
	this.db = new couchbase.Connection(args);

	var views =
	[
		{
			ddoc: "objects",
			view: "objects",
			mapReduce:
			{
				map : String(function (doc, meta)
				{
					var parts = meta.id.split('-');
					if(parts.length > 0 && parts[0] == 'omap')
						emit(doc.token); //, meta.id]);
				})
			}
		},
		{
			ddoc: "objects",
			view: "objects_by_type",
			mapReduce:
			{
				map : String(function (doc, meta)
				{
					var parts = meta.id.split('-');
					if(parts.length > 0 && parts[0] == 'omap')
						emit([doc.token, doc.type]); //, meta.id]);
				})
			}
		},
		{
			ddoc: "types",
			view: "types",
			mapReduce:
			{
				map: String(function (doc, meta)
				{
					var parts = meta.id.split('-');
					if(parts.length > 0 && parts[0] == 'type')
						emit(meta.id);
				})
			}
		},
		{
			ddoc: "types",
			view: "type_properties",
			mapReduce:
			{
				map : String(function (doc, meta)
				{
					var parts = meta.id.split('-');
					if(parts.length > 0 && parts[0] == 'type')
							for(p in doc['@context'])
								emit([doc['@context'][p]['@id'], doc['@context'][p]['@type']], meta.id);
				}),
				reduce: String(function (key, values, rereduce)
				{
					return values;
				})
			}
		}
	];

	var insertView = function(db, v, cb)
	{
		db.getDesignDoc(v.ddoc , function(err, ddoc, meta)
		{
			//if(!(views[v].view in ddoc['views']) )
			{
				if (err)
					ddoc = { views : {} };
				ddoc['views'][v.view] = v.mapReduce;
				db.setDesignDoc(v.ddoc, ddoc, function( err, res )
				{
					cb(err, res);
				});
			}
		});
	}

	var db = this.db;
	async.series([
		function(cb){ insertView(db, views[0], cb); },
		function(cb){ insertView(db, views[1], cb); },
		function(cb){ insertView(db, views[2], cb); },
		function(cb){ insertView(db, views[3], cb); }
	]);
}

CouchbaseDBC.prototype = {};

CouchbaseDBC.prototype.createType = function(obj, cb)
{
	var that = this;
	checkType(this.db, obj, function(err, t)
	{
		if(err)
			cb(err);
		else
		{
			var n = norm.norm(t);
			var id = norm.hash(n);

			that.db.add(conf.type.prefix + id, JSON.parse(n), function(err, res)
			{
				if(err && err.code != 12)
					cb(new Error('Type could not be added'));
				else
					cb(undefined, id);
			});
		}
	});
}

CouchbaseDBC.prototype.readType = function(id, cb)
{
	_readType(this.db, conf.type.prefix + id, cb);
}

CouchbaseDBC.prototype.listTypes = function(sinceID, count, cb)
{
	var that = this;
	
	count = ((typeof count !== 'number' || isNaN(count)) ? 30 : count);
	skip = (typeof sinceID !== 'string' ? 0 : 1);
	sinceID = (typeof sinceID !== 'string' ? "" : conf.type.prefix + sinceID);

	this.db.view("dev_types", "types", {skip: skip, startkey_docid: sinceID, stale : false, limit : count }).query(function(err, res)
	{
		var keys = Lazy(res).pluck('key').map(removePrefix).toArray();
		/*
		that.db.getMulti(keys, null, function(err, res)
		{
			var r = [];
			for(var k in res) {
				res[k].value.id = k;
				r.push(res[k].value);
			}
			cb(r);
		});
		*/
		cb(undefined, keys);
	});
}

CouchbaseDBC.prototype.cast = function(tokenID, oid, tid, cb)
{
	var that = this;
	readMap(this.db, oid, function(err, m)
	{
		_createMap(that.db, tokenID, m.object, conf.type.prefix + tid, cb);
	});
}

CouchbaseDBC.prototype.createObject = function(tokenID, ob, cb)
{
	var that = this;
	if(!ob[conf.type.property.type.prefix])
	{
		cb(new Error('Object is missing type field'));
		return;
	}
	this.readType(ob[conf.object.property.type.prefix], function(err, t)
	{
		obj = evolution.create(ob, t).toObject();
		checkObject(that.db, obj, t, function(err, obj)
		{
			if(err)
				cb(err)
			else
			{
				var id = uuid.v4();
				that.db.add(conf.object.prefix + id, obj, function(err, res)
				{
					if(err)
						cb(new Error('Object could not be added'));
					else
						createMap(that.db, tokenID, id, ob[conf.type.property.type.prefix], function(err, res)
						{
							if(err) // fixme roll back
								cb(err);
							else
								cb(undefined, res);
						});
				});
			}
		});
	});
}

CouchbaseDBC.prototype.readObject = function(tokenID, id, as, cb)
{
	readAllByMap(this.db, id, function(err, m, t, o)
	{
		if(err)
			cb(err);
		else
		{
			if(typeof tokenID !== 'string' || (conf.token.prefix + tokenID) != m.token)
				cb(new Error('No such object'))
			else if(typeof as === 'string' && as == 'jsonld')
				cb(undefined, evolution.merge(evolution.merge(t, evolution.read(o, t).toObject()), {"@context":{"openi" : "_:"}}).toObject());
			else
				cb(undefined, evolution.read(o, t).toObject());
		}
	});
}

CouchbaseDBC.prototype.updateObject = function(id, obj, cb)
{
	var that = this;
	readAllByMap(this.db, id, function(err, m, t, o)
	{
		if(err)
			cb(err);
		else
			checkObject(that.db, obj, t, function(err, obj)
			{
				if(err)
					cb(err);
				else
				{
					o = evolution.update(o, obj, t).toObject();
					that.db.replace(m.object, o, function(err, res)
					{
						if(err)
							cb(err);//new Error('Object could not be updated'));
						else
							cb(undefined, id);
						
					});
				}
			});
	});
}

/*function isEmpty(obj)
{
    for(var key in obj)
    {
        if(obj.hasOwnProperty(key))
            return false;
    }
    return true;
}*/

CouchbaseDBC.prototype.deleteObject = function(id, cb) //todo fix merge
{
	var that = this;
	readAllByMap(this.db, id, function(err, m, t, o)
	{
		if(err)
			cb(err);
		else
		{
			o = evolution.delete(o, t);
			that.db.replace(conf.objectPrefix + m.object, o, function(err, res)
			{
				if(err)
					cb(new Error('Could not delete object'));
				else
					cb(undefined, id);
			});
		}
	});
}

CouchbaseDBC.prototype.listObjects = function(tokenID, type, sinceID, count, cb)
{
	var that = this;
	
	//userID = (typeof userID !== 'string' ? "\u0000" : userID);
	count = ((typeof count !== 'number' || isNaN(count)) ? 30 : count);
	skip = (typeof sinceID !== 'string' ? 0 : 1);
	sinceID = (typeof sinceID !== 'string' ? "" : conf.omap.prefix + sinceID);
	tokenID = conf.token.prefix + tokenID;
	
	var vie;
	var par;
	if(typeof type !== 'string')
	{
		vie = "objects";
		par = { startkey_docid: sinceID, skip: skip, startkey : tokenID, endkey: tokenID, stale : false, limit : count };
	}
	else
	{
		vie = "objects_by_type";
		type = conf.type.prefix + type;
		par = { startkey_docid: sinceID, skip: skip, startkey : [tokenID, type], endkey : [tokenID, type], stale : false, limit : count };
	}
	
	this.db.view("dev_objects", vie, par).query(function(err, res)
	{
		console.log(res);
		var values = Lazy(res).pluck('id').map(removePrefix).toArray();
		/*
		that.db.getMulti(keys, null, function(err, res)
		{
			var r = [];
			for(var k in res) {
				res[k].value.id = k;
				r.push(res[k].value);
			}
			cb(r);
		});
		*/
		cb(undefined, values);
	});
}

// -- proto

CouchbaseDBC.prototype.createUser = function(userID, passwd, cb)
{
	var db = this.db;
	//scrypt.hash.config.keyEncoding = "utf8";
	//scrypt.hash(passwd, {N: 1, r:1, p:1}, function(err, result)
	scrypt.passwordHash(passwd, 0.1, 0, 0.5, function(err, result)
	{
		if(err)
			cb(err);
		else // fixme: turn to add
			db.set(conf.user.prefix + userID, {'user': userID, 'password': result}, function(err, res)
			{
				if(err)
					cb(err);
				else
					cb(undefined, userID);
			});
	});
}

CouchbaseDBC.prototype.createApplication = function(name, cb)
{
	//var id = uuid.v4();
	var n = norm.norm({name: name}); // stable hash for dev
	var id = norm.hash(n);
	// fixme: turn to add
	this.db.set(conf.application.prefix + id, JSON.parse(n), function(err, res)
	{
		if(err)
			cb(err);
		else
			cb(undefined, id);
	});
}

function verifyCredentials(db, userID, passwd, cb)
{
	db.get(conf.user.prefix + userID, function(err, r)
	{
		if(err)
			cb(err);
		else
		{
			//scrypt.verify.config.keyEncoding = "utf8";
			//scrypt.verify(res.value.password, passwd, function(err, res)
			scrypt.verifyHash(r.value.password, passwd, function(err, res)
			{
				if(err)
					cb(err);
				else
					cb(undefined, true);
			});
		}
	});
}

CouchbaseDBC.prototype.authorizeApplication = function(userID, passwd, appID, cb)
{
	var db = this.db;
	verifyCredentials(db, userID, passwd, function(err, res)
	{
		if(err)
			cb(err);
		else
		{
			// fixme: guessable token id - insecure, dev purpose
			var n = norm.norm({user: conf.user.prefix + userID, application: conf.application.prefix + appID});
			var id = norm.hash(n);
			db.set(conf.token.prefix + id, JSON.parse(n), function(err, res)
			{
				if(err)
					cb(err);
				else
					cb(undefined, id);
			});
		}
	});
}

CouchbaseDBC.prototype.deauthorizeApplication = function(userID, passwd, tokenID, cb)
{
	var db = this.db;
	verifyCredentials(db, userID, passwd, function(err, res)
	{
		db.remove(conf.token.prefix + tokenID, function(err, res)
		{
			if(err)
				cb(err);
			else
				cb(err, true);
		});
	});
}

CouchbaseDBC.prototype.getToken = function(tokenID, cb)
{
	this.db.get(conf.token.prefix + tokenID, function(err, res)
	{
		if(err)
			cb(err);
		else
			cb(undefined, {user: removePrefix(res.value.user), application: removePrefix(res.value.application)});
	});
}

module.exports = CouchbaseDBC;
