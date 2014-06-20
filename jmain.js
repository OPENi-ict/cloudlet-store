var express = require("express");
var Couch = require("./lib/couchbaseDBC.js");

var db = new Couch({host: 'localhost:8091', bucket: 'default'});

var app = express();
app.use(express.json());
app.use(express.urlencoded());
app.use(express.logger());
app.use(express.static(__dirname + '/public'));

app.use(app.router);
app.use(function(err, req, res, next)
{
    if(!err) return next();
    console.log(err);
    res.send(err);
});

app.use(express.cookieParser());
app.use(express.session({secret: '2dedc6e63269dc3a73caf1fb'}));

function handle(res, err, rslt)
{
	if(err)
		res.send(400, err.message);
	else
	{
		res.setHeader('Content-Type', 'application/json');
		res.send(200, rslt);
	}
}

app.get('/initDummy', function(req, reslt)
{
	db.createType({'@context':{'name':{'@id':'dbpd:Name', '@type':'openi:string'}}, '@type': 'dbpd:Person'}, function(err, t1)
	{
		db.createType({'@context':{'name':{'@id':'dbpd:Name', '@type':'openi:string'}, 'age':{'@id':'dbpd:Age','@type':'openi:integer'}}, '@type': 'dbpd:Person'}, function(err, t2)
		{
			db.createType({'@context':{'name':{'@id':'dbpd:Full_Name', '@type':'openi:string'}, 'age':{'@id':'dbpd:Age','@type':'openi:string'}}, '@type': 'dbpd:Person'}, function(err, t3)
			{
				db.createUser('test', 'passwd', function(err, res)
				{
					db.createApplication('TestApp', function(err, res)
					{
						db.authorizeApplication('test', 'passwd', res, function(err, res)
						{
							/*
							db.createObject(res, {'@type':t1, 'name':'John Doe'}, function(err, o1)
							{
								//db.cast(o1, t2 , function(err, m1){});
								//db.cast(o1, t3, function(err, m2){});
							});
							db.createObject(res, {'@type':t2, 'name':'John Doe', 'age':'42'}, function(err, o2)
							{
								// db.cast(o2, t1 , function(err, m1)
								// {
								// });
								// db.cast(o2, t3, function(err, m2)
								// {
								// });
							});
							db.createObject(res, {'@type':t1, 'name':'Jane Doe'}, function(err, o1)
							{
								db.cast(res, o1, t2 , function(err, m1)
								{
									db.updateObject(m1, {'name': 'Foo Doe'}, function(err, r)
									{
										console.log(r);
										console.log(err);
									});
								});
								db.updateObject(o1, {'name': 'Jup Doe', 'age': '17', 'foo': 'bar'}, function(err, r)
								{
									console.log(r);
									console.log(err);
								});
								db.updateObject(o1, {'name': 'Moo Doe', 'age': '133742'}, function(err, r)
								{
									console.log(r);
									console.log(err);
								});
							});
							*/
							
							db.updateObject('9f1be71e1dd0ef5efa8368255cc1ae94-158', {'name': 'Moo Doe', 'age': '133742'}, function(err, r)
							{
								console.log(r);
								console.log(err);
							});

							reslt.send(200, res);
						});
					});
				});
				
			});
		});
	});
});

function printAOrB(err, res)
{
	if(err)
		console.log(err);
	else
		console.log(res);
}

app.get('/token', function(req, rs)
{
	db.createUser('hange', 'passwd123*', function(err, res)
	{
		printAOrB(err, res);
		db.createApplication('foobar', function(err, res)
		{
			printAOrB(err, res);
			db.authorizeApplication('hange', 'passwd123*', res, function(err, res)
			{
				printAOrB(err, res);
				db.getToken(res, function(err, r)
				{
					printAOrB(err, r);
					/*
					db.deauthorizeApplication('hange', 'passwd123*', res, function(err, res)
					{
						printAOrB(err, res);
					});
					*/
				});
			});
		});
	});

	rs.send(200);


	/*
	db.createUser('foo', 'bar', function(err, res)
	{
		if(err)
			console.log(err);
		else
		{
			console.log(res);
			db.login('foo', 'foopw', function(err, res)
			{
				if(err)
					console.log(err);
				else
					console.log(res);
			});
			db.login('foo', 'bar', function(err, res)
			{
				if(err)
					console.log(err);
				else
					console.log(res);
			});
		}
	});
	*/
});

app.post('/types', function(req, res)
{
	db.createType(req.body, function(err, rslt)
	{
		handle(res, err, rslt);
	});
});

app.get('/types', function(req, res)
{
	db.listTypes(req.query.since_id, parseInt(req.query.count), function(err, rslt)
	{
		handle(res, err, rslt);
	});
});


app.get('/types/:id', function(req, res)
{
	db.readType(req.params['id'], function(err, rslt)
	{
		handle(res, err, rslt);
	});
});

app.post('/objects', function(req, res)
{
	db.createObject(req.query.token, req.body, function(err, rslt)
	{
		handle(res, err, rslt);
	});
});

app.get('/objects', function(req, res)
{
	// todo paging search
	// params in format prop:type

	db.listObjects(req.query.token, req.query.type, req.query.since_id, parseInt(req.query.count), function(err, rslt)
	{
		handle(res, err, rslt);
	});
}); 

app.get('/objects/:id', function(req, res)
{
	db.readObject(req.query.token, req.params['id'], req.query.as, function(err, rslt)
	{
		handle(res, err, rslt);
	});
});

app.post('/cast', function(req, res)
{
	db.cast(req.query.token, req.body, function(req, res)
	{
		handle(res, err, rslt);
	});
});

app.listen(13370);
