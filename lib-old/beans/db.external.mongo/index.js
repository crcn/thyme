var mongoose = require('mongoose');

exports.params = {
	host: {
		type: 'input', 
		name: 'Host'
	},
	database: {
		type: 'input',
		name: 'Default database'
	},
	user: {
		type: 'input',
		name: 'Username'
	},
	password: {
		type: 'input',
		name: 'Password'
	}
};



exports.init = function(mediator, params)
{
	console.ok('Starting up application database');
	
	
	var connectionString = '';
	
	if(params.user && params.password)
	{
		connectionString += params.user + ':' + params.password + '@';
	}
	
	connectionString += (params.host || 'localhost') + '/' + params.database;

	mongodb = mongoose.createConnection('mongodb://' + connectionString);
	
	mediator.push('mongodb', mongodb);
	
	
	function getMongodb(callback)
	{
		callback(mongodb);
	}
	
	mediator.on({
		'pull mongodb': getMongodb
	});
	
	return mongodb;
}


exports.plugin = function(mediator, params)
{
	
	function init()
	{
		exports.init(mediator, params);
	}
	
	
	mediator.on({
		'push init': init
	});
}



