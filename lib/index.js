var beanpole = require('beanpole');


var params = {
	'db.external.mongo':{
		host: 'minimacblack.local',
		database: 'spiceio'
	}
}

beanpole.
params(params).
require(['glue.core','glue.http']).
require(__dirname + '/beans').
push('init')