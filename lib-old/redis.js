var beanpole = require('beanpole');


var params = {
	'db.external.mongo':{
		host: 'minimacblack.local',
		database: 'spiceio'
	}
}

beanpole.
params(params).
require(['hook.core','hook.http.mesh']).
require(__dirname + '/beans').
push('init')