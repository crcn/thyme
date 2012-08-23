exports.plugin = function(router) {

	var params = this.params();

	this.
	paths(__dirname + '/plugins').
	paths(__dirname + '/../node_modules').
	params(params.mongodb || {
	    'mongodb': {
	        "host": process.env.MONGO_HOST || "localhost",
	        "database": process.env.MONGO_DATABASE || "thyme"
	    }
	}).
	require({
		"bean.database.mongo": true,
	    "thyme.broker.mongo": true,
	    "thyme.core": true
	});
}