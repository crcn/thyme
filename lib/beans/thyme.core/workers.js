var Structr = require('structr'),
Worker = require('./worker');


var Workers = module.exports = Structr({

	/**
	 * @param target the target cluster 
	 * @broker broker used 
	 */
	 
	'__construct': function(target, broker, router) {

		//the target cluster collection
		this.target = target;

		//calls to be made to
		this.router = router;

		//redis, mongo, etc.
		this.broker = broker;

		//info about each worker
		this._workers = { };

		this._throttle = { timeout: 500, max: 0 };

	},

	/**
	 */

	'throttle': function(data) {
		if(arguments.length) {
			this._throttle = data;
		}

		return this._throttle;
	},


	/**
	 * returns any given worker
	 */

	 'worker': function(path, options) {   
	                      
	 	var worker = this._getWorker(path);     
	                                               

	 	//options exist? set them for the worker. They include info like
	 	//how many concurrent requests should be made for a given worker
	 	if(options) {

			worker.options(options);  

		}

	 	return worker;

	 },

	/**
	 * returns a worker, or creates a new one
	 */

	'_getWorker': function(path) {

		return this._workers[path] || (this._workers[path] = new Worker(this, path));

	},
	
})