var Structr = require('structr'),
Worker = require('./worker');


var Workers = module.exports = Structr({

	/**
	 * @param target the target cluster 
	 * @broker broker used 
	 */
	 
	'__construct': function(target, broker, router)
	{
		//the target cluster collection
		this.target = target;

		//calls to be made to
		this.router = router;

		//redis, mongo, etc.
		this.broker = broker;

		//info about each worker
		this._workers = { };
	},


	/**
	 * returns any given worker
	 */

	 'worker': function(channel, options)
	 {                        
	 	var worker = this._getWorker(this.target + channel);   
	                                                      

	 	//options exist? set them for the worker. They include info like
	 	//how many concurrent requests should be made for a given worker
	 	if(options) worker.options(options); 
	    
		worker._ops.channel = channel;

	 	return worker;
	 },

	/**
	 * returns a worker, or creates a new one
	 */

	'_getWorker': function(channel)
	{
		return this._workers[channel] || (this._workers[channel] = new Worker(this, channel));
	},
	
})