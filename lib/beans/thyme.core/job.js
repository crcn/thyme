var Structr = require('structr'),
logger = require('winston').loggers.get('thyme'),
sprintf = require('sprintf').sprintf,
EventEmitter = require('events').EventEmitter;


var Job = module.exports = Structr({
	
	/**
	 */

	'__construct': function(router, target, path) {  

		this._em = new EventEmitter();

		//target queue
		this._target  = target;  

		//THIS router
		this._router  = router; 

		//path we're targeting
		this._path = path;
		                                 
	},    
	
	/**
	 */
	 
	'on': function(type, callback) {

		this._em.on(type, callback);

	},

	/**
	 */

	'once': function(type, callback) {

		this._em.on(type, callback);

	},
	
	/**
	 */
	
	'dispose': function() {

		this._em.emit('dispose');

	},
	              

	/**
	 * runs the job
	 */

	'run': function(task, callback) {                       
		
		var self = this;      
		
		this._running  = true; 
		this._complete = false;    
		this._callback = callback;  
		
		//pull the request
        this._request = this._router.
        request(this._path).
        query(task.data).
        headers({ queue: this._target }).
        success(this.getMethod('_onSuccess')).
        error(this.getMethod('_onError')).pull();

	},

	/**
	 */

	'abort': function() {

		if(this._running || this._callback) this._abort(false);	

	},

	/**
	 * errors should *not* occur
	 */

	'_onError': function(err) {

        logger.error(err);
        this._abort(true);

	},
	
	/**
	 */
	
	'_onSuccess': function(response) {      
	           
	   	//this will happen *if* the worker has failed
		if(!this._running) return;   

        logger.debug(sprintf('%s responded', this._target));  
		       
		
		var resp = {
			success: !!response
		}                      
		
		if(typeof response == 'object') Structr.copy(response, resp, true);
		             
		//finally we can update...          
		this._onComplete(resp);
	},
	
	/**
	 */
	
	'_abort': function(targetDisconnected) {
		
        logger.error(sprintf('Cannot fulfill %s task, stopping', this._target));  
		
		this._running = false;   
		               
		//tell the worker the job has failed because there's nothing to handle it. The worker
		//at this point will flag all other jobs as "failed" 
		if(targetDisconnected) this._em.emit('down');   
		
		//update the job data to send immediately after the worker is back up
		this._onComplete({
			sendAt: 0
		});

	},   

	
	
	/** 
	 * called after job is done
	 */
	
	'_onComplete': function(response) {      

	    
		//done *will* be called twice if there's a response *after* the kill timeout has been executed.
		if(this._complete) return;   
		
		this._complete = true;  
		this._running  = false;    
		    
		this._callback(null, response);  
		this._em.emit('complete', response);
	}  	
});
                              