var Structr = require('structr'),
logger = require('winston').loggers.get('thyme'),
sprintf = require('sprintf').sprintf;


var Job = module.exports = Structr({
	
	/**
	 */

	'__construct': function(worker) {                       
		this._worker = worker;    
		this._ops    = worker._ops;    
		this._target = worker._target;  
		this._router = worker._router;                                  
	},           
	
	/**
	 */
	
	'dispose': function() {
		this._worker.removeJob(this);
	},
	              

	/**
	 * runs the job
	 */

	'run': function(job, callback) {                       
		var self = this;      
		
		this._running = true; 
		this._finished = false;    
		
		this._callback = callback;                                        
		                          
		                                                                          
		this._start = Date.now();
                                    
                                    
        //console.log('pulling %s', this._worker._id);   

		//pull the request
        this._router.
        request(this._ops.channel).
        query(job.data).
        headers({ queue: this._target }).
        success(this.getMethod('_onResponse')).
        error(function(err) {
        	logger.error(err);
        	self._onDisconnect(true);
        }).pull();
	},

	/**
	 */

	'disconnect': function() {
		if(this.running || this._callback) this._onDisconnect();	
	},
	
	/**
	 */
	
	'_onResponse': function(response) {      
	           
	   	//this will happen *if* the worker has failed
		if(!this._running) return;   

        logger.debug(sprintf('%s responded', this._target._id));  
		       
		
		var resp = {
			success: !!response
		}                      
		
		if(typeof response == 'object') Structr.copy(response, resp, true);
		             
		//finally we can update...          
		this._onDone(resp, !!response);
	},
	
	/**
	 */
	
	'_onDisconnect': function(notifyWorker) {
		
        logger.error(sprintf('Cannot add queue %s, stopping', this._target));  
		
		this._running = false;   
		
		                                                                    
		//tell the worker the job has failed because there's nothing to handle it. The worker
		//at this point will flag all other jobs as "failed" 
		if(notifyWorker) this._worker._workerDown();    
		
		//update the job data to send immediately after the worker is back up
		this._onDone({
			sendAt: 0
		})
	},   
	
	
	/** 
	 * called after job is done
	 */
	
	'_onDone': function(response) {                 
		//done *will* be called twice if there's a response *after* the kill timeout has been executed.
		if(this._finished) return;   
		
		this._finished = true;  
		this._running = false;          
		
		this._callback(response);      
	}  	
});
                              