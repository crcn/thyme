var Structr = require('structr');


var Job = module.exports = Structr({
	
	/**
	 */

	'__construct': function(worker)
	{                       
		this._worker = worker;  
		this._router = worker._router;  
		this._ops = worker._ops;      
		this._target = worker._target;                                  
	},           
	
	/**
	 */
	
	'dispose': function()
	{
		this._worker._runningJobs.splice(this._worker._runningJobs.indexOf(this), 1);
	},
	              

	/**
	 * runs the job
	 */

	'run': function(job, callback)
	{                       
		var self = this;      
		
		this._running = true; 
		this._finished = false;    
		
		this._callback = callback;
		                              
		                 
		//start the countdown to kill the queue. We don't want a job stopping all the rest. 
		//TODO: This isn't a great solution, just a bandaid. *if* there are many stale workers, memory will just
		//be consumed. This is really only great if the job can be disposed of.                          
		this._killTimeout = setTimeout(function()
		{                         
			console.warn('Queue %s is taking too long, skipping', self._ops.channel);
			
			self._onDone({ sendAt: Date.now() + self._ops.lockTTL }, true);    
			
		}, self._ops.timeout);                                                   
		                          
		                                                                          
		this._start = Date.now();
                                        

		//pull the request
		this._router.pull(this._ops.channel, job.data, { meta: { queue: this._target }}, self.getMethod('_onResponse'));
	},
	
	/**
	 */
	
	'_onResponse': function(response)
	{                        
	   	//this will happen *if* the worker has failed
		if(!this._running) return;   
		                                          
		                   
		                       
		//job is down? kill it, and notify the worker.
		if(response && response.errors && response.result && !response.result.connection)
		{
			return this._onDisconnect(true);
		}    
		
		var resp = {
			success: !!response
		}                      
		
		if(typeof response == 'object') Structr.copy(response, resp, true);
		             
		//finally we can update...          
		this._onDone(resp, !!response);
	},
	
	/**
	 */
	
	'_onDisconnect': function(notifyWorker)
	{
		console.log('Cannot add queue, stopping');  
		
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
	
	'_onDone': function(response)   
	{                 
		//done *will* be called twice if there's a response *after* the kill timeout has been executed.
		if(this._finished) return;   
		
		this._finished = true;  
		this.running = false;          
		
		this._callback(response);      
		
		clearTimeout(this._killTimeout);
	}  	
});
                              