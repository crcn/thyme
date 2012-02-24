var Structr = require('structr'),
_           = require('underscore'),
Job         = require('./job'),
logger      = require('winston').loggers.get('thyme'),
sprintf     = require('sprintf').sprintf,
outcome     = require('outcome');



var Worker = Structr({
	
	/**
	 */

	'__construct': function(workers, path) {
       
		                             
		//the jobs which are currently being executed
		this._runningJobs = [];    
		                                     
		//the proxy where calls are passed to / from (duh)
		this._router = workers.router;        
		
		//the target queue for rabbit
		this._target = workers.target;     
		
		this._path = path;  
		                   
		//queue which contains all jobs           
		this._queue = workers.broker.connect(path);
		     
		this._id = this._target + ':' + path;     
		                     
		this.countQueued = _.throttle(this.getMethod('_countQueued'), 1500);    
		
		this.options({});    
	},


	/**
	 */

	'options': function(value) {    
	            
		if(!value) return this._ops;

		this._ops = {
 
			//max number of concurrent queues of a given path
			max: value.num || value.max || 1,

			//max number of tries after fail before we stop sending the queue
			maxTries: value.maxTries || 3,

			//timeout before we kill it
			timeout: value.timeout || 20000,

			//lock until we retry
			lockTTL: value.lockTTL || 0,//1000 * 60 * 10,//1000 * 60 * 30,

			//delay to send the queue. -1 = immediate
			delay: -1,

			//the description of the item
			description: value.description,
				
			//how the queue is ordered: seq, group
			//based on pulled statistics 
			order: value.order
		};

		return this._ops;
	},

	/**
	 * adds a job (duh)
	 */

	'addJob': function(job) {      
	
		var self = this;
		                
		this._queue.push({   
			
			//id of the job incase it could be added again
			jobId: job._id,

			//data for the job
			data: job.data,

			//date job was created
			createdAt: Date.now(),

			//WHEN to send the job
			sendAt: job.sendAt || Date.now(),

			//number of tries on current job
			tries: 1

		}, function() {

			self.run();

		});
	},


	/**
	 * runs the job
	 */

	'run': function() {  
        
		//locked = worker is down.      
		//popping = receiving job from queue
		if(this._locked) return false;         
		                                                                    
		if(this._runningJobs.length > this._ops.max) return false;
		
		//clear the sleep interval incase the worker is sleeping :P
		clearTimeout(this._sleepTimeout);     
		                  
		return this._run();
	},    

	/**
	 */

	'_run': function(job) {

		if(this._popping) return false;
		this._popping = true;

		var self = this;
		job      = job || this._nextJob();

		
		function onNoJob() {

			//no more jobs? remove.                     
			if(job) job.dispose(); 
				                             
			return self._sleep(); 

		}
		                            
		this._queue.pop(outcome.success(function(task, callback) {     

			self._popping = false;    

			                                  
			if(!task) return onNoJob(); 

			           
			job.run(task, function(response) {    
			                         
				callback(response);  
				                        
				//run the job again. If the job cannot be run, then remove it
				if(!self._run(job)) {

					job.dispose();

				}

			});                         
			      
			//run until we cannot run           
			self.run();

		}).
		error(function(err) {

			self._popping = false;
			logger.error(err);
			onNoJob();

		}));

		return true;
	},
	
	/**
	 */
	 
	'_nextJob': function() {

		var job = new Job(this._router, this._target, this._path), self = this;
		
		this._runningJobs.push(job);

		job.once('dispose', function() {

			var i = self._runningJobs.indexOf(job);
			if(i > -1) self._runningJobs.splice(i, 1);

		});

		job.once('down', function() {

			self._lock(10000);

		});

		return job;
	},  
	
	
	/**  
	 * called when the worker is down
	 */
	
	'_lock': function(ms) {                                
		
		if(this._locked) return;     
		
	   var self = this;
		
		logger.info(sprintf('locking for %d ms', ms));                    
		
		//lock the worker
		this._locked = true;                             
		
		//disconnect current jobs
		this._runningJobs.forEach(function(job) {                              
			
			//disconnect. This puts the job back into the queue
			job.abort();

		});               
		
		//remove all running jobs
		this._runningJobs = [];
                                                
		//wait for N seconds, then try running the queue again.
		setTimeout(function(self) {

			self._locked = false;
			self.run();

		}, ms, this)
	},         
	     
	/**
	 */
   
	'_countQueued': function() { 
	      
		var self = this;
		                        
		this._queue.countQueued(Date.now(), function(num) {

			logger.debug(sprintf('Worker %s has %d items in queue, %d jobs running'.grey, self._id,  num, self._runningJobs.length));        
			                                                                                                                      
		});

	},
	        
	/**
	 * puts the worker to sleep until another job is needed
	 */

	'_sleep': function() {

		var self = this;   

		if(self._sleeping) return;  
		  
		self._sleeping = true; 

		//get the next job ~ use that timeout to run the next
		this._queue.nextTime(function(err, sendAt) { 
		                      
			self._sleeping = false;    

			//IF there's a job, then timeout
			if(sendAt) {                       

				//throttle the timeout
				var timeout = Math.max(sendAt - Date.now(), 500);
				
				console.log('Worker %s sleeping for %s seconds', self._id, timeout/1000);

				self._sleepTimeout = setTimeout(self.getMethod('run'), timeout);
			}
			
		});

	},  
});

module.exports = Worker;