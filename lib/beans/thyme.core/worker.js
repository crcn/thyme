var Structr = require('structr'),
_ = require('underscore'),
Job = require('./job');


var Worker = Structr({
	
	/**
	 */

	'__construct': function(workers, opsOrName)
	{
		var ops = {};            
		                             

		if(typeof opsOrName == 'string')
		{
			ops = { name: opsOrName };	
		}
		else
		{
			ops = opsOrName;                   
		}
                               
		//the jobs which are currently being executed
		this._runningJobs = [];                      
		                                     
		//the proxy where calls are passed to / from (duh)
		this._router = workers.router;        
		
		//the target queue for rabbit
		this._target = workers.target;                       
		                                        
		                          
		                   
		//queue which contains all jobs           
		this._queue = workers.broker.connect(ops.name);
                                                               
		this.options(ops);     
		                     
		
		this.countQueued = _.throttle(this.getMethod('_countQueued'), 1500);      
	},

	/**
	 */

	'options': function(value)
	{                
		if(!value) return this._ops;
                   
		this._ops = {

			channel: value.channel,    
			
			// color: value.color || 'grey',

			//max number of concurrent queues of a given channel
			max: value.num || value.max || 1,

			//max number of tries after fail before we stop sending the queue
			maxTries: value.maxTries || 3,

			//timeout before we kill it
			timeout: value.timeout || 20000,

			//lock until we retry
			lockTTL: value.lockTTL || 1000 * 60 * 10,//1000 * 60 * 30,

			//delay to send the queue. -1 = immediate
			delay: -1,

			//the description of the item
			description: value.description,
				
			//how the queue is ordered: seq, group
			//based on pulled statistics 
			order: value.order
		}
	},

	/**
	 * adds a job (duh)
	 */

	'addJob': function(job)
	{                      
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
		});

		this.run();
	},


	/**
	 * runs the job
	 */

	'run': function(job)
	{                                                     
		//locked = worker is down.      
		//popping = receiving job from queue
		if(this._locked || this._popping) return false;         
		                                                                      
		                                                                          
		//make sure
		if(!job && this._runningJobs.length > this._ops.max) return false;
		     
		this.countQueued();       

		                        
		var self = this;  
		                                        
		if(!job)
		{                            
			self._runningJobs.push(job = new Job(self));
		}
		
		//clear the sleep interval incase the worker is sleeping :P
		clearTimeout(this._sleepTimeout);     
		                  
		
		//this is to prevent a race condition. The job might be done faster than jobs that can 
		//be popped off ~ queue ~ 250 ms, while job execution / ack ~ 5 ms 
		self._popping = true;
		                            
		this._queue.pop(function(err, jobData, callback)
		{              
			self._popping = false;    
			                                  
			if(!jobData)
			{                              
				//no more jobs? remove.                     
				if(job) job.dispose(); 
				                             
				return self._sleep(); 
			}  
			
			           
			job.run(jobData, function(response)
			{                             
				callback(response);       
				                        
				
				//run the job again. If the job cannot be run, then remove it
				if(!self.run(job)) job.dispose();
			});                         
			                 
			self.run();
		})  
		
		return true;
	},         
	
	/**
	 */
	
	'_pop': function()
	{
		
	},
	
	/**  
	 * called when the worker is down
	 */
	
	'_workerDown': function()
	{                                
		
		if(this._locked) return;     
		
	   var ttl = 10000, self = this;
		
		console.log('locking for %d ms', ttl);
		                        
		
		//lock the worker
		this._locked = true;        
		       
		                         
		
		//disconnect current jobs
		this._runningJobs.forEach(function(job)
		{                              
			
			//disconnect. This puts the job back into the queue
			job._onDisconnect();
		});
		                       
		
		//remove all running jobs
		this._runningJobs = [];
		                       
		                                                       
		//wait for N seconds, then try running the queue again.
		setTimeout(function(self)
		{
			self._locked = false;
			self.run();
		}, ttl, this)
	},         
	     
	/**
	 */
   
	'_countQueued': function()
	{        
		var self = this;
		                        
		this._queue.countQueued(Date.now(), function(num)
		{
			console.log('Worker %s:%s has %d items in queue, %d jobs running'.grey, self._target, self._ops.channel, num, self._runningJobs.length );  
			                                                                                                                                 
		});
	},
	        
	/**
		 * puts the worker to sleep until another job is needed
	 */

	'_sleep': function()
	{
		var self = this;   

		if(self._sleeping) return;    
		self._sleeping = true; 
		                                        


		//get the next job ~ use that timeout to run the next
		this._queue.nextTime(function(err, sendAt)
		{                       
			self._sleeping = false;    

			//IF there's a job, then timeout
			if(sendAt)
			{                       
				var timeout = Math.max(sendAt - Date.now(), 500);
				
				console.log('Worker %s sleeping for %s seconds', self._ops.channel, timeout/1000);

				self._sleepTimeout = setTimeout(self.getMethod('run'), timeout);
			}
			else
			{
				console.log('No jobs for %s, stopping.', self._ops.channel);
			}
		});
	},  
});



module.exports = Worker;