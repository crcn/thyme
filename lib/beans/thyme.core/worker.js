var Structr = require('structr'),
_ = require('underscore');


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

		this._numRunning = 0;
		this._router = workers.router;
		this._target = workers.target;

		this._queue = workers.broker.connect(ops.name);

		//unlock anything that might be stuck after crash
		//this._queue.unlock();

		this.options(ops);     
		                     
		
		this.countQueued = _.throttle(this.getMethod('_countQueued'), 500);
	},

	/**
	 */

	'options': function(value)
	{                
		if(!value) return this._ops;
                   
		this._ops = {

			channel: value.channel,



			//max number of concurrent queues of a given channel
			max: value.num || value.max || 1,

			//max number of tries after fail before we stop sending the queue
			maxTries: value.maxTries || 3,

			//timeout before we kill it
			timeout: value.timeout || 20000,

			//lock until we retry
			lockTTL: value.lockTTL || 1000 * 60 * 30,

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

	'run': function(skip)
	{
                  
		this.countQueued();
		
		//clear the sleep interval incase the worker is sleeping :P
		clearTimeout(this._sleepTimeout);

		//if running, then there's stuff in the queue. OTHERWISE, skip is called internally
		if((this._running && !skip) || !this._ops.channel)
		{
			return;
		}          
		
		var self = this;

		//make sure the route is up before moving on...
		if(!this._router.has('pull ' + this._ops.channel))
		{
			self._running = false;
			self._numRunning = 0;
			return console.warn('Worker is not up, cannot run job "%s"', this._ops.channel);
		}

		//NOW we're running...
		this._running = true;


		var self = this;



		///pop off the next job
		this._queue.pop(function(err, job)
		{                    
			//no job? go to sleep
			if(!job) return self._sleep();

                            

			self._numRunning++;         
			                        

			//run until we can't run anymore...
			if(self._numRunning < self._ops.max)
			{
				self.run(true);
			}  
			       
			self._run(job);
		});
	},

	/**
	 * runs the given jobs
	 */

	'_run': function(job)
	{                                                                                    

		var self = this, killed;     
		
		function done(resp)
		{          
			if(killed) return;
			 
			//decr num running
			self._numRunning--;    
			       
			job.done(resp);
			
			//run the next item.
			self.run(true);     
			
			clearTimeout(killTimeout);
		}  
		                 
		//start the countdown to kill the queue                          
		var killTimeout = setTimeout(function()
		{                         
			
			console.warn('Queue %s is taking too long, skipping', self._ops.channel);
			
			done({ sendAt: Date.now() + self._ops.lockTTL });
			killed = true;
			
		}, self._ops.timeout);
                                               

		//pull the request
		this._router.pull(this._ops.channel, job.data, function(response)
		{                              
			
			var resp = {
				success: !!response
			}                      
			
			if(typeof response == 'object') Structr.copy(response, resp, true);
			             
			//finally we can update...          
			done(resp);
                             
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
		this._queue.next(function(err, job)
		{
              
			self._sleeping = false;
			
			//unlock
			self._running = false;

			//IF there's a job, then timeout
			if(job)
			{
				var timeout = Math.max(job.sendAt - Date.now(), 1);

				console.log('Worker %s sleeping for %s ms', self._ops.channel, timeout);

				self._sleepTimeout = setTimeout(self.getMethod('run'), timeout);
			}
			else
			{
				console.log('No jobs for %s, stopping.', self._ops.channel);
			}
		});
	},
	
	/**
	 */
   
	'_countQueued': function()
	{        
		var self = this;
		                        
		this._queue.countQueued(Date.now(), function(num)
		{
			console.log('Worker %s has %d items in queue', self._ops.channel, num);
		});
	}
});



module.exports = Worker;