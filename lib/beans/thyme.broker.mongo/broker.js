var redis = require('redis'),
Structr = require('structr'),
_ = require('underscore'),
cashew = require('cashew');    



var Broker = Structr({
	
	/**
	 */

	'__construct': function(collections, group)
	{
		this._queue = collections.queue;
		// this._stats = collections.stats;
		this._failed = collections.failed;   
		this._idGen = cashew.register('queue');
		this._group = group;              
		this._queuedPops = [];

		//time to deteermine whether a job is stale
		this._staleTime = 1000 * 60 * 60; // N minutes
		
		this._queue.ensureIndex('jobId', function(){});    
		
		
		//reset after ever N minutes
		setInterval(this.getMethod('_resetStaleTasks'), 1000 * 60 * 10, 0);  
		this._resetStaleTasks(0); 
	},

	/**
	 */

	'push': function(job, callback)
	{                      
		                      
		        
		if(job._id)
		{
			this._queue.update({ _id: job._id }, { $set: { state: 'queued', data: job.data }}, callback || function(){});
		}
		else
		{   
			function insert()
			{           
				var toInsert = { group: self._group, 
					sendAt: job.sendAt, 
					data: job.data, 
					state: 'queued', 
					jobId: jobId,
					createdAt: Date.now() };

				self._queue.insert(toInsert, callback || function(){});
			}      

			//job id could be set, or unique                          
			var self = this,
			jobId = this._idGen.hash(this._group + (job.jobId || self._idGen.uid()));
                        
			//if a jobId already exists, then make sure it doesn't exist in the 
			//database - JUST update the data
			if(job.jobId)
			{
				this._queue.findOne({ jobId: jobId }, function(err, item)
				{
					if(item)
					{          
						var update = {};
						
						if(job.data) update.data = job.data;
						if(job.sendAt) update.sendAt = job.sendAt;
						          
                    	return self._queue.update({ _id: item._id }, update, callback || function(){})
					}          
					   
					insert();
					
				})
			}
			else
			{
				insert();
			}                                                                                   

                                                                                                                         
                                                                       
		}

	},

	/**
	 * pops queue items off the list (DUH)
	 */

	'pop': function(count, callback)
	{   
		if(!callback)
		{
			callback = count;
			count = 1;
		}                   
		
		if(this._popping)
		{                                       
			this._queuedPops.unshift(callback);
			return;
		}          
		
		this._popping = true;

		var self = this,
		now = Date.now();       
		                             

		this.next(now, function(err, job)
		{                           
			self._popping = false; 

			if(self._queuedPops.length) self.pop(self._queuedPops.pop());              
			
			if(!job) return callback(err, job);      
			                             
                                                                         
			                                                                    

			callback(err, job, function(response)
			{                            

				if(response.success || response.success == undefined)
				{
					//push back into the queue
					if(response.sendAt != undefined)
					{	                           
						var toUpdate = { sendAt: response.sendAt, state: 'queued', tries: 0, sentAt: 0 };


						//set new data...
						if(response.data) toUpdate.data = response.data;

						self._queue.update({ _id: job._id }, {$set: toUpdate }, function(){});          
						
						return;
					} 
				}
				else
				{
					var failure = {
						group: self._group,
						error: response.error || 'unknown',
						payload: job,
						exception: response.exception || 'generic',
						backtrace: response.backtrace || ['unknown'],
						failedAt: Date.now()	
					};

					self._failed.insert(failure, function(){});
				}


				self._queue.remove({ _id: job._id }, function(){});
			});
		});
	},

	/**
	 * returns the next job
	 */

	'next': function(since, callback)
	{
		if(!callback)
		{
			callback = since;
			since = undefined;
		}                                          
		
		this._queue.findAndModify(this._queuedQuery(since), [['sendAt', 1]], { $set: { state: 'running', sentAt: Date.now() }, $inc: { tries: 1 }, $inc: { executions: 1 } }, function(err, item)
		{                 
			callback(err, item);
		});  
	},   
	
	/**
	 */
	
	'nextTime': function(callback)
	{
		this._queue.find(this._queuedQuery(), { sort: [['sendAt', 1]], limit: 1 }, function(err, cursor)
		{          
			cursor.toArray(function(err, items)
			{            
				callback(err, items && items.length ? items[0].sendAt : null);
			})                                       
		});
	},

	/**
	 */

	'countQueued': function(since, callback)
	{                 
		if(!callback)
	  	{
			callback = since;
			since = undefined;
		}                  
		                              
			
		this._queue.find(this._queuedQuery(since), function(err, cursor)
		{                            
			cursor.count(function(err, num)
			{
				callback(num);
			})
		});
	}, 
	
	/**
	 */
	
   	'_queuedQuery': function(since)
	{
		var search = { group: this._group, state: 'queued' };

		if(since) search.sendAt = { $lt: since };
		
		return search;
	},

	/**
	 */

	'countRunning': function(callback)
	{
		
	},

	/**
	 */

	'_cleanStaleTasks': function() 
	{
		this._resetStaleTasks();
		this._removeFailedTasks();
	},

	/**
	 */

	'_resetStaleTasks': function(staleTime)
	{
		var search = { $or: [{ sentAt: undefined }, { sentAt: Date.now() - staleTime }] , state: 'running' },
		self = this;

		self._queue.find(search, function(err, cursor) 
		{
			cursor.count(function(err, count)
			{
				if(!count) return;

				console.warn('Resetting %d stale workers', count);

				self._queue.update(search, 
				{ $set: { sentAt: 0, state: 'queued' }}, { multi: true }, function(err, result) 
				{
				});
			});
				
		});
	},

	/**
	 */

	'_removeFailedTasks': function()
	{
		// this._queue.remove({ tries: { $gt: 5 }})
	}

})


module.exports = function(db)
{
	// var stats = db.collection('thyme.stats'),
	// failed = db.collection('thyme.failed');
	
	return {
		type: 'mongo',
		connect: function(group)
		{                   
			var col = group.replace(/\/+/g,'.');
			      
			return new Broker({
				queue: db.collection('thyme.queue.' + col),
				// stats: stats,
				failed: db.collection('thyme.failed.' + col)
			}, group);
		}
	}
}