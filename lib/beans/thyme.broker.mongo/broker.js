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
		
		this._queue.ensureIndex('jobId', function(){});       
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
				var toInsert = { group: self._group, sendAt: job.sendAt, data: job.data, state: 'queued', jobId: jobId };
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
                    	return self._queue.update({ _id: item._id }, { data: job.data }, callback || function(){})
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
						var toUpdate = { sendAt: response.sendAt, state: 'queued' };


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
		
		this._queue.findAndModify(this._queuedQuery(since), [['sendAt', 1]], { $set: { state: 'running' }}, function(err, item)
		{                 
			callback(err, item);
		})
             
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