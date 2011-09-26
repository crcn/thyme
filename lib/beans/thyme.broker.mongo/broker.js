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
		this._stats = collections.stats;
		this._failed = collections.failed;   
		this._idGen = cashew.register('queue');
		this._group = group;     
		
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
			//job id could be set, or unique                          
			var jobId = this._idGen.hash(this._group + (job.jobId || this._idGen.uid()));
			
			var toInsert = { group: this._group, sendAt: job.sendAt, data: job.data, state: 'queued', jobId: jobId };    
			
			this._queue.insert(toInsert, callback || function(){});
		}

	},

	/**
	 * pops queue items off the list (DUH)
	 */

	'pop': function(callback)
	{

		var self = this,
		now = Date.now();

		this.next(now, function(err, job)
		{
			
			if(!job) return callback(err, job);      
			                             

			self._queue.update({ _id: job._id }, { $set: { state: 'running' }}, function()
			{
				//set a callback function that triggers completion
				job.done = function(response)
				{                            

					if(response.success)
					{
						//push back into the queue
						if(response.sendAt)
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
				}

				callback(err, job);
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
                                                   
		this._queue.find(this._queuedQuery(since), { sort: [['sendAt', 1]], limit: 1 }, function(err, cursor)
		{          
			cursor.toArray(function(err, items)
			{            
				callback(err, items ? items[0] : items);
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
	var queue = db.collection('thyme.queue'),
	stats = db.collection('thyme.stats'),
	failed = db.collection('thyme.failed');
	
	return {
		type: 'mongo',
		connect: function(group)
		{
			return new Broker({
				queue: queue,
				stats: stats,
				failed: failed
			}, group);
		}
	}
}