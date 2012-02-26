var redis = require('redis'),
Structr   = require('structr'),
_         = require('underscore'),
cashew    = require('cashew'),
outcome   = require('outcome'),
step      = require('stepc'),
logger    = require('winston').loggers.get('thyme');



var Broker = Structr({
	
	/**
	 */

	'__construct': function(collections, group) {

		this._queue      = collections.queue;
		this._failed     = collections.failed;   
		this._idGen      = cashew.register('queue');
		this._group      = group;              
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

	'push': function(task, callback) {  
	
		if(!callback) callback = function(){};                    
		                   
		if(task._id) {

			this._queue.update({ _id: task._id }, { $set: { state: 'queued', data: task.data }}, callback);

			return;

		}
			     

		//job id could be set, or unique                          
		var self = this,
		jobId = this._idGen.hash(this._group + (task.jobId || self._idGen.uid()));

		function insert() {      

			logger.verbose('insert task');
		     
			var toInsert = { group: self._group, 
				sendAt: task.sendAt, 
				data: task.data, 
				state: 'queued', 
				jobId: jobId,
				createdAt: Date.now() };

			self._queue.insert(toInsert, callback);

		} 
                    
		//if a jobId already exists, then make sure it doesn't exist in the 
		//database - JUST update the data
		if(task.jobId) {

			logger.verbose('update job');

			this._queue.findOne({ jobId: jobId }, function(err, item) {
				if(item) {          
					var update = {};
					
					if(task.data) update.data = task.data;
					if(task.sendAt) update.sendAt = task.sendAt;
					        
					       
					console.log('Updating queue %s', jobId);

                	return self._queue.update({ _id: item._id }, { $set: update }, callback)
				}          
				   
				insert();
				
			})
		} else {
			insert();
		}      
	},

	/**
	 * pops queue items off the list (DUH)
	 */

	'pop': function(callback) {   


		//caught in the middle of popping? make it wait incase the same item is popped off.
		if(this._popping) {                                       
			this._queuedPops.unshift(callback);
			return;
		}          
		
		this._popping = true;

		var self = this,
		now      = Date.now(),
		on       = outcome.error(callback),
		runningTask;
		
		
		step(

			/**
			 * fetch the next job
			 */

			function() {

				self.next(now, this);

			},

			/**
			 */

			on.success(function(task) {

				self._popping = false;

				if(self._queuedPops.length) self.pop(self._queuedPops.pop()); 
				   

				this(null, task);
			}),

			/**
			 * run the job
			 */

			on.success(function(task) {


				if(!task) return callback(err, task);  

				runningTask = task;

				callback(null, task, this);
			}),

			/**
			 * task complete
			 */

			outcome.error(function(err) {

				var failure = {
					group     : self._group,
					message   : err.message || 'unknown',
					payload   : runningTask,
					failedAt  : Date.now(),
					stack     : err.stack
					// exception : response.exception || 'generic',
					// backtrace : response.backtrace || ['unknown']
				};

				// self._failed.insert(failure, function(){});

				this();

			}).

			/**
			 */

			success(function(response) {

					
				//resend the item at a later time? don't remove it them!
				if(response.sendAt != undefined) {

					logger.verbose('resending task for a later time');

					var toUpdate = { sendAt: response.sendAt, state: 'queued', tries: 0, sentAt: 0 };


					//set new data...
					if(response.data) toUpdate.data = response.data;


					self._queue.update({ _id: runningTask._id }, { $set: toUpdate }, function() { });          
					
					return;
				}

				this();
			}),

			/**
			 * done? remove the job.
			 */

			function() {

				self._queue.remove({ _id: runningTask._id }, function(){});

			}
		);
	},

	/**
	 * returns the next task
	 */

	'next': function(since, callback) {

		if(!callback) {

			callback = since;
			since = undefined;

		}        
		
		var query = {
			$set: {
				state: 'running',
				sentAt: Date.now()
			},
			$inc: {
				tries: 1,
				executions: 1
			}
		};                     
		
		this._queue.findAndModify(this._queuedQuery(since), [['sendAt', 1]], query, function(err, item) {  
		               
			callback(err, item);

		});  
	},   
	
	/**
	 */
	
	'nextTime': function(callback) {

		this._queue.find(this._queuedQuery(), { sort: [['sendAt', 1]], limit: 1 }, function(err, cursor) {     
		     
			cursor.toArray(function(err, items) {           
			 
				callback(err, items && items.length ? items[0].sendAt : null);

			});
			                                     
		});

	},

	/**
	 */

	'countQueued': function(since, callback) {  
	               
		if(!callback) {
			callback = since;
			since = undefined;
		}                  
		                              
			
		this._queue.find(this._queuedQuery(since), function(err, cursor) {
		                            
			cursor.count(function(err, num) {

				callback(num);

			});

		});

	}, 
	
	/**
	 */
	
   	'_queuedQuery': function(since) {

		var search = { group: this._group, state: 'queued' };

		if(since) search.sendAt = { $lt: since };
		
		return search;

	},

	/**
	 */

	'countRunning': function(callback) {
		
	},

	/**
	 */

	'_cleanStaleTasks': function() {

		this._resetStaleTasks();
		this._removeFailedTasks();

	},

	/**
	 */

	'_resetStaleTasks': function(staleTime) {

		var search = { $or: [{ sentAt: undefined }, { sentAt: {$lt:  Date.now() - staleTime } }] , state: 'running' },
		self = this;



		self._queue.find(search, function(err, cursor) {

			cursor.count(function(err, count) {

				if(!count) return;

				console.warn('Resetting %d stale workers', count);

				self._queue.update(search,  { $set: { sentAt: 0, state: 'queued' }}, { multi: true }, function(err, result) {

				});

			});
				
		});
	},

	/**
	 */

	'_removeFailedTasks': function() {
		// this._queue.remove({ tries: { $gt: 5 }})
	}

})


module.exports = function(db)
{
	// var stats = db.collection('thyme.stats'),
	// failed = db.collection('thyme.failed');
	
	return {
		type: 'mongo',
		connect: function(group) {                   
			var col = group.replace(/\/+/g,'.');
			      
			return new Broker({
				queue: db.collection('thyme.queue.' + col),
				// stats: stats,
				failed: db.collection('thyme.failed.' + col)
			}, group);
		}
	}
}
