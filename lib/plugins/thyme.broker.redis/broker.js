var redis = require('redis'),
Structr = require('structr');

var Broker = Structr({
	
	/**
	 */

	'__construct': function(client, group) {
		this._client = client;
		this._group = group;
	},

	/**
	 */

	'push': function(job, callback) {
		this._client.lpush( this._key('queue'), JSON.stringify(job), callback || function(){});
	},

	/**
	 */

	'pop': function(callback) {
		var self = this;

		//backup the job incase we go down...
		this._client.rpoplpush( this._key('queue'), this._key('locked'), function(err, jobStr) {
			var job = JSON.parse(jobStr);
			
			//return something that expects a response
			callback(false, {
				data: job,
				done: function(success) {
					//remove from the locked position
					self._client.lrem( self._key('locked'), -1, data);


					//self._client.incr(self._key('success'));
				}
			})
		});
	},

	/**
	 * number of succeeded jobs
	 */

	'succeeded': function() {
		
	},

	/**
	 * number of failed jobs
	 */

	'failed': function() {
		
	},

	/**
	 * length of current queue
	 */

	'length': function() {
		
	},

	/**
	 */

	'_key': function(name) {
		return this._group + ':' + name;
	}
})


module.exports = function(ops) {

	var client = redis.createClient();

	
	return {
		type: 'redis',
		connect: function(group) {
			return new Broker(client, group);
		}
	}
}