var Workers = require('./workers');

exports.plugin = function(router, params)
{	

	//no broker? default = redis
	if(!params.broker) params.broker = 'mongo';

	//brokers: redis, mongo, 0mq, etc.
	var broker,
	collections = {};


	function workers(target)
	{
		return collections[target] || (collections[target] = new Workers(target, broker, router));
	}


	router.on({
		

		/**
		 */

		'push thyme/use/broker': function(data)
		{
			broker = data.broker;

			router.push('ready','thyme');
		},


		/**
		 * adds a queue to thyme
		 */

		'push -public thyme/enqueue OR thyme/job': function(job)
		{
			console.log('Adding job');  
			
			if(!job.channel) return console.warn('Job was added without providing a channel');             
			
			var worker = workers(this.from.id).worker(job.channel);

			if(worker)
			{
				worker.addJob(job);
			}
		},

		/**
		 * adds a worker for thyme to callback to.
		 */
		
		'push -public thyme/worker': function(worker)
		{
			console.log("Adding worker options");

			workers(this.from.id).worker(worker.channel, worker).run();
		}
	})
}