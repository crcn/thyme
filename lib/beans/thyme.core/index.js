var Workers = require('./workers');

exports.plugin = function(router, params)
{	

	//no broker? default = redis
	if(!params.broker) params.broker = 'mongo';

	//brokers: redis, mongo, 0mq, etc.
	var broker,
	collections = {};


	function allWorkers(target)
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
			if(!job.queue) return console.warn('Unable to add job without target queue');
			if(!job.channel) return console.warn('Job was added without providing a channel');             
			
			var worker = allWorkers(job.queue).worker(job.channel);

			if(worker)
			{
				worker.addJob(job);
			}
		},

		/**
		 * adds a worker for thyme to callback to.
		 */
		
		'push -public thyme/worker': function(workers)
		{
			if(!(workers instanceof Array)) workers = [workers];      
			                           
			
			
			workers.forEach(function(workerInfo)
			{                                 
				if(!workerInfo.queue) return console.error('Unable to add worker %s without queue', workerInfo.channel);
				
				console.log("Adding worker options for %s", workerInfo.channel);

				var worker = allWorkers(workerInfo.queue).worker(workerInfo.channel, workerInfo);
				
				worker.run();
				
				router.on('push ' + workerInfo.queue + '/ready', function()
				{
					console.log('Queue %s is ready, running worker', workerInfo.queue);
					
					worker.run();
				})
			})
		}
	})
}