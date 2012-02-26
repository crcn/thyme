var Workers = require('./workers'),
logger      = require('winston').loggers.get('thyme'),
sprintf     = require('sprintf').sprintf;

exports.plugin = function(router, params) {	

	//no broker? default = redis
	if(!params.broker) params.broker = 'mongo';

	//brokers: redis, mongo, 0mq, etc.
	var broker,
	collections = {};



	function allWorkers(target) {

		return collections[target] || (collections[target] = new Workers(target, broker, router));

	}


	router.on({

		/**
		 */

		'push -hook thyme/throttle': function(data) {
			logger.info(sprintf('throttling %s', data.queue));
			logger.info(sprintf('max concurrent jobs: %d', data.max));
			logger.info(sprintf('job timeout: %d', data.timeout));
			allWorkers(data.queue).throttle(data);
		},
		

		/**
		 */

		'push thyme/use/broker': function(data) {
			broker = data.broker;

			router.push('ready','thyme');
		},

		/**
		 * adds a queue to thyme
		 */

		'push -hook thyme/enqueue OR thyme/job': function(job) {

			if(!job.queue) return console.warn('Unable to add job without target queue');
			if(!job.path) return console.warn('Job was added without providing a path'); 

			console.log('Adding thyme job');            

			var worker = allWorkers(job.queue).worker(job.path);
			if(worker) {


				worker.addJob(job);

			}
		},

		/**
		 * adds a worker for thyme to callback to.
		 */
		
		'push -hook thyme/worker': function(workers) {

			if(!(workers instanceof Array)) workers = [workers];      
			                         
			workers.forEach(function(workerInfo) {     
			                            
				if(!workerInfo.queue) return console.error('Unable to add worker %s without queue', workerInfo.path);
				
				console.log("Adding worker options for %s", workerInfo.path);

				var worker = allWorkers(workerInfo.queue).worker(workerInfo.path, workerInfo);
				
				worker.run();

			});

		}
	})
}