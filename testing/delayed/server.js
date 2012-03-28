var plugin = require('beanie'),
loader = plugin.loader();

loader.
paths(__dirname + "/../../node_modules").
require({
	daisy: {
		name: 'thyme-blarg',
		transport: {
			rabbitmq: {
				host: 'localhost'
			}
		}
	}
});


                                    
loader.router.on({ 

	/**
	 */

	'push -hook thyme/ready': function() {
		loader.router.push('thyme/throttle', { queue: 'thyme-worker' });
	},
	
	
	/**
	 */ 
	
	'push -hook thyme-worker/ready': function()
	{
		console.log('Worker ready! Starting to work. Start typing some stuff to blast off (e.g: message "hello world!")');
		    
		var timeout = Date.now() + 1000;


		for(var i = 1; i--;)
		loader.router.push('thyme/job', {
			path: 'hello/worker',
			queue: 'thyme-worker',
			_id: 'fsdfsdfs',
			sendAt: timeout,// + (i * 1000),
			data: { i: "BLAH" }
		});                                                                         
	}
	
});


     

loader.load();          