var beanpole = require('beanpole'),
router = beanpole.router(),
celeri = require('celeri');

require( __dirname + '/../../node_modules/daisy').plugin(router, {
   	name: 'thyme-test',
	transport: {
		rabbitmq: {
			host: 'localhost'
		}
	}
});

                                    
router.on({ 
	
	/**
	 */
	
	'push -public thyme/ready': function()
	{
		console.log('Thyme is ready. Fire up the worker!');
		
		this.from.push('thyme/worker', { queue: 'thyme-worker', max: 20, channel: '/hello/workerr' });
	},
	
	/**
	 */ 
	
	'push -public worker/ready': function()
	{
		console.log('Worker ready! Starting to work. Start typing some stuff to blast off (e.g: message "hello world!")');
		                                                                                  
	}
	
});

celeri.on({
   
	/**
	 */                                                                                          
	
   	'message :message OR message :message :timeout': function(data)
	{                                                                                                                      
		router.push('thyme/enqueue', { _id: 'hello-test', queue:'thyme-worker', channel: '/hello/workerr', data: data.message, sendAt: Date.now() + (Number(data.timeout) || 0) });
	}
});  


celeri.open();                   
router.push('init');