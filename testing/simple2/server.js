var beanpole = require('beanpole'),
router = beanpole.router();
                      

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
		
		this.from.push('thyme/worker', { queue: 'thyme-worker', max: 20, path: '/hello/worker' });
	},
	
	/**
	 */ 
	
	'push -public thyme-worker/ready': function()
	{                                                                                            
		var i = 0;
		        
		//start feeding the queue some data  
		setInterval(function()
		{       
			router.push('thyme/enqueue', { queue:'thyme-worker', path: '/hello/worker', data: i++, sendAt: Date.now()  });
		}, 1);                                                                                                                                                                                                                                     
	}
	
});
                                
router.push('init');