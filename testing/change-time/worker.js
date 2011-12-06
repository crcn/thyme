var beanpole = require('beanpole'),
router = beanpole.router();


require( __dirname + '/../../node_modules/daisy').plugin(router, {
   	name: 'thyme-worker',
	transport: {
		rabbitmq: {
			host: 'localhost'
		}
	}
});
                                    
router.on({
	
	'pull -rotate -public hello/workerr': function(request)
	{                                                        
		console.log(request.data);            
		
		return { sendAt: Date.now() + 3000, data: request.data + 'h' };
	}                            
});                                  
                                  
router.push('init');
