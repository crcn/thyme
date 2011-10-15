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
	
	'pull -rotate -public hello/worker': function(request)
	{                                                        
		console.log(request.data);            
		
		if(request.data.length > 50) return true;
		
		return { sendAt: Date.now() + 1, data: request.data + 'h' };
	}                            
});                                  
                                  
router.push('init');