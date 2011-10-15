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
		
		//simulate lagging worker                
		var ttl = request.data % 100 ? Math.random() * 1000 : 20000;          
		                        
		                                  
		setTimeout(function()
		{             
			request.end({ message: 'success!' });
		}, 100 + ttl)                                       
		
		// return { sendAt: Date.now() + 300 };
	}                            
});                                  

router.push('ready', 'worker');  
router.push('set/id', 'test');
router.push('init');