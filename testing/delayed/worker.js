var plugin = require('beanie'),
loader = plugin.loader();

loader.
paths(__dirname + "/../../node_modules").
require({
	daisy: {
		name: 'thyme-worker',
		transport: {
			rabbitmq: {
				host: 'localhost'
			}
		}
	}
})


              
                                    
loader.router.on({
	
	'pull -hook hello/worker': function(req, res)
	{                                     
		console.log(req.query)                   
		
		res.end({ sendAt: Date.now() + 1000 });
	}                       

});                                  
         

loader.load();                  
