var beanpoll = require('beanpoll'),
haba = require('haba'),
router = beanpoll.router(),
loader = haba.loader();

loader.
options(router, true).
require({
	daisy: {
		remoteName: 'thyme-worker',
		transport: {
			rabbitmq: {
				host: 'localhost'
			}
		}
	}
}).init();
                                    
router.on({
	
	'pull -hook hello/worker': function(req, res)
	{                                     
		console.log(req.query)                   
		
		res.end({ sendAt: Date.now() + 500, data: Math.random() });
	},
	
	'push -hook thyme-test/ready': function() {
		console.log('server is up')
	},
	
	'push -hook thyme/ready': function() {
		console.log('thyme is up')
	}                         

});                                  
                                  
router.push('init');
