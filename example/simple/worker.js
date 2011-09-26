var beanpole = require('beanpole'),
router = beanpole.router();

router.require('hook.core','hook.http.mesh');
                                    
router.on({
	
	'pull -rotate -public hello/worker': function(request)
	{                                                        
		console.log('Working');                  
		
		console.log(request.data);                                
		
		return { sendAt: Date.now() + 300, data: request.data + 'h' };
	}                            
});                                  

router.push('ready', 'worker');  
router.push('set/id', 'test');
router.push('init');