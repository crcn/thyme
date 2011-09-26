var broker = require('./broker');


exports.plugin = function(router)
{
	
	router.on({
		
		/**
		 */


		'push init': function()
		{
			router.push('thyme/use/broker', { broker: broker() });
		}
	});
}