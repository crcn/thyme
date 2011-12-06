var broker = require('./broker');


exports.plugin = function(router) {
	
	router.on({

		/**
		 */

		'push mongodb': function(db) {
			router.push('thyme/use/broker', { broker: broker(db) });
		}
	});
}