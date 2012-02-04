
require('colors')

var beanpoll = require('beanpoll'),
router = beanpoll.router(),
haba =  require('haba'),
pluginLoader = haba.loader();



pluginLoader.
options(router, true).
paths(__dirname + '/beans').
paths(__dirname + '/../node_modules').
require({
	"bean.database.mongo": {
        "host": "localhost",
        "database": "spiceio"
    },
    "thyme.broker.mongo": true,
    "daisy": {
        "remoteName": "thyme",
        "transport": {
            "rabbitmq": {
                "host": "localhost"
            }
        }
    },
    "thyme.core": true
}).
load(function() {
    router.push('init');
});



