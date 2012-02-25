
require('colors')

var beanie = require('beanie'),
loader = beanie.loader();



loader.
paths(__dirname + '/beans').
paths(__dirname + '/../node_modules').
require({
	"bean.database.mongo": {
        "host": "localhost",
        "database": "spiceio"
    },
    "thyme.broker.mongo": true,
    "daisy": {
        "name": "thyme",
        "transport": {
            "rabbitmq": {
                "host": "localhost"
            }
        }
    },
    "thyme.core": true
}).
load();



