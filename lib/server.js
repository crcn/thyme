
require('colors')

var beanie = require('beanie'),
loader = beanie.loader();



loader.
paths(__dirname + '/beans').
paths(__dirname + '/../node_modules').
require({
	"bean.database.mongo": {
        "host": process.env.MONGO_HOST || "localhost",
        "database": process.env.MONGO_DATABASE || "spiceio"
    },
    "thyme.broker.mongo": true,
    "daisy": {
        "name": "thyme",
        "transport": {
            "rabbitmq": {
                "host": process.env.RABBITMQ_HOST ||"localhost"
            }
        }
    },
    "thyme.core": true
}).
load();



