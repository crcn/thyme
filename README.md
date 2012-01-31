What's this?
------------

A queue / cron manager for beanpole based apps, kinda like cron jobs. 

What can it do?
---------------

- Register a request to call at a specific time.
- Register a ton of calls at an unspecified time, and thyme will send them immediately, one after the other.
- If a request fails, you can specify the time to try and make the request again, and the number of times to send the request before disposing it.
- You can specifed a number of concurrent requests to make at any given time. 



## Requirements

- node.js
- rabbitmq
- haba
- daisy


## Usage

Startup the thyme server:

	thyme

In your **master** server:

````javascript
var router = require('beanpoll').router(),
loader = require('haba').loader();

loader.require({
	daisy: {
		remoteName: 'app-master',
		transport: {
			rabbitmq: 'localhost'
		}
	}
});

router.on({
	
	'push -hook thyme/ready': function() {
		
		this.from.push('thyme/worker', { channel: 'do/work', queue: 'app-slave' });

	},

	'push -hook app-slave/ready': function() {
		
		router.push('thyme/enqueue', { queue:'app-slave', channel: 'do/work', data: data.message, sendAt: Date.now() + cron.timeout('* * * * * *') });
		
	}
});

````

In your **slave** server (worker):

```javascript
var router = require('beanpoll').router(),
loader = require('haba').loader(),
cron = require('cron');

loader.require({
	daisy: {
		remoteName: 'app-slave',
		transport: {
			rabbitmq: 'localhost'
		}
	}
});

router.on({
	
	'pull -hook do/work': function(data) {
		
		//re-add the job with NEW data N seconds from now
		res.end({ sendAt: Date.now() + 1000, data: 'new data' });
	}
});
```


To Do:
------

- ordering queue based on request/response speed. 
- need to check if call exists before making it.
- don't send queues of handler is not present.
