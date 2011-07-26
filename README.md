What's this?
------------

A queue / time manager for beanpole based apps, kinda like cron jobs. 

What can it do?
---------------

- Register a request to call at a specific time.
- Register a ton of calls at an unspecified time, and thyme will send them immediately, one after the other.
- If a request fails, you can specify the time to try and make the request again, and the number of times to send the request before disposing it.
- You can specifed a number of concurrent requests to make at any given time. 


What's with the name?
---------------------

Do I *really* need to explain this??

Requirments:
------------

- NPM
- beet

Installation:
-------------

	npm install thyme


Code Usage:
-----------

In another beanpole app:

```javascript


exports.plugin = function(mediator)
{
	
	mediator.on({
		'push -pull thyme.ready': function()
		{
			//tries = number of tries before killing
			//max = max number of concurrent
			mediator.push('set.thyme.info', { channel: 'load.heavy.http.scraper', tries: 5, max: 50 });



		},
		'pull load.heavy.http.scraper': function(pull)
		{
			//do heavy stuff..

			//this lets thyme know we're done
			pull.end();
		}
	})



	function scrapeContent(site)
	{
		//ID must be present so there's no dupes
		mediator.push('thyme.add', { channel: 'load.heavy.http.scraper', data: { _id: site, v:site }});
	}

	///code below...
}

```


To Do:
------

- ordering queue based on request/response speed. 
- need to check if call exists before making it.