/**
 * queues actions one after the other
 */

var Queue = require('./queue').Queue,
parseTime = require('./parseTime'),
vine = require('vine')


Date.prototype.toJSON = function()
{
	return this.getTime();
}
 
exports.plugin = function(m)
{

	var queue_c,
	info_c,
	ready = false,
	queues = {};

	//mem stored info of info
	info = {};

	function onDb(db)
	{

		queue_c = db.collection('pull.thyme'),
		info_c = db.collection('pull.thyme.info');

		//load the channel info so we know how many to call at a given time without asking the DB. this
		//should be a relatively low num
		loadInfo();
	}

	function loadInfo()
	{
		info_c.find(function(err, cursor)
		{
			cursor.each(function(err, item)
			{
				if(!item) return isReady();

				info[ _key(item) ] = item;
			});
		})
	}

	function isReady()
	{
		ready = true;
                                                        
		m.push('ready', 'thyme');

		//on init, call next immediately
		for(var key in info)
		{
			next(info[key]);
		}
	}


	/**
	 * adds an item to the queue
	 */

	function add(item, err, req)
	{

		if(!item) return;

		//target host: localhost, some website, w/e
		var host = item.host || '*',

		//scan.image PULL. queue needs to finish its job
		channel = item.channel,

		//data to push to the channel
		data = item.data,

		//the group this queue belongs to, like the group of processes. e.g: engadget.com
		//helps to better throttle queues and priorities based on speed of each group
		group = item.group,

		//when to send the call
		sendAt = new Date(item.sendAt),

		//the label for the queue. useful for logging if it fucks up.
		label = item.label || '',

		//the target group of queue handlers 
		target = item.target = req.from.id,

		//key so changes cna be made
		key = item.key || req.inner.key;


		if(!data._id) return console.error('added queue for channel %s where id does not exist', channel);

		if(!queue_c)
		{
			console.error("queue not ready. unable to add %s", channel);
			console.error(item);
			return;
		}
			

		data._id = data._id.toString();

		queue_c.findOne({ channel: channel, _qid: data._id, target: target }, function(err, cue)
		{
			//cue exists already
			if(cue) return next(cue);

			var inf = getInfo2(item, req);

			queue_c.insert({ host: host, 
			channel: channel, 
			_qid: data._id,
			data: data, 
			label: label,
			key: key,
			target: target,
			sendAt: sendAt || new Date(new Date().getTime() + inf.delay ) }, function()
			{
				next(item);
			});
		});
	}

	/**
	 */

	function getInfo(pull, err, req)
	{
		return getInfo2(pull.data, req);
	}

	/**
	 */

	function getInfo2(item, req)
	{
		if(!info[_key(item)])
		{
			setInfo(item, null, req);
		}

		return info[_key(item)];
	}

	function _key(item)
	{
		return item.channel + (item.target || '');
	}

	/**
	 * sets the burst for a given call. Burts are the number of concurrent
	 * pulls to a given channel
	 */


	function setInfo(item, err, req)
	{
		//cannot add null channel
		if(!item.channel) return;

		console.success('Setting queue info for %s target client %s', item.channel, req.from.id );


		var toSet = {

			channel: item.channel,

			//max number of concurrent queues of a given channel
			max: item.num || item.max || 1,

			//max number of tries after fail before we stop sending the queue
			maxTries: item.maxTries || 3,

			//timeout before we kill it
			timeout: item.timeout || 20000,

			//lock until we retry
			lockTTL: item.lockTTL || 1000 * 60 * 30,

			//delay to send the queue. -1 = immediate
			delay: -1,

			//description about the queue so we can pretty print it
			name: item.name,

			//the description of the item
			description: item.description,
				
			//how the queue is ordered: seq, group
			//based on pulled statistics 
			order: item.order,

			//send ONLY to the apps with these id's 
			target: req.from.id
		}

		info_c.update({ channel: item.channel, target: toSet.target }, { $set: toSet }, { upsert: true }, function() { } );

		info[ _key(toSet) ] = toSet;

		next(toSet)
	}

	/**
	 * next call
	 */

	function next(item)
	{
		var key = _key(item);

		var inf = info[key];


		if(!inf || !m.has(item.channel, { type: 'pull' })) return;

		(queues[key] || (queues[key] = new Queue(m, queue_c, inf ))).next();
	}

	/**
	 */

	function getSchedule(request)
	{
		queue_c.find({ key: request.data.key }, function(err, cursor)
		{
			cursor.toArray(function(err, items)
			{
				vine.result(items).end(request);//request.end(item);
			})
		});
	}

	function updateSchedule(request)
	{
		var d = this.data;


		queue_c.findOne({ _id: d._id, key: d.key }, function(err, cue)
		{
			//cue exists already
			if(!cue) return vine.error('scheduled item does not exist').end(request);


			var toUpdate = {};

			if(d.sendAt) toUpdate = cue.sendAt = new Date(d.sendAt) || new Date();
		
			if(d.data)
			{
				for(var prop in d.data)
				{
					toUpdate['data.'+prop] = cue.data[prop] = d.data[prop];
				}
			}

			for(var prop in d)
			{
				if(prop.substr(0,5) == 'data.') toUpdate[prop] = cue.data[prop.substr(5)] = d[prop];
			}


			queue_c.update({ _id: d._id }, {$set:toUpdate}, function(err, item)
			{
				vine.result(cue).end(request)
			});
		});
	}

	function updateSchedule(request)
	{
		var d = this.data;


		queue_c.findOne({ _id: d._id, key: d.key }, function(err, cue)
		{
			//cue exists already
			if(!cue) return vine.error('scheduled item does not exist').end(request);


			var toUpdate = {};

			if(d.sendAt) toUpdate = cue.sendAt = new Date(d.sendAt) || new Date();
		
			if(d.data)
			{
				for(var prop in d.data)
				{
					toUpdate['data.'+prop] = cue.data[prop] = d.data[prop];
				}
			}

			for(var prop in d)
			{
				if(prop.substr(0,5) == 'data.') toUpdate[prop] = cue.data[prop.substr(5)] = d[prop];
			}


			queue_c.update({ _id: d._id }, {$set:toUpdate}, function(err, item)
			{
				vine.result(cue).end(request)
			});
		});
	}

	function removeSchedule(request)
	{
		var d = this.data;

		queue_c.remove({ _id: d._id, key: d.key }, function(err, cue)
		{
			// console.log(cue)
			return vine.result(true).end(request);
		});
	}




	m.on({

		/**
		 */

		'push -pull mongodb': onDb,

		/**
		 */

		'push -public add/thyme': add,

		/**
		 */

		'push -public set/thyme/info': setInfo,

		/**
		 */

		'pull -public -rotate get/thyme/info': getInfo,

		/**
		 */

		'pull -public -rotate thymes': getSchedule,

		/**
		 */

		'pull -public -rotate update/thyme': updateSchedule,


		/**
		 */

		'pull -public -rotate remove/thyme': removeSchedule

	});
}