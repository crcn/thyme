/**
 * queues actions one after the other
 */

var Queue = require('./queue').Queue;


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

	function add(item, req)
	{
		if(!item) return;

		//target host: localhost, some website, w/e
		var host = item.host,

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
		label = item.label,

		//the target group of queue handlers 
		cluster = item.cluster = req.from.cluster || '';


		if(!data._id) return console.error('added queue for channel %s where id does not exist', channel);

		if(!queue_c)
		{
			console.error("queue not ready. unable to add %s", channel);
			console.error(item);
			return;
		}
		

		queue_c.findOne({ channel: channel, queueId: data._id, cluster: cluster }, function(err, cue)
		{
			//cue exists already
			if(cue) return;

			var inf = getInfo2(item, req);


			queue_c.insert({ host: host || '*', 
			channel: channel, 
			queueId: data._id,
			data: data, 
			label: label,
			cluster: cluster,
			sendAt: sendAt || new Date(new Date().getTime() + inf.delay ) }, function()
			{
				next(item);
			});
		});
	}

	/**
	 */

	function getInfo(pull, req)
	{
		pull.end(getInfo2(pull.data, req));
	}

	/**
	 */

	function getInfo2(item, req)
	{
		if(!info[_key(item)])
		{
			setInfo(item, req);
		}

		return info[_key(item)];
	}

	function _key(item)
	{
		return item.channel + (item.cluster || '');
	}

	/**
	 * sets the burst for a given call. Burts are the number of concurrent
	 * pulls to a given channel
	 */


	function setInfo(item, req)
	{
		//cannot add null channel
		if(!item.channel) return;

		console.success('Setting queue info for %s in cluster %s', item.channel, req.from.cluster );

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
			cluster: req.from.cluster || ''
		}

		info_c.update({ channel: item.channel, cluster: toSet.cluster }, { $set: toSet }, { upsert: true }, function() { } );

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


	m.on({
		//used for remote connections
		'push -pull mongodb': onDb,
		'push -public add/thyme': add,
		'push -public set/thyme/info': setInfo,
		'pull -public -rotate get/thyme/info': getInfo
	});
}