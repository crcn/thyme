var Structr = require('structr');


var Queue = Structr({
	
	/**
	 */

	'__construct': function(mediator, collection, ops)
	{
		this._mediator = mediator;
		this._collection = collection;
		this._ops = ops;
		this._used = 0;
		this._running = 0;


		//incase there are timed queue items, need to check periodically to make sure
		//they're available
		var self = this;

		setInterval(function()
		{
			self.next();
		}, 10000);
	},

	/**
	 */

	'next': function()
	{
		var self = this;


		if(this._running == this._ops.max || this._locked) return;

		this._locked = true;


		this._collection.find({ channel: this._ops.channel, cluster: this._ops.cluster, sendAt: { $lt: new Date() } }, function(err, cursor)
		{
			cursor.count(function(err, n)
			{
				if(!n) return self._locked = false;

				if(self._pn != n) console.ok('%d items queued in "%s"', n, self._ops.channel);

				self._pn = n
				self._next();
			})
		});

				
	},

	/**
	 */

	'_next': function()
	{
		var self = this,
			col = this._collection,
			now = new Date();


		col.find({ channel: this._ops.channel, cluster: this._ops.cluster, sendAt: { $lt: now } }, { limit: this._ops.max - this._running }, function(err, cursor)
		{
			cursor.toArray(function(err, items)
			{
				var ids = [];


				items.forEach(function(item)
				{
					ids.push(item._id);
				});

				//cache for one our
				now.setTime(now.getTime() + self._ops.lockTTL);

				//update sendAt incase things break
				col.update({ _id: { $in: ids }}, { $set: { sendAt: now }}, { multi: true }, function()
				{
					self._locked = false;

					items.forEach(function(item)
					{
						self._running++;
						self._used++;
						self._call(item);
					});
				});
			});	
		});	
	},

	'_call': function(item)
	{
		var self = this,
		nexted = false;


		function next(response)
		{
			if(nexted) return;
			nexted = true;

			self._running--;

			clearTimeout(killTimeout);

			var tooManyTries = self._ops.maxTries != -1 && item.tries < self._ops.maxTries;

			if(response === true && !tooManyTries)
			{	
				//give it another shot later...
				self._collection.update({ _id: item._id }, {$inc: { tries: 1 }}, self.getMethod('next'));
			}
			else

			//update the queue to be called next time
			if(response && (typeof response == 'object'))
			{
				var toSet = { tries: 0 };

				if(response.sendAt) toSet.sendAt = new Date(response.sendAt);
				if(response.data) toSet.data = response.data;

				self._collection.update({ _id: item._id }, {$set: toSet, $inc: { loads: 1 } }, self.getMethod('next') );
			}

			//nothing
			else
			{
				if(tooManyTries)
				{
					console.warn('the item %s in channel %s in cluster %s was removed because there have been too many attempts to process it', item.label, self._ops.channel, self._ops.cluster)	
				}

				self._collection.remove({ _id: item._id }, self.getMethod('next'));
			}
		}

		var killTimeout = setTimeout(function()
		{
			next(true);

			console.warn('channel queue %s for %s in cluster %s is taking too fucking long, skipping', self._ops.channel, item.label, self._ops.cluster);

		}, self._ops.timeout);

		if(item.data)
		{
			item.data.loads = item.loads;
		}

		this._mediator.pull(item.channel, item.data, { meta: { cluster: item.cluster } }, next);
	}
});

exports.Queue = Queue;