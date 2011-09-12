var beanpole = require('beanpole').router();



beanpole.require(['hook.core','hook.http.mesh']);

		var i = 0, self = this;

function push()
{
	beanpole.push('add/thyme', { channel: 'say/hello' , data: { num: i++, _id: new Date().getTime()+'.'+i+'.'+Math.round(Math.random()*99999999999) }});
}

beanpole.on({
	'push -public thyme/ready': function()
	{
		console.log('ready!')
		push()
	},
	'pull -public -rotate say/hello': function()
	{
		console.log("%s: call %d",new Date().toString(), i);

		push();
		this.end();
	}
});

beanpole.push('init');
beanpole.push('set/id', new Buffer(process.argv.pop()).toString('base64'));
