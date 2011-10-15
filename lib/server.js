require('./index'); 
require('colors')


var beanpole = require('beanpole'),
router = beanpole.router();
                                      
require.paths.unshift(__dirname + '/../node_modules');

router.require(__dirname + '/../package.json').
require('thyme.core');              

router.push('init');
