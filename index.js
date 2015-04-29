require('coffee-script/register');
require('chai').should();
require('colors');

// export
module.exports = require('./source/exchange');
module.exports.version = require('./package.json').version;
