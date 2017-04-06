/*
* @Author: dverma
* @Date:   2016-04-11 23:16:19
* @Last Modified by:   dverma
* @Last Modified time: 2016-09-29 12:19:47
*/

'use strict';

const messageBus = require('./lib');
messageBus.version = require('./package.json').version;

module.exports = messageBus;
