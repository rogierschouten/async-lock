'use strict';

var assert = require('assert');
var fs = require('fs');
var path = require('path');
var vm = require('vm');
var source = fs.readFileSync(path.join(__dirname, '../lib/index.js'), 'utf8');

describe('Browser script', function () {
	it('loads without CommonJS and serializes asynchronous work', function () {
		var context = vm.createContext({ setTimeout: setTimeout, clearTimeout: clearTimeout });
		vm.runInContext(source, context);
		assert.strictEqual(typeof context.AsyncLock, 'function');
		var lock = new context.AsyncLock();
		var order = [];
		return Promise.all([
			lock.acquire('key', function () {
				return new Promise(function (resolve) {
					setTimeout(function () { order.push(1); resolve(); }, 5);
				});
			}),
			lock.acquire('key', function () { order.push(2); })
		]).then(function () {
			assert.deepStrictEqual(order, [1, 2]);
			assert.strictEqual(lock.isBusy(), false);
			assert.throws(function () { lock.acquire('key', null); }, context.AsyncLock.AsyncLockError);
			assert.throws(function () { return new context.AsyncLock({ domainReentrant: true }); }, /process.domain/);
		});
	});

	it('retains CommonJS error constructor identities', function () {
		var AsyncLock = require('../');
		var errors = require('../lib/errors');
		Object.keys(errors).forEach(function (name) {
			assert.strictEqual(AsyncLock[name], errors[name]);
		});
	});
});
