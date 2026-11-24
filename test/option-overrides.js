'use strict';

var assert = require('assert');
var AsyncLock = require('../');

describe('Per-acquisition zero options', function () {
	it('rejects queuing with maxPending zero without changing the instance limit', function () {
		var lock = new AsyncLock({ maxPending: 2 });
		var release;
		var rejected;
		var ran = false;
		lock.acquire('key', function (cb) { release = cb; }, function (err) {
			assert.ifError(err);
		});
		lock.acquire('key', function (cb) {
			ran = true;
			cb();
		}, function (err) {
			rejected = err;
		}, { maxPending: 0 });
		release();
		assert(rejected instanceof AsyncLock.AsyncLockMaxPendingError);
		assert.strictEqual(ran, false);
		assert.strictEqual(lock.maxPending, 2);
		return lock.acquire('key', function () { return 42; }, { maxPending: 0 })
			.then(function (value) { assert.strictEqual(value, 42); });
	});

	it('disables the default queue timeout with a zero override', function () {
		var lock = new AsyncLock({ timeout: 10 });
		var holder = lock.acquire('key', function (cb) { setTimeout(cb, 40); });
		var queued = lock.acquire('key', function () { return 'acquired'; }, { timeout: 0 });
		return Promise.all([holder, queued]).then(function (values) {
			assert.strictEqual(values[1], 'acquired');
			assert.strictEqual(lock.timeout, 10);
		});
	});

	['maxExecutionTime', 'maxOccupationTime'].forEach(function (option) {
		it('disables the default ' + option + ' with a zero override', function () {
			var defaults = {};
			var override = {};
			defaults[option] = 10;
			override[option] = 0;
			var lock = new AsyncLock(defaults);
			return lock.acquire('key', function (cb) {
				setTimeout(function () { cb(null, 'completed'); }, 40);
			}, override).then(function (value) {
				assert.strictEqual(value, 'completed');
				assert.strictEqual(lock[option], 10);
				assert.strictEqual(lock.isBusy(), false);
			});
		});
	});
});
