'use strict';

var assert = require('assert');
var AsyncLock = require('../');

describe('Synchronous queue handover', function () {

	it('completes a large callback queue in order without exhausting the stack', function () {
		var lock = new AsyncLock({maxPending: Infinity});
		var release;
		var actual = [];
		var expected = [];
		lock.acquire('key', function (cb) { release = cb; }, function (err) { assert.ifError(err); });

		function enqueue(value) {
			expected.push(value);
			lock.acquire('key', function (cb) {
				cb(null, value);
			}, function (err, result) {
				actual.push(err || result);
			});
		}

		for (var i = 0; i < 10000; i++) enqueue(i);
		release();

		assert.strictEqual(actual.length, expected.length);
		assert.deepStrictEqual(actual, expected);
		assert.strictEqual(lock.isBusy(), false);
	});

	it('pauses at an asynchronous holder and resumes the remaining queue', function () {
		var lock = new AsyncLock({maxPending: Infinity});
		var release;
		var releaseMiddle;
		var order = [];
		var completed = 0;
		var errors = [];
		lock.acquire('key', function (cb) { release = cb; }, function (err) { assert.ifError(err); });

		function enqueue(value) {
			lock.acquire('key', function (cb) {
				order.push(value);
				cb();
			}, function (err) {
				if (err) errors.push(err);
				completed++;
			});
		}

		var i;
		for (i = 0; i < 5000; i++) enqueue(i);
		lock.acquire('key', function (cb) {
			order.push('middle');
			releaseMiddle = cb;
		}, function (err) { assert.ifError(err); });
		for (i = 5000; i < 10000; i++) enqueue(i);
		lock.acquire('key', function (cb) {
			order.push('priority');
			cb();
		}, function (err) { assert.ifError(err); }, {skipQueue: true});

		release();
		assert.strictEqual(completed, 5000);
		assert.strictEqual(order[0], 'priority');
		assert.strictEqual(order[5001], 'middle');
		assert.strictEqual(lock.isBusy('key'), true);

		var unrelated = false;
		lock.acquire('other', function (cb) {
			unrelated = true;
			cb();
		}, function (err) { assert.ifError(err); });
		assert.strictEqual(unrelated, true);
		assert.strictEqual(completed, 5000);

		releaseMiddle();
		assert.strictEqual(completed, 10000);
		assert.deepStrictEqual(errors, []);
		assert.strictEqual(order.length, 10002);
		assert.strictEqual(order[5002], 5000);
		assert.strictEqual(order[10001], 9999);
		assert.strictEqual(lock.isBusy(), false);
	});
});
