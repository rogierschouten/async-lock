'use strict';

var assert = require('assert');
var AsyncLock = require('../index');

describe('Expired queue entries', function () {

	['timeout', 'maxOccupationTime'].forEach(function (limit) {
		it('reclaims a pending slot after ' + limit, function () {
			var lock = new AsyncLock({ maxPending: 1 });
			var release;
			var expiredRan = false;
			var holder = lock.acquire('key', function (done) { release = done; });
			var options = {};
			options[limit] = 10;
			return lock.acquire('key', function () { expiredRan = true; }, options)
				.then(function () { assert.fail('the queued acquisition must expire'); }, function (error) {
					assert(error instanceof (limit === 'timeout' ?
						AsyncLock.AsyncLockTimeoutError : AsyncLock.AsyncLockMaxOccupationTimeError));
					assert(lock.isBusy('key'));
					var replacement = lock.acquire('key', function () { return 'replacement'; });
					release();
					return Promise.all([holder, replacement]);
				}).then(function (results) {
					assert.strictEqual(results[1], 'replacement');
					assert.strictEqual(expiredRan, false);
					assert.strictEqual(lock.isBusy(), false);
				});
		});
	});

	it('preserves live waiters when an entry at the front expires', function (done) {
		var lock = new AsyncLock({ maxPending: 2 });
		var release;
		var order = [];
		lock.acquire('key', function (cb) { release = cb; }, function (error) {
			assert.ifError(error);
		});
		lock.acquire('key', function (cb) {
			order.push('first');
			cb();
		}, function (error) { assert.ifError(error); });
		lock.acquire('key', function () { assert.fail('expired work ran'); }, function (error) {
			assert(error instanceof AsyncLock.AsyncLockTimeoutError);
			lock.acquire('key', function (cb) {
				order.push('second');
				cb();
			}, function (error) {
				if (error) return done(error);
				assert.deepStrictEqual(order, ['first', 'second']);
				done();
			});
			release();
		}, { timeout: 10, skipQueue: true });
	});
});
