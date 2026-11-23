'use strict';

var assert = require('assert');
var AsyncLock = require('../');

function check(error, name, message) {
	assert.strictEqual(typeof AsyncLock[name], 'function');
	assert(error instanceof AsyncLock[name]);
	assert(error instanceof AsyncLock.AsyncLockError);
	assert(error instanceof Error);
	assert.strictEqual(error.name, name);
	assert.strictEqual(error.message, message);
	assert.strictEqual(typeof error.stack, 'string');
}

describe('Recognisable lock errors', function () {
	it('exports a base error for invalid acquire arguments', function () {
		try {
			new AsyncLock().acquire('key', null);
			assert.fail('An invalid worker must throw');
		} catch (error) {
			check(error, 'AsyncLockError', 'You must pass a function to execute');
		}
	});

	it('reports a typed queue-wait timeout', function () {
		var lock = new AsyncLock();
		var release;
		var holder = lock.acquire('key', function (cb) { release = cb; });
		return lock.acquire('key', function () {
			assert.fail('Timed-out work must not run');
		}, { timeout: 10 }).then(function () {
			assert.fail('The waiter must time out');
		}, function (error) {
			release();
			check(error, 'AsyncLockTimeoutError', 'async-lock timed out in queue key');
			assert.strictEqual(error.key, 'key');
			return holder;
		});
	});

	[
		['maxOccupationTime', 'AsyncLockMaxOccupationTimeError', 'Maximum occupation time is exceeded in queue key'],
		['maxExecutionTime', 'AsyncLockMaxExecutionTimeError', 'Maximum execution time is exceeded key']
	].forEach(function (entry) {
		it('reports a typed ' + entry[0] + ' error', function () {
			var opts = {};
			opts[entry[0]] = 10;
			var release;
			return new AsyncLock().acquire('key', function (cb) { release = cb; }, opts).then(function () {
				assert.fail('The task must time out');
			}, function (error) {
				release();
				check(error, entry[1], entry[2]);
				assert.strictEqual(error.key, 'key');
			});
		});
	});

	it('reports a typed pending-limit error in callback mode', function (done) {
		var lock = new AsyncLock({ maxPending: 0 });
		var release;
		lock.acquire('key', function (cb) { release = cb; });
		lock.acquire('key', function (cb) {
			cb(new Error('Must not run'));
		}, function (error) {
			release();
			try {
				check(error, 'AsyncLockMaxPendingError', 'Too many pending tasks in queue key');
				assert.strictEqual(error.key, 'key');
				done();
			} catch (failure) { done(failure); }
		});
	});

	it('preserves the original worker error', function () {
		var failure = new Error('worker failed');
		return new AsyncLock().acquire('key', function () { throw failure; }).then(function () {
			assert.fail('The worker must reject');
		}, function (error) {
			assert.strictEqual(error, failure);
		});
	});
});
