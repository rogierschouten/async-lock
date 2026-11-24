'use strict';

var assert = require('assert');
var AsyncLock = require('../index');

describe('Empty batch callback mode', function () {
	[false, true].forEach(function (promise) {
		it('delivers a ' + (promise ? 'promised' : 'synchronous') + ' return value', function () {
			var lock = new AsyncLock();
			var result;
			var calls = 0;
			lock.acquire([], function () { return promise ? Promise.resolve(42) : 42; }, function (error, value) {
				assert.ifError(error);
				calls++;
				result = value;
			});
			return Promise.resolve().then(function () {
				assert.strictEqual(calls, 1);
				assert.strictEqual(result, 42);
				assert.strictEqual(lock.isBusy(), false);
			});
		});
	});

	it('delivers a thrown worker error to the callback', function () {
		var lock = new AsyncLock();
		var error = new Error('worker failed');
		var result;
		lock.acquire([], function () { throw error; }, function (err) { result = err; });
		return Promise.resolve().then(function () { assert.strictEqual(result, error); });
	});

	it('catches callback-style worker errors and ignores duplicate completion', function () {
		var lock = new AsyncLock();
		var error = new Error('worker failed');
		var result;
		lock.acquire([], function (done) { throw error; }, function (err) { result = err; });
		assert.strictEqual(result, error);
		var calls = 0;
		lock.acquire([], function (done) { done(null, 42); done(null, 43); }, function (err, value) {
			assert.ifError(err);
			assert.strictEqual(value, 42);
			calls++;
		});
		assert.strictEqual(calls, 1);
	});

	it('reports invalid workers consistently for empty batches', function () {
		var lock = new AsyncLock();
		assert.throws(function () { lock.acquire([], null); }, AsyncLock.AsyncLockError);
	});
});
