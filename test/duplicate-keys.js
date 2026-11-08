'use strict';

var assert = require('assert');
var AsyncLock = require('../index.js');

describe('Duplicate batch keys (#64)', function () {

	it('acquires each key once without changing a frozen input array', function () {
		var lock = new AsyncLock({maxPending: 0});
		var keys = Object.freeze(['a', 'b', 'a', 'b']);
		var calls = 0;
		var result;
		lock.acquire(keys, function (done) {
			calls++;
			assert(lock.isBusy('a'));
			assert(lock.isBusy('b'));
			done(null, 'value');
		}, function (err, value) {
			assert.ifError(err);
			result = value;
		});
		assert.strictEqual(calls, 1);
		assert.strictEqual(result, 'value');
		assert.deepStrictEqual(keys, ['a', 'b', 'a', 'b']);
		assert.strictEqual(lock.isBusy(), false);
	});

	[
		{name: 'native', Promise: Promise},
		{name: 'Q', Promise: require('q').Promise},
		{name: 'Bluebird', Promise: require('bluebird')}
	].forEach(function (implementation) {
		it('resolves the worker result with ' + implementation.name, function () {
			var lock = new AsyncLock({Promise: implementation.Promise, timeout: 30});
			var calls = 0;
			var result = lock.acquire(['a', 'a', 'b', 'a'], function () {
				calls++;
				return implementation.Promise.resolve('value');
			});
			return result.then(function (value) {
				assert.strictEqual(value, 'value');
				assert.strictEqual(calls, 1);
				assert.strictEqual(lock.isBusy(), false);
			});
		});
	});

	it('keeps all keys locked until the batch completes and releases waiting tasks', function () {
		var lock = new AsyncLock();
		var release;
		var calls = [];
		lock.acquire(['a', 'b', 'a'], function (done) {
			calls.push('batch');
			release = done;
		}, function (err) { assert.ifError(err); });
		['a', 'b'].forEach(function (key) {
			lock.acquire(key, function (done) {
				calls.push(key);
				done();
			}, function (err) { assert.ifError(err); });
		});
		assert.deepStrictEqual(calls, ['batch']);
		release();
		assert.deepStrictEqual(calls, ['batch', 'a', 'b']);
		assert.strictEqual(lock.isBusy(), false);
	});

	it('releases every batch key when the worker throws', function () {
		var lock = new AsyncLock();
		var expected = new Error('worker failed');
		var actual;
		lock.acquire(['a', 'b', 'a'], function (done) {
			throw expected;
		}, function (err) { actual = err; });
		assert.strictEqual(actual, expected);
		assert.strictEqual(lock.isBusy(), false);
		var entered = false;
		lock.acquire(['a', 'b'], function (done) {
			entered = true;
			done();
		}, function (err) { assert.ifError(err); });
		assert.strictEqual(entered, true);
	});

	it('uses queue key identity for numeric aliases and prototype property names', function () {
		var lock = new AsyncLock();
		var calls = 0;
		lock.acquire(['__proto__', 'constructor', '__proto__', 1, '1'], function (done) {
			calls++;
			assert(lock.isBusy('__proto__'));
			assert(lock.isBusy('constructor'));
			assert(lock.isBusy('1'));
			done();
		}, function (err) { assert.ifError(err); });
		assert.strictEqual(calls, 1);
		assert.strictEqual(lock.isBusy(), false);
	});

	it('deduplicates each acquisition independently', function () {
		var lock = new AsyncLock();
		var keys = ['a', 'a'];
		var calls = 0;
		for (var i = 0; i < 2; i++) {
			lock.acquire(keys, function (done) { // jshint ignore:line
				calls++;
				done();
			}, function (err) { assert.ifError(err); }); // jshint ignore:line
		}
		assert.strictEqual(calls, 2);
		assert.deepStrictEqual(keys, ['a', 'a']);
		assert.strictEqual(lock.isBusy(), false);
	});
});
