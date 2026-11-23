'use strict';

var assert = require('assert');
var AsyncLock = require('../');

describe('Occupation timeout queue progression', function () {

	['callback', 'promise'].forEach(function (mode) {
		it('releases a running ' + mode + ' task once, including after late completion', function () {
			var lock = new AsyncLock();
			var finishFirst;
			var finishSecond;
			var order = [];
			var work = mode === 'callback' ? function (cb) {
				finishFirst = cb;
			} : function () {
				return new Promise(function (resolve) { finishFirst = resolve; });
			};
			var first = lock.acquire('key', work, { maxOccupationTime: 10 });
			var second = lock.acquire('key', function (cb) {
				order.push('second');
				finishSecond = cb;
			});
			var third = lock.acquire('key', function () { order.push('third'); });

			return first.then(function () {
				assert.fail('The first task must exceed its occupation limit');
			}, function (err) {
				assert.match(err.message, /Maximum occupation time/);
				assert.deepEqual(order, ['second']);
				finishFirst();
				return Promise.resolve().then(function () {
					assert.deepEqual(order, ['second'], 'late completion must not release the next holder');
					assert(lock.isBusy('key'));
					finishSecond();
					return Promise.all([second, third]);
				});
			}).then(function () {
				assert.deepEqual(order, ['second', 'third']);
				assert(!lock.isBusy());
			});
		});
	});

	it('does not release the active holder when a waiting task expires', function () {
		var lock = new AsyncLock();
		var finishHolder;
		var started = false;
		var holder = lock.acquire('key', function (cb) { finishHolder = cb; });
		var expired = lock.acquire('key', function () {
			assert.fail('An expired waiter must not run');
		}, { maxOccupationTime: 10 });
		var next = lock.acquire('key', function () { started = true; });
		return expired.then(function () {
			assert.fail('The waiting task must exceed its occupation limit');
		}, function (err) {
			assert.match(err.message, /Maximum occupation time/);
			assert(!started);
			assert(lock.isBusy('key'));
			finishHolder();
			return Promise.all([holder, next]);
		}).then(function () {
			assert(started);
			assert(!lock.isBusy());
		});
	});
});
