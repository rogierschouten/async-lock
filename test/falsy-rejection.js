'use strict';

var assert = require('assert');
var AsyncLock = require('../index');

describe('Falsy promise rejection reasons', function () {
	[null, undefined, false, 0, ''].forEach(function (reason, index) {
		['key', ['key', 'other']].forEach(function (keys) {
			['reject', 'throw'].forEach(function (mode) {
				it('preserves reason ' + index + ' for ' + mode + ' with ' + JSON.stringify(keys), function () {
					var lock = new AsyncLock();
					var failed = lock.acquire(keys, function () {
						if (mode === 'throw') {
							throw reason;
						}
						return Promise.reject(reason);
					});
					var next = lock.acquire(keys, function () { return 'next'; });
					return failed.then(function () {
						throw new Error('The acquisition must reject');
					}, function (error) {
						assert.strictEqual(error, reason);
					}).then(function () {
						return next;
					}).then(function (value) {
						assert.strictEqual(value, 'next');
						assert.strictEqual(lock.isBusy(), false);
					});
				});
			});
		});
	});

	it('retains null-error success for callback tasks', function () {
		var lock = new AsyncLock();
		return lock.acquire(['one', 'two'], function (done) {
			done(null, 42);
		}).then(function (value) {
			assert.strictEqual(value, 42);
		});
	});
});
