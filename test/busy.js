'use strict';

var assert = require('assert');
var AsyncLock = require('../');

describe('Busy checks for falsy keys', function () {
	['', 0].forEach(function (key) {
		it('checks only the requested key ' + JSON.stringify(key), function () {
			var lock = new AsyncLock();
			var releaseOther;
			var releaseKey;
			lock.acquire('other', function (cb) { releaseOther = cb; }, function () {});
			try {
				assert.strictEqual(lock.isBusy(), true);
				assert.strictEqual(lock.isBusy(undefined), true);
				assert.strictEqual(lock.isBusy(key), false);
				lock.acquire(key, function (cb) { releaseKey = cb; }, function () {});
				assert.strictEqual(lock.isBusy(key), true);
				assert.strictEqual(lock.isBusy(String(key)), true);
				releaseKey();
				assert.strictEqual(lock.isBusy(key), false);
				assert.strictEqual(lock.isBusy('other'), true);
			} finally {
				if (releaseKey) releaseKey();
				releaseOther();
			}
			assert.strictEqual(lock.isBusy(), false);
		});
	});
});
