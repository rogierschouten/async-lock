'use strict';

var assert = require('assert');
var AsyncLock = require('../index');

describe('Completion callbacks', function () {
	it('releases queued work even if a completion callback throws', function () {
		var lock = new AsyncLock();
		var release;
		var ran = false;
		var error = new Error('completion failed');
		lock.acquire('key', function (done) { release = done; }, function () { throw error; });
		lock.acquire('key', function (done) { ran = true; done(); }, function (err) { assert.ifError(err); });
		assert.throws(function () { release(); }, function (err) { return err === error; });
		assert.strictEqual(ran, true);
		assert.strictEqual(lock.isBusy(), false);
	});

	it('does not release a new holder acquired by its completion callback', function () {
		var lock = new AsyncLock();
		var releaseFirst;
		var releaseSecond;
		var thirdRan = false;
		lock.acquire('key', function (done) { releaseFirst = done; }, function () {
			lock.acquire('key', function (done) { releaseSecond = done; }, function (err) { assert.ifError(err); });
			lock.acquire('key', function (done) { thirdRan = true; done(); }, function (err) { assert.ifError(err); });
		});
		releaseFirst();
		assert.strictEqual(thirdRan, false);
		assert.strictEqual(lock.isBusy('key'), true);
		releaseSecond();
		assert.strictEqual(thirdRan, true);
		assert.strictEqual(lock.isBusy(), false);
	});
});
