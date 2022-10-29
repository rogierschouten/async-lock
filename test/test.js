'use strict';

var Q = require('q');
var _ = require('lodash');
var AsyncLock = require('../index.js');
var domain = require('domain');
var assert = require('assert');

Q.longStackSupport = true;

describe('AsyncLock Tests', function () {
	it('Single key test', function (done) {
		var lock = new AsyncLock();

		var taskCount = 8;
		var keyCount = 2;
		var finishedCount = 0;

		var isRunning = {};

		var taskNumbers = [];
		for (var i = 0; i < taskCount; i++) {
			taskNumbers.push(i);
		}

		taskNumbers.forEach(function (number) {
			var key = number % keyCount;
			lock.acquire(key, function (cb) {
				assert(!isRunning[key]);
				assert(lock.isBusy() && lock.isBusy(key));
				isRunning[key] = true;

				var timespan = Math.random() * 10;
				console.log('task%s(key%s) start, %s ms', number, key, timespan);
				setTimeout(cb.bind(null, null, number), timespan);
			}, function (err, result) {
				if (err) {
					return done(err);
				}

				console.log('task%s(key%s) done', number, key);

				isRunning[key] = false;
				finishedCount++;
				if (finishedCount === taskCount) {
					assert(!lock.isBusy());
					done();
				}
			});
		});
	});

	it('Multiple keys test', function (done) {
		var lock = new AsyncLock();
		var busy1 = false, busy2 = false;

		var finishCount = 0;
		var finish = function () {
			finishCount++;
			if (finishCount === 3) {
				done();
			}
		};

		lock.acquire(1, function (cb) {
			assert(!busy1);
			busy1 = true;

			var timespan = 10;
			console.log('task1(key1) start, %sms', timespan);
			setTimeout(cb, timespan);
		}, function (err) {
			if (err) {
				return done(err);
			}

			busy1 = false;
			console.log('task1(key1) done');
			finish();
		});

		lock.acquire(2, function (cb) {
			assert(!busy2);
			busy2 = true;

			var timespan = 20;
			console.log('task2(key2) start, %sms', timespan);
			setTimeout(cb, timespan);
		}, function (err) {
			if (err) {
				return done(err);
			}

			busy2 = false;
			console.log('task2(key2) done');
			finish();
		});

		lock.acquire([1, 2], function (cb) {
			assert(!busy1 && !busy2);
			busy1 = busy2 = true;

			var timespan = 10;
			console.log('task3(key1&2) start, %sms', timespan);
			setTimeout(cb, timespan);
		}, function (err) {
			if (err) {
				return done(err);
			}
			busy1 = busy2 = false;

			console.log('task3(key1&2) done');
			finish();
		});
	});

	it('should allow lock names that are also Object keys', function (done) {
		var lock = new AsyncLock();
		var key = 'constructor';
		var cb1;
		var client1Done = false;
		lock.acquire(key, function(cb) {
			cb1 = cb;
		});
		lock.acquire(key, function(cb) {
			assert(client1Done);
			cb();
			done();
		});
		assert(!!cb1);
		client1Done = true;
		cb1();
	});

	it('Time out test', function (done) {
		var lock = new AsyncLock({ timeout: 20 });
		var timedout = false;

		lock.acquire('key', function (cb) {
			setTimeout(cb, 50);
		})
			.then(function (err) {
				assert(timedout);
				done();
			});

		lock.acquire('key', function (cb) {
			assert('should not execute here');
			cb();
		})
			.catch(function (err) {
				// timed out
				console.log(err);
				if (err) {
					timedout = true;
				}
			});
	});

	it('Max occupation time test', function (done) {
		var lock = new AsyncLock({ maxOccupationTime: 10 });
		var maxOccupationTimeExceeded = false;
		var callBack = null;
		var order = 0;

		lock.acquire('key', function (cb) {
			callBack = cb;
			setTimeout(function(){
				assert.equal(++order, 3);
				if (!maxOccupationTimeExceeded) cb();
				assert(maxOccupationTimeExceeded);
				done();
			}, 50);
		})
		.catch(function (err) {
			// max occupation time is passed
			console.log(err);
			assert.equal(++order, 1);
			// release the lock and cancel the job if needed
			maxOccupationTimeExceeded = true;
			callBack();
		});

		lock.acquire('key', function (cb) {
			// Should be executed first
			assert.equal(++order, 2);
			cb();
		});
	});

	it('Max execution time test', function (done) {
		var lock = new AsyncLock({ maxExecutionTime: 10 });
		var shortFuncsResults = [];
		var longFuncsResults = [];

		var funcs = Array(6).fill().map(function (_, idx) {
			var isLongFuncCheck = idx % 2;
			var timeout;

			if (isLongFuncCheck) {
				timeout = 20;
			} else {
				timeout = 1;
			}

			return function () {
				return new Promise(function (resolve, reject) {
					var timer;

					lock.acquire('key', function (cb) {
						timer = setTimeout(function () {
							if (isLongFuncCheck) {
								longFuncsResults.push(true);
							} else {
								shortFuncsResults.push(true);
							}
							cb();
						}, timeout);
					}, function (err) {
						if (err) {
							// max execution time is passed
							console.log(err);
							clearTimeout(timer);

							if (isLongFuncCheck) {
								longFuncsResults.push(false);
							} else {
								shortFuncsResults.push(false);
							}
						}

						resolve();
					});
				});
			};
		});

		Promise.all(funcs.map(function (func) {
			return func();
		}))
		.then(function (args) {
			assert.equal(shortFuncsResults.length, 3);
			assert.equal(longFuncsResults.length, 3);
			assert(shortFuncsResults.every(function (isSuccessful) {
				return isSuccessful;
			}));
			assert(longFuncsResults.every(function (isSuccessful) {
				return !isSuccessful;
			}));
			done();
		});
	});

	it('Promise mode (Q)', function (done) {
		var lock = new AsyncLock();
		var value = 0;
		var concurrency = 8;

		Q.all(_.range(concurrency).map(function () {
			return lock.acquire('key', function () {
				var tmp = null;
				// Simulate non-atomic check and set
				return Q() // jshint ignore:line
					.delay(_.random(10))
					.then(function () {
						tmp = value;
					})
					.delay(_.random(20))
					.then(function () {
						value = tmp + 1;
					});
			});
		}))
			.then(function () {
				assert(value === concurrency);
			})
			.then(function () {
				var key1 = false, key2 = false;
				lock.acquire('key1', function () {
					key1 = true;
					return Q.delay(20).then(function () {
						key1 = false;
					});
				});
				lock.acquire('key2', function () {
					key2 = true;
					return Q.delay(10).then(function () {
						key2 = false;
					});
				});

				return lock.acquire(['key1', 'key2'], function () {
					assert(key1 === false && key2 === false);
				});
			})
			.nodeify(done);
	});

	it('reentrant lock in the same domain', function (done) {
		var lock = new AsyncLock({ domainReentrant: true });
		var d1 = domain.create();
		d1.run(function () {
			lock.acquire('key', function () {
				console.log('d1 locked key');
				return Q() // jshint ignore:line
					.delay(20)
					.then(function () {
						//Enter same lock twice
						return lock.acquire('key', function () {
							console.log('d1 locked key twice');
						});
					});
			});
		});

		var d2 = domain.create();
		d2.run(function () {
			return Q() // jshint ignore:line
				.delay(10)
				.then(function () {
					return lock.acquire('key', function () {
						console.log('d2 locked key');
						return lock.acquire('key', function () {
							console.log('d2 locked key twice');
						});
					});
				})
				.nodeify(done);
		});
	});

	it('Error handling: promise mode, throwing error', function (done) {
		var lock = new AsyncLock();
		lock.acquire('key', function () {
			throw new Error('error');
		}).catch(function (e) {
			assert(e.message === 'error');
			assert(!lock.isBusy());
			done();
		});
	});

	it('Error handling: promise mode, returning rejection', function (done) {
		var lock = new AsyncLock();
		lock.acquire('key', function () {
			return Promise.reject(new Error('error'));
		}).catch(function (e) {
			assert(e.message === 'error');
			assert(!lock.isBusy());
			done();
		});
	});

	it('Error handling: callback mode, throwing error', function (done) {
		var lock = new AsyncLock();
		lock.acquire('key', function (_lockDone) {
			throw new Error('error');
		}).catch(function (e) {
			assert(e.message === 'error');
			assert(!lock.isBusy());
			done();
		});
	});

	it('Error handling: callback mode, returning error', function (done) {
		var lock = new AsyncLock();
		lock.acquire('key', function (lockDone) {
			lockDone(new Error('error'));
		}).catch(function (e) {
			assert(e.message === 'error');
			assert(!lock.isBusy());
			done();
		});
	});

	it('Too much pending', function (done) {
		var lock = new AsyncLock({ maxPending: 1 });
		lock.acquire('key', function () {
			return Q().delay(20); // jshint ignore:line
		});
		lock.acquire('key', function () {
			return Q().delay(20); // jshint ignore:line
		});

		lock.acquire('key', function () { })
			.catch(function (e) {
				done();
			});
	});

	it('Infinitely max pending', function (done) {
		var lock = new AsyncLock({ maxPending: Infinity });
		lock.acquire('key', function () {
			return Q().delay(20); // jshint ignore:line
		});
		lock.acquire('key', function () {
			return Q().delay(20); // jshint ignore:line
		});

		lock.acquire('key', function () { })
			.then(function () {
				done();
			})
			.catch(function (e) {
				done(e);
			});
	});

	it('set maxPending to 0', function(done){
		var lock = new AsyncLock({maxPending : 0});
		lock.acquire('key', function(){
			return Q().delay(20); // jshint ignore:line
		});
		lock.acquire('key', function(){})
		.catch(function(e){
			done();
		});
	});

	it('use bluebird promise', function(done){
		var lock = new AsyncLock({Promise : require('bluebird')});
		lock.acquire('key', function(){})
		.nodeify(done);
	});

	it('use ES6 promise', function (done) {
		var lock = new AsyncLock({ Promise: Promise });
		lock.acquire('key', function () { })
			.then(done, done);
	});

	it('uses global Promise by default', function (done) {
		var lock = new AsyncLock({ });
		lock.acquire('key', function () { })
			.then(done, done);
	});

	it('invalid parameter', function (done) {
		var lock = new AsyncLock();
		try {
			lock.acquire('key', null);
		}
		catch (e) {
			done();
		}
	});

	it('bug #2 on https://github.com/rain1017/async-lock/issues/2', function (done) {
		var lock = new AsyncLock({});

		// this case gave a TypeError
		function work(cb) {
			return setTimeout(cb, 10);
		}

		var doneCount = 0;
		function cb() {
			doneCount++;
			if (doneCount === 2) {
				done();
			}
		}

		lock.acquire(['A', 'B', 'C'], work, cb);
		lock.acquire(['A', 'B', 'C'], work, cb);
	});


	it('Allow queue skipping', function (done) {
		var lock = new AsyncLock();
		var completeTasks = [];
		function onDone () {
			if (completeTasks.length !== 3) return;
			assert.deepEqual(completeTasks, [1, 3, 2], 'Expected third task to skip queue');
			done();
		}
		lock.acquire('key', function (lockDone) {
			setTimeout(function () {
				completeTasks.push(1);
				lockDone();
			}, 20);
		}, onDone);
		lock.acquire('key', function (lockDone) {
			setTimeout(function () {
				completeTasks.push(2);
				lockDone();
			}, 20);
		}, onDone);
		lock.acquire('key', function (lockDone) {
			setTimeout(function () {
				completeTasks.push(3);
				lockDone();
			}, 20);
		}, onDone, { skipQueue: true });
	});


	it('Promise Mode: should call worker even when no keys are locked', function (done) {
		var lock = new AsyncLock();
		lock.acquire([], function () {
			done();
			return new Q(true);
		});
	});

	it('Promise Mode: should pass worker\'s result', function() {
		var lock = new AsyncLock();
		return lock.acquire(['key'], function () {
			return new Q('result');
		}).then(function (status) {
			assert.equal(status, 'result');
		});
	});

	it('Promise Mode: should pass worker\'s result even when no keys are locked', function () {
		var lock = new AsyncLock();
		return lock.acquire([], function cb () {
			return new Q('result');
		}).then(function (status) {
			assert.equal(status, 'result');
		});
	}).timeout(20);

	it('Does not mutate key array during batch acquire (#51)', function () {
		var lock = new AsyncLock();
		var keys = [1, 2, 3];
		return lock.acquire(keys, function () { })
			.then(function () {
				assert.deepEqual(keys, [1, 2, 3]);
			});
	});
});
