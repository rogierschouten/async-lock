'use strict';

var Q = require('q');
var _ = require('lodash');
var AsyncLock = require('../index.js');
var domain = require('domain');
var assert = require('assert');

Q.longStackSupport = true;

describe('AsyncLock Tests', function(){
	it('Single key test', function(done){
		var lock = new AsyncLock();

		var taskCount = 8;
		var keyCount = 2;
		var finishedCount = 0;

	    var isRunning = {};

	    var taskNumbers = [];
	    for(var i=0; i<taskCount; i++){
	        taskNumbers.push(i);
	    }

	    taskNumbers.forEach(function(number){
	    	var key = number % keyCount;
	        lock.acquire(key, function(cb){
	        	assert(!isRunning[key]);
	        	assert(lock.isBusy() && lock.isBusy(key));

	            var timespan = Math.random() * 10;
	            console.log('task%s(key%s) start, %s ms', number, key, timespan);
	            setTimeout(cb.bind(null, null, number), timespan);
	        }, function(err, result){
	        	if(err){
	        		return done(err);
	        	}

	            console.log('task%s(key%s) done', number, key);

	            isRunning[key] = false;
	            finishedCount++;
	            if(finishedCount === taskCount){
	            	assert(!lock.isBusy());
	            	done();
	            }
	        });
	    });
	});

	it('Multiple keys test', function(done){
		var lock = new AsyncLock();
		var busy1 = false, busy2 = false;

		var finishCount = 0;
		var finish = function(){
			finishCount++;
			if(finishCount === 3){
				done();
			}
		};

	    lock.acquire(1, function(cb){
	    	assert(!busy1);
	    	busy1 = true;

	        var timespan = 10;
	        console.log('task1(key1) start, %sms', timespan);
	        setTimeout(cb, timespan);
	    }, function(err){
	    	if(err){
	    		return done(err);
	    	}

	    	busy1 = false;
	        console.log('task1(key1) done');
	        finish();
	    });

	    lock.acquire(2, function(cb){
	    	assert(!busy2);
	    	busy2 = true;

	        var timespan = 20;
	        console.log('task2(key2) start, %sms', timespan);
	        setTimeout(cb, timespan);
	    }, function(err){
	    	if(err){
	    		return done(err);
	    	}

	    	busy2 = false;
	        console.log('task2(key2) done');
	        finish();
	    });

	    lock.acquire([1, 2], function(cb){
	    	assert(!busy1 && !busy2);
	    	busy1 = busy2 = true;

	        var timespan = 10;
	        console.log('task3(key1&2) start, %sms', timespan);
	        setTimeout(cb, timespan);
	    }, function(err){
	    	if(err){
	    		return done(err);
	    	}
	    	busy1 = busy2 = false;

	        console.log('task3(key1&2) done');
	        finish();
	    });
	});

	it('Time out test', function(done){
		var lock = new AsyncLock({timeout : 20});
		var timedout = false;

		lock.acquire('key', function(cb){
			setTimeout(cb, 50);
		})
		.then(function(err){
			assert(timedout);
			done();
		});

		lock.acquire('key', function(cb){
			assert('should not execute here');
			cb();
		})
		.catch(function(err){
			// timed out
			console.log(err);
			if(err){
				timedout = true;
			}
		});
	});

	it('Promise mode', function(done){
		var lock = new AsyncLock();
		var value = 0;
		var concurrency = 8;

		Q.all(_.range(concurrency).map(function(){
			return lock.acquire('key', function(){
				var tmp = null;
				// Simulate non-atomic check and set
				return Q() // jshint ignore:line
				.delay(_.random(10))
				.then(function(){
					tmp = value;
				})
				.delay(_.random(20))
				.then(function(){
					value = tmp + 1;
				});
			});
		}))
		.then(function(){
			assert(value === concurrency);
		})
		.then(function(){
			var key1 = false, key2 = false;
			lock.acquire('key1', function(){
				key1 = true;
				return Q.delay(20).then(function(){
					key1 = false;
				});
			});
			lock.acquire('key2', function(){
				key2 = true;
				return Q.delay(10).then(function(){
					key2 = false;
				});
			});

			return lock.acquire(['key1', 'key2'], function(){
				assert(key1 === false && key2 === false);
			});
		})
		.nodeify(done);
	});

	it('reentrant lock in the same domain', function(done){
		var lock = new AsyncLock({domainReentrant : true});
		var d1 = domain.create();
		d1.run(function(){
			lock.acquire('key', function(){
				console.log('d1 locked key');
				return Q() // jshint ignore:line
				.delay(20)
				.then(function(){
					//Enter same lock twice
					return lock.acquire('key', function(){
						console.log('d1 locked key twice');
					});
				});
			});
		});

		var d2 = domain.create();
		d2.run(function(){
			return Q() // jshint ignore:line
			.delay(10)
			.then(function(){
				return lock.acquire('key', function(){
					console.log('d2 locked key');
					return lock.acquire('key', function(){
						console.log('d2 locked key twice');
					});
				});
			})
			.nodeify(done);
		});
	});

	it('Error handling', function(done){
		var lock = new AsyncLock();
		lock.acquire('key', function(){
			throw new Error('error');
		}).catch(function(e){
			assert(e.message === 'error');
			done();
		});
	});

	it('Too much pending', function(done){
		var lock = new AsyncLock({maxPending : 1});
		lock.acquire('key', function(){
			return Q().delay(20); // jshint ignore:line
		});
		lock.acquire('key', function(){
			return Q().delay(20); // jshint ignore:line
		});

		lock.acquire('key', function(){})
		.catch(function(e){
			done();
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

	it('invalid parameter', function(done){
		var lock = new AsyncLock();
		try{
			lock.acquire('key', null);
		}
		catch(e){
			done();
		}
	});
});
