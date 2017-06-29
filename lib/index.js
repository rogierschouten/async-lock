'use strict';

var AsyncLock = function (opts) {
	opts = opts || {};

	this.Promise = opts.Promise || Promise;

	// format: {key : [fn, fn]}
	// queues[key] = null indicates no job running for key
	this.queues = {};

	// domain of current running func {key : fn}
	this.domains = {};

	// lock is reentrant for same domain
	this.domainReentrant = opts.domainReentrant || false;

	this.timeout = opts.timeout || AsyncLock.DEFAULT_TIMEOUT;
	this.maxPending = opts.maxPending || AsyncLock.DEFAULT_MAX_PENDING;
};

AsyncLock.DEFAULT_TIMEOUT = 0; //Never
AsyncLock.DEFAULT_MAX_PENDING = 1000;

/**
 * Acquire Locks
 *
 * @param {String|Array} key 	resource key or keys to lock
 * @param {function} fn 	async function
 * @param {function} cb 	callback function, otherwise will return a promise
 * @param {Object} opts 	options
 */
AsyncLock.prototype.acquire = function (key, fn, cb, opts) {
	if (Array.isArray(key)) {
		return this._acquireBatch(key, fn, cb, opts);
	}

	if (typeof (fn) !== 'function') {
		throw new Error('You must pass a function to execute');
	}

	var deferred = null;
	if (typeof (cb) !== 'function') {
		opts = cb;
		cb = null;

		// will return a promise
		deferred = this._deferPromise();
	}

	opts = opts || {};

	var resolved = false;
	var timer = null;
	var self = this;

	var done = function (locked, err, ret) {
		if (locked) {
			if (self.queues[key].length === 0) {
				delete self.queues[key];
			}
			delete self.domains[key];
		}

		if (!resolved) {
			if (!deferred) {
				if (typeof (cb) === 'function') {
					cb(err, ret);
				}
			}
			else {
				//promise mode
				if (err) {
					deferred.reject(err);
				}
				else {
					deferred.resolve(ret);
				}
			}
			resolved = true;
		}

		if (locked) {
			//run next func
			if (!!self.queues[key] && self.queues[key].length > 0) {
				self.queues[key].shift()();
			}
		}
	};

	var exec = function (locked) {
		if (resolved) { // may due to timed out
			return done(locked);
		}

		if (timer) {
			clearTimeout(timer);
			timer = null;
		}

		if (locked) {
			self.domains[key] = process.domain;
		}

		// Callback mode
		if (fn.length === 1) {
			var called = false;
			fn(function (err, ret) {
				if (!called) {
					called = true;
					done(locked, err, ret);
				}
			});
		}
		else {
			// Promise mode
			self._promiseTry(function () {
				return fn();
			})
			.then(function(ret){
				done(locked, undefined, ret);
			}, function(error){
				done(locked, error);
			});
		}
	};
	if (!!process.domain) {
		exec = process.domain.bind(exec);
	}

	if (!self.queues[key]) {
		self.queues[key] = [];
		exec(true);
	}
	else if (self.domainReentrant && !!process.domain && process.domain === self.domains[key]) {
		// If code is in the same domain of current running task, run it directly
		// Since lock is re-enterable
		exec(false);
	}
	else if (self.queues[key].length >= self.maxPending) {
		done(false, new Error('Too much pending tasks'));
	}
	else {
		self.queues[key].push(function () {
			exec(true);
		});

		var timeout = opts.timeout || self.timeout;
		if (timeout) {
			timer = setTimeout(function () {
				timer = null;
				done(false, new Error('async-lock timed out'));
			}, timeout);
		}
	}

	if (deferred) {
		return deferred.promise;
	}
};

/*
 * Below is how this function works:
 *
 * Equivalent code:
 * self.acquire(key1, function(cb){
 *     self.acquire(key2, function(cb){
 *         self.acquire(key3, fn, cb);
 *     }, cb);
 * }, cb);
 *
 * Equivalent code:
 * var fn3 = getFn(key3, fn);
 * var fn2 = getFn(key2, fn3);
 * var fn1 = getFn(key1, fn2);
 * fn1(cb);
 */
AsyncLock.prototype._acquireBatch = function (keys, fn, cb, opts) {
	if (typeof (cb) !== 'function') {
		opts = cb;
		cb = null;
	}

	var self = this;
	var getFn = function (key, fn) {
		return function (cb) {
			self.acquire(key, fn, cb, opts);
		};
	};

	var fnx = fn;
	keys.reverse().forEach(function (key) {
		fnx = getFn(key, fnx);
	});

	if (typeof (cb) === 'function') {
		fnx(cb);
	}
	else {
		var deferred = this._deferPromise();
		fnx(function (err, ret) {
			if (err) {
				deferred.reject(err);
			}
			else {
				deferred.resolve(ret);
			}
		});
		return deferred.promise;
	}
};

/*
 *	Whether there is any running or pending asyncFunc
 *
 *	@param {String} key
 */
AsyncLock.prototype.isBusy = function (key) {
	if (!key) {
		return Object.keys(this.queues).length > 0;
	}
	else {
		return !!this.queues[key];
	}
};

/**
 * Promise.try() implementation to become independent of Q-specific methods
 */
AsyncLock.prototype._promiseTry = function(fn) {
	try {
		return this.Promise.resolve(fn());
	} catch (e) {
		return this.Promise.reject(e);
	}
};

/**
 * Promise.defer() implementation to become independent of Q-specific methods
 */
AsyncLock.prototype._deferPromise = function() {
	if (typeof this.Promise.defer === 'function') {
		// note that Q does not have a constructor with reject/resolve functions so we have no option but use its defer() method
		return this.Promise.defer();
	} else {
		// for promise implementations that don't have a defer() method we create one ourselves
		var result = {
			reject: function(err) {
				// some promise libraries e.g. Q take some time setting the reject property while others do it synchronously
				return Promise.resolve().then(function() {
					result.reject(err);
				});
			},
			resolve: function(ret) {
				// some promise libraries e.g. Q take some time setting the reject property while others do it synchronously
				return Promise.resolve().then(function() {
					result.resolve(ret);
				});
			},
			promise: undefined
		};
		result.promise = new this.Promise(function(resolve, reject) {
			result.reject = reject;
			result.resolve = resolve;
		});
		return result;
	}
};

module.exports = AsyncLock;

