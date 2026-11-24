/* global self */
(function (root, factory) {
	'use strict';
	if (typeof module === 'object' && module.exports) {
		module.exports = factory();
	} else {
		root.AsyncLock = factory();
	}
}(typeof self !== 'undefined' ? self : this, function () {
'use strict';

function AsyncLockError(message, name) {
	this.name = name || 'AsyncLockError';
	this.message = message;
	if (Error.captureStackTrace) {
		Error.captureStackTrace(this, this.constructor);
	} else {
		this.stack = new Error(message).stack;
	}
}

AsyncLockError.prototype = Object.create(Error.prototype);
AsyncLockError.prototype.constructor = AsyncLockError;

function queueError(name, prefix) {
	var LockError = function (key) {
		AsyncLockError.call(this, prefix + key, name);
		this.key = key;
	};
	LockError.prototype = Object.create(AsyncLockError.prototype);
	LockError.prototype.constructor = LockError;
	return LockError;
}

var errors = {
	AsyncLockError: AsyncLockError,
	AsyncLockTimeoutError: queueError('AsyncLockTimeoutError', 'async-lock timed out in queue '),
	AsyncLockMaxOccupationTimeError: queueError('AsyncLockMaxOccupationTimeError', 'Maximum occupation time is exceeded in queue '),
	AsyncLockMaxExecutionTimeError: queueError('AsyncLockMaxExecutionTimeError', 'Maximum execution time is exceeded '),
	AsyncLockMaxPendingError: queueError('AsyncLockMaxPendingError', 'Too many pending tasks in queue ')
};

var AsyncLock = function (opts) {
	opts = opts || {};

	this.Promise = opts.Promise || Promise;

	// format: {key : [fn, fn]}
	// queues[key] = null indicates no job running for key
	this.queues = Object.create(null);

	// lock is reentrant for same domain
	this.domainReentrant = opts.domainReentrant || false;
	if (this.domainReentrant) {
		if (typeof process === 'undefined' || typeof process.domain === 'undefined') {
			throw new errors.AsyncLockError(
				'Domain-reentrant locks require `process.domain` to exist. Please flip `opts.domainReentrant = false`, ' +
				'use a NodeJS version that still implements Domain, or install a browser polyfill.');
		}
		// domain of current running func {key : fn}
		this.domains = Object.create(null);
	}

	this.timeout = opts.timeout || AsyncLock.DEFAULT_TIMEOUT;
	this.maxOccupationTime = opts.maxOccupationTime || AsyncLock.DEFAULT_MAX_OCCUPATION_TIME;
	this.maxExecutionTime = opts.maxExecutionTime || AsyncLock.DEFAULT_MAX_EXECUTION_TIME;
	if (opts.maxPending === Infinity || (Number.isInteger(opts.maxPending) && opts.maxPending >= 0)) {
		this.maxPending = opts.maxPending;
	} else {
		this.maxPending = AsyncLock.DEFAULT_MAX_PENDING;
	}
};

AsyncLock.DEFAULT_TIMEOUT = 0; //Never
AsyncLock.DEFAULT_MAX_OCCUPATION_TIME = 0; //Never
AsyncLock.DEFAULT_MAX_EXECUTION_TIME = 0; //Never
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
		throw new errors.AsyncLockError('You must pass a function to execute');
	}

	// faux-deferred promise using new Promise() (as Promise.defer is deprecated)
	var deferredResolve = null;
	var deferredReject = null;
	var deferred = null;

	if (typeof (cb) !== 'function') {
		opts = cb;
		cb = null;

		// will return a promise
		deferred = new this.Promise(function(resolve, reject) {
			deferredResolve = resolve;
			deferredReject = reject;
		});
	}

	opts = opts || {};

	var resolved = false;
	var timer = null;
	var occupationTimer = null;
	var executionTimer = null;
	var holdingLock = false;
	var released = false;
	var self = this;

	var done = function (locked, err, ret) {
		// A timed-out holder may still finish later; it must not release its successor.
		if (locked) {
			if (released) return;
			released = true;
			holdingLock = false;
		}

		if (occupationTimer) {
			clearTimeout(occupationTimer);
			occupationTimer = null;
		}

		if (executionTimer) {
			clearTimeout(executionTimer);
			executionTimer = null;
		}

		if (locked) {
			if (!!self.queues[key] && self.queues[key].length === 0) {
				delete self.queues[key];
			}
			if (self.domainReentrant) {
				delete self.domains[key];
			}
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
					deferredReject(err);
				}
				else {
					deferredResolve(ret);
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
		holdingLock = locked;
		if (resolved) { // may due to timed out
			return done(locked);
		}

		if (timer) {
			clearTimeout(timer);
			timer = null;
		}

		if (self.domainReentrant && locked) {
			self.domains[key] = process.domain;
		}

		var maxExecutionTime = opts.maxExecutionTime || self.maxExecutionTime;
		if (maxExecutionTime) {
			executionTimer = setTimeout(function () {
				if (!!self.queues[key]) {
					done(locked, new errors.AsyncLockMaxExecutionTimeError(key));
				}
			}, maxExecutionTime);
		}

		// Callback mode
		if (fn.length === 1) {
			var called = false;
			try {
				fn(function (err, ret) {
					if (!called) {
						called = true;
						done(locked, err, ret);
					}
				});
			} catch (err) {
				// catching error thrown in user function fn
				if (!called) {
					called = true;
					done(locked, err);
				}
			}
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

	if (self.domainReentrant && !!process.domain) {
		exec = process.domain.bind(exec);
	}

	var maxPending = opts.maxPending || self.maxPending;

	if (!self.queues[key]) {
		self.queues[key] = [];
		exec(true);
	}
	else if (self.domainReentrant && !!process.domain && process.domain === self.domains[key]) {
		// If code is in the same domain of current running task, run it directly
		// Since lock is re-enterable
		exec(false);
	}
	else if (self.queues[key].length >= maxPending) {
		done(false, new errors.AsyncLockMaxPendingError(key));
	}
	else {
		var taskFn = function () {
			exec(true);
		};
		if (opts.skipQueue) {
			self.queues[key].unshift(taskFn);
		} else {
			self.queues[key].push(taskFn);
		}

		var timeout = opts.timeout || self.timeout;
		if (timeout) {
			timer = setTimeout(function () {
				timer = null;
				done(false, new errors.AsyncLockTimeoutError(key));
			}, timeout);
		}
	}

	var maxOccupationTime = opts.maxOccupationTime || self.maxOccupationTime;
		if (maxOccupationTime && !resolved) {
			occupationTimer = setTimeout(function () {
				if (!!self.queues[key]) {
					done(holdingLock, new errors.AsyncLockMaxOccupationTimeError(key));
				}
			}, maxOccupationTime);
		}

	if (deferred) {
		return deferred;
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
	var seen = Object.create(null);
	keys = keys.filter(function (key) {
		if (seen[key]) {
			return false;
		}
		seen[key] = true;
		return true;
	});

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

	var fnx = keys.reduceRight(function (prev, key) {
		return getFn(key, prev);
	}, fn);

	if (typeof (cb) === 'function') {
		fnx(cb);
	}
	else {
		return new this.Promise(function (resolve, reject) {
			// check for promise mode in case keys is empty array
			if (fnx.length === 1) {
				fnx(function (err, ret) {
					if (err) {
						reject(err);
					}
					else {
						resolve(ret);
					}
				});
			} else {
				resolve(fnx());
			}
		});
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

Object.keys(errors).forEach(function (name) {
	AsyncLock[name] = errors[name];
});

return AsyncLock;
}));
