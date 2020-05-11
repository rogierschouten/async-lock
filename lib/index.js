'use strict';

// Small Map polyfill to remain ES5-compatible as requested in issue #21 while also solving PR #30

var MapPolyfill = function () {
	this.elements = [];
	this.size = 0;
};

MapPolyfill.prototype.has = function (key) {
	var i;
	for (i = 0; i < this.elements.length; ++i) {
		if (this.elements[i].key === key) {
			return true;
		}
	}
	return false;
};

MapPolyfill.prototype.get = function (key) {
	var i;
	for (i = 0; i < this.elements.length; ++i) {
		if (this.elements[i].key === key) {
			return this.elements[i].value;
		}
	}
	return false;
};

MapPolyfill.prototype.set = function (key, value) {
	var i;
	for (i = 0; i < this.elements.length; ++i) {
		if (this.elements[i].key === key) {
			this.elements[i].value = value;
			return;
		}
	}
	this.elements.push({ key: key, value: value });
	this.size++;
};

MapPolyfill.prototype.delete = function (key) {
	var i;
	for (i = 0; i < this.elements.length; ++i) {
		if (this.elements[i].key === key) {
			this.elements.splice(i, 1);
			this.size--;
			return;
		}
	}
};

var AsyncLock = function (opts) {
	opts = opts || {};

	this.Promise = opts.Promise || Promise;

	// format: {key : [fn, fn]}
	// queues.has(key) indicates job running for key
	this.queues = new MapPolyfill();

	// domain of current running func {key : fn}
	this.domains = new MapPolyfill();

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

	// faux-deferred promise using new Promise() (as Promise.defer is deprecated)
	var deferredResolve = null;
	var deferredReject = null;
	var deferred = null;

	if (typeof (cb) !== 'function') {
		opts = cb;
		cb = null;

		// will return a promise
		deferred =  new this.Promise(function(resolve, reject) {
			deferredResolve = resolve;
			deferredReject = reject;
		});
	}

	opts = opts || {};

	var resolved = false;
	var timer = null;
	var self = this;

	var done = function (locked, err, ret) {
		if (locked) {
			if (self.queues.get(key).length === 0) {
				self.queues.delete(key);
			}
			self.domains.delete(key);
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
			if (self.queues.has(key) && self.queues.get(key).length > 0) {
				self.queues.get(key).shift()();
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
			self.domains.set(key, process.domain);
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

	if (!self.queues.has(key)) {
		self.queues.set(key, []);
		exec(true);
	}
	else if (self.domainReentrant && !!process.domain && process.domain === self.domains.get(key)) {
		// If code is in the same domain of current running task, run it directly
		// Since lock is re-enterable
		exec(false);
	}
	else if (self.queues.get(key).length >= self.maxPending) {
		done(false, new Error('Too many pending tasks'));
	}
	else {
		var taskFn = function () {
			exec(true);
		};
		if (opts.skipQueue) {
			self.queues.get(key).unshift(taskFn);
		} else {
			self.queues.get(key).push(taskFn);
		}

		var timeout = opts.timeout || self.timeout;
		if (timeout) {
			timer = setTimeout(function () {
				timer = null;
				done(false, new Error('async-lock timed out'));
			}, timeout);
		}
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
		return this.queues.size > 0;
	}
	else {
		return this.queues.has(key);
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

module.exports = AsyncLock;
