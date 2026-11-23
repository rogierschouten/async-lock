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

module.exports = {
	AsyncLockError: AsyncLockError,
	AsyncLockTimeoutError: queueError('AsyncLockTimeoutError', 'async-lock timed out in queue '),
	AsyncLockMaxOccupationTimeError: queueError('AsyncLockMaxOccupationTimeError', 'Maximum occupation time is exceeded in queue '),
	AsyncLockMaxExecutionTimeError: queueError('AsyncLockMaxExecutionTimeError', 'Maximum execution time is exceeded '),
	AsyncLockMaxPendingError: queueError('AsyncLockMaxPendingError', 'Too many pending tasks in queue ')
};
