'use strict';

var AsyncLock = require('./index');

module.exports = {
	AsyncLockError: AsyncLock.AsyncLockError,
	AsyncLockTimeoutError: AsyncLock.AsyncLockTimeoutError,
	AsyncLockMaxOccupationTimeError: AsyncLock.AsyncLockMaxOccupationTimeError,
	AsyncLockMaxExecutionTimeError: AsyncLock.AsyncLockMaxExecutionTimeError,
	AsyncLockMaxPendingError: AsyncLock.AsyncLockMaxPendingError
};
