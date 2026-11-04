<?php

namespace Illuminate\Queue;

use Illuminate\Contracts\Events\Dispatcher as EventDispatcher;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Attributes\Delay;
use Illuminate\Queue\Events\QueueFailedOver;
use Illuminate\Support\Collection;
use RuntimeException;
use Throwable;

use function Illuminate\Support\enum_value;

class FailoverQueue extends Queue implements QueueContract
{
    /**
     * The queues which failed on the last action.
     *
     * @var list<string>
     */
    protected array $failingQueues = [];

    /**
     * Create a new failover queue instance.
     */
    public function __construct(
        public QueueManager $manager,
        public EventDispatcher $events,
        public array $connections
    ) {
    }

    /**
     * Get the size of the queue.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return int
     */
    public function size($queue = null)
    {
        return $this->manager->connection($this->connections[0])->size($queue);
    }

    /**
     * Get the number of pending jobs.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return int
     */
    public function pendingSize($queue = null)
    {
        return $this->manager->connection($this->connections[0])->pendingSize($queue);
    }

    /**
     * Get the number of delayed jobs.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return int
     */
    public function delayedSize($queue = null)
    {
        return $this->manager->connection($this->connections[0])->delayedSize($queue);
    }

    /**
     * Get the number of reserved jobs.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return int
     */
    public function reservedSize($queue = null)
    {
        return $this->manager->connection($this->connections[0])->reservedSize($queue);
    }

    /**
     * Get the number of jobs across every queue.
     *
     * @return int
     */
    public function totalSize()
    {
        return $this->manager->connection($this->connections[0])->totalSize();
    }

    /**
     * Get the number of pending jobs across every queue.
     *
     * @return int
     */
    public function totalPendingSize()
    {
        return $this->manager->connection($this->connections[0])->totalPendingSize();
    }

    /**
     * Get the number of delayed jobs across every queue.
     *
     * @return int
     */
    public function totalDelayedSize()
    {
        return $this->manager->connection($this->connections[0])->totalDelayedSize();
    }

    /**
     * Get the number of reserved jobs across every queue.
     *
     * @return int
     */
    public function totalReservedSize()
    {
        return $this->manager->connection($this->connections[0])->totalReservedSize();
    }

    /**
     * Get the pending jobs for the given queue.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return \Illuminate\Support\Collection
     */
    public function pendingJobs($queue = null): Collection
    {
        return $this->manager->connection($this->connections[0])->pendingJobs($queue);
    }

    /**
     * Get the delayed jobs for the given queue.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return \Illuminate\Support\Collection
     */
    public function delayedJobs($queue = null): Collection
    {
        return $this->manager->connection($this->connections[0])->delayedJobs($queue);
    }

    /**
     * Get the reserved jobs for the given queue.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return \Illuminate\Support\Collection
     */
    public function reservedJobs($queue = null): Collection
    {
        return $this->manager->connection($this->connections[0])->reservedJobs($queue);
    }

    /**
     * Get all pending jobs across every queue.
     *
     * @return \Illuminate\Support\Collection
     */
    public function allPendingJobs(): Collection
    {
        return $this->manager->connection($this->connections[0])->allPendingJobs();
    }

    /**
     * Get all delayed jobs across every queue.
     *
     * @return \Illuminate\Support\Collection
     */
    public function allDelayedJobs(): Collection
    {
        return $this->manager->connection($this->connections[0])->allDelayedJobs();
    }

    /**
     * Get all reserved jobs across every queue.
     *
     * @return \Illuminate\Support\Collection
     */
    public function allReservedJobs(): Collection
    {
        return $this->manager->connection($this->connections[0])->allReservedJobs();
    }

    /**
     * Get the creation timestamp of the oldest pending job, excluding delayed jobs.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return int|null
     */
    public function creationTimeOfOldestPendingJob($queue = null)
    {
        return $this->manager
            ->connection($this->connections[0])
            ->creationTimeOfOldestPendingJob($queue);
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  object|string  $job
     * @param  mixed  $data
     * @param  \UnitEnum|string|null  $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->attemptOnAllConnections(__FUNCTION__, func_get_args(), $job);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string  $payload
     * @param  \UnitEnum|string|null  $queue
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        return $this->attemptOnAllConnections(__FUNCTION__, func_get_args());
    }

    /**
     * Push a new job onto the queue after (n) seconds.
     *
     * @param  \DateTimeInterface|\DateInterval|int  $delay
     * @param  string  $job
     * @param  mixed  $data
     * @param  \UnitEnum|string|null  $queue
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->attemptOnAllConnections(__FUNCTION__, func_get_args(), $job);
    }

    /**
     * Push an array of jobs onto the queue.
     *
     * @param  array  $jobs
     * @param  mixed  $data
     * @param  \UnitEnum|string|null  $queue
     * @return void
     */
    public function bulk($jobs, $data = '', $queue = null)
    {
        foreach ((array) $jobs as $job) {
            $delay = is_object($job) ? $this->getAttributeValue($job, Delay::class, 'delay') : null;

            if (isset($delay)) {
                $this->later($delay, $job, $data, $queue);
            } else {
                $this->push($job, $data, $queue);
            }
        }
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  \UnitEnum|string|null  $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        return $this->manager->connection($this->connections[0])->pop($queue);
    }

    /**
     * Attempt the given method on all connections.
     *
     * @param  mixed  $job
     * @return mixed
     *
     * @throws \Throwable
     */
    protected function attemptOnAllConnections(string $method, array $arguments, $job = null)
    {
        // An inner connection defers an after-commit push by registering a callback and
        // returning, so the push would fail outside of the failover loop. Deferring the
        // whole attempt instead means it runs once no transaction is open, where the
        // inner connections' own addCallback() executes the push immediately. Open
        // transactions are checked first so connections are only resolved if needed.
        if ($job !== null &&
            $this->hasCallbackApplicableTransactions() &&
            $this->shouldDispatchAfterCommit($job)) {
            // Building the payload now surfaces serialization errors inside the transaction,
            // as an inner connection would, instead of after the data has been committed...
            $this->createPayloadForAttempt($method, $arguments, $job);

            return $this->attemptOnAllConnectionsAfterCommit($method, $arguments, $job);
        }

        return $this->attemptOnAllConnectionsNow($method, $arguments, $job);
    }

    /**
     * Attempt the given method on all connections once the open transactions have committed.
     *
     * @param  mixed  $job
     * @return mixed
     */
    protected function attemptOnAllConnectionsAfterCommit(string $method, array $arguments, $job)
    {
        $this->registerRollbackCallbacksForJobsThatDispatchAfterCommit($job);

        return $this->container->make('db.transactions')->addCallback(function () use ($method, $arguments, $job) {
            // A transaction may still be open on another connection, where an inner connection
            // would defer the push again, outside of the failover loop, so wait for it too...
            return $this->hasCallbackApplicableTransactions()
                ? $this->attemptOnAllConnectionsAfterCommit($method, $arguments, $job)
                : $this->attemptOnAllConnectionsNow($method, $arguments, $job);
        });
    }

    /**
     * Determine if there are open transactions that after-commit callbacks would wait for.
     *
     * @return bool
     */
    protected function hasCallbackApplicableTransactions()
    {
        return isset($this->container) &&
            $this->container->bound('db.transactions') &&
            $this->container->make('db.transactions')->callbackApplicableTransactions()->isNotEmpty();
    }

    /**
     * Create the payload for the given job, as the attempted method would.
     *
     * @param  mixed  $job
     * @return string
     *
     * @throws \Illuminate\Queue\InvalidPayloadException
     * @throws \RuntimeException
     */
    protected function createPayloadForAttempt(string $method, array $arguments, $job)
    {
        [$delay, $data, $queue] = $method === 'later'
            ? [$arguments[0], $arguments[2] ?? '', $arguments[3] ?? null]
            : [null, $arguments[1] ?? '', $arguments[2] ?? null];

        return $this->createPayload($job, enum_value($queue), $data, $delay);
    }

    /**
     * Attempt the given method on all connections immediately.
     *
     * @param  mixed  $job
     * @return mixed
     *
     * @throws \Throwable
     */
    protected function attemptOnAllConnectionsNow(string $method, array $arguments, $job = null)
    {
        [$lastException, $failedQueues] = [null, []];

        try {
            foreach ($this->connections as $connection) {
                try {
                    return $this->manager->connection($connection)->{$method}(...$arguments);
                } catch (Throwable $e) {
                    $lastException = $e;

                    $failedQueues[] = $connection;

                    if ($job !== null && ! in_array($connection, $this->failingQueues)) {
                        $this->events->dispatch(new QueueFailedOver($connection, $job, $e));
                    }
                }
            }
        } finally {
            $this->failingQueues = $failedQueues;
        }

        throw $lastException ?? new RuntimeException('All failover queue connections failed.');
    }

    /**
     * Determine if the job should be dispatched after all database transactions have committed.
     *
     * The job's own preference wins; otherwise the primary connection's configuration
     * decides, since that is the connection the job is normally pushed to.
     *
     * @param  \Closure|string|object  $job
     * @return bool
     */
    protected function shouldDispatchAfterCommit($job)
    {
        try {
            $primary = $this->manager->connection($this->connections[0]);
        } catch (Throwable) {
            // The failover loop reports the error when it tries this connection...
            return parent::shouldDispatchAfterCommit($job);
        }

        return $primary instanceof Queue
            ? $primary->shouldDispatchAfterCommit($job)
            : parent::shouldDispatchAfterCommit($job);
    }
}
