package com.cellulant;

import com.cellulant.utils.Logging;
import java.util.LinkedList;


/**
 * A ThreadPool is a group of a limited number of threads that are used to
 * execute tasks.
 *
 *  Cellulant Ltd
 * @author <a href="kim.kiogora@cellulant.com">Kim Kiogora</a>
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 * @version Version 3.0
 */
public class ThreadPool extends ThreadGroup {
    /**
     * Flag to check if the pool is active and can accept new tasks.
     */
    private boolean isAlive;
    /**
     * The list of tasks to perform.
     */
    private LinkedList<Runnable> taskQueue;
    /**
     * Identifier for the worker thread. Incremented for each worker thread.
     */
    private int threadID;
    /**
     * Identifier for the thread pool. Incremented for each new thread pool.
     */
    private static int threadPoolID;
    /**
     * Logging class instance.
     */
    private Logging log;

    /**
     * Creates a new ThreadPool.
     *
     * @param numThreads the number of threads in the pool
     * @param log the logging class for the thread pool
     */
    @SuppressWarnings({
        "ValueOfIncrementOrDecrementUsed",
        "CallToThreadStartDuringObjectConstruction"
    })
    public ThreadPool(final int numThreads, final Logging log) {
        super("ThreadPool-" + threadPoolID++);
        this.log = log;
        setDaemon(true);
        isAlive = true;
        taskQueue = new LinkedList<Runnable>();

        for (int i = 0; i < numThreads; i++) {
            new WorkerThread().start();
        }
    }

    /**
     * <p>Requests a new task to run. This method returns immediately, and the
     * task executes on the next available idle thread in this ThreadPool.</p>
     * <p>Tasks start execution in the order they are received.</p>
     *
     * @param task the task to run (if null, no action is taken)
     *
     * @throws IllegalStateException if this ThreadPool is already closed
     */
    public synchronized void runTask(final Runnable task) {
        if (!isAlive) {
            throw new IllegalStateException();
        }

        if (task != null) {
            taskQueue.add(task);
            /*
                * Use notify() here NOT notifyAll() because all the waiting
                * threads are interchangeable (the order they wake up doesn't
                * matter). When a task is added, only one of the threads should
                * be notified to wake up, execute the task and the go back to
                * sleep.
                */
            notify();
        }

    }

    /**
     * Get a Runnable task from the task queue.
     *
     * @return the Runnable task
     *
     * @throws InterruptedException thrown when if the thread is waiting,
     *                              sleeping, or otherwise occupied, and the
     *                              thread is interrupted, either before or
     *                              during the activity
     */
    private synchronized Runnable getTask() throws InterruptedException {
        while (taskQueue.isEmpty()) {
            if (!isAlive) {
                return null;
            }

            wait();
        }

        return taskQueue.removeFirst();
    }

    /**
     * <p>Closes this ThreadPool and returns immediately. All threads are
     * stopped, and any waiting tasks are not executed. Once a ThreadPool is
     * closed, no more tasks can be run on this ThreadPool.</p>
     * <p>NOTE: use the join() method to close the ThreadPool after finishing
     * waiting tasks.</p>
     */
    public synchronized void close() {
        if (isAlive) {
            isAlive = false;
            taskQueue.clear();
            interrupt();
        }
    }

    /**
     * Closes this ThreadPool and waits for all running threads to finish. Any
     * waiting tasks are executed.
     */
    public void join() {
        // Notify all waiting threads that this ThreadPool is no longer alive
        synchronized (this) {
            isAlive = false;
            notifyAll();
        }

        // Wait for all threads to finish
        Thread[] threads = new Thread[activeCount()];
        int count = enumerate(threads);
        for (int i = 0; i < count; i++) {
            try {
                threads[i].join(2000);
            } catch (InterruptedException ex) {
                log.info("ThreadPool | Failed to finish all tasks: "
                        + ex.getMessage());
            }
        }
    }

    /**
     * Get the size of the task queue.
     *
     * @return the size of the task queue
     */
    public synchronized int getListSize() {
        return this.taskQueue.size();
    }

    /**
     * Method <i>clearQueue</i> clears all the records in the queue.
     */
    public void clearQueue() {
        this.taskQueue.clear();
    }

    /**
     * A WorkerThread is a Thread in a ThreadPool group, designed to run tasks
     * (Runnables).
     */
    private class WorkerThread extends Thread {
        /**
         * Constructor.
         */
        @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
        WorkerThread() {
            super(ThreadPool.this, "WorkerThread-" + threadID++);
        }

        /**
         * Processes (runs) the task queue.
         */
        @Override
        public void run() {
            while (!isInterrupted()) {
                // Get a task to run
                Runnable task = null;
                try {
                    task = getTask();
                } catch (InterruptedException ex) {
                    log.error(ex.getMessage());
                }

                /*
                 * If getTask() returned null or was interrupted, close this
                 * thread by returning.
                 */
                if (task == null) {
                    return;
                }

                // Run the task, and eat any exceptions it throws
                try {
                    task.run();
                } catch (Throwable t) {
                    uncaughtException(this, t);
                }
            }
        }
    }
}
