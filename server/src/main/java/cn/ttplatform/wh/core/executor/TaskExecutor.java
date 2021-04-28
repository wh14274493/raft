package cn.ttplatform.wh.core.executor;

/**
 * @author Wang Hao
 * @date 2020/6/30 10:28
 */
public interface TaskExecutor {

    /**
     * Submit a task to the thread pool
     *
     * @param task a special task
     */
    void execute(Runnable task);

    /**
     * Close the thread pool
     */
    void close();
}
