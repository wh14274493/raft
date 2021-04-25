package cn.ttplatform.wh.core.support;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:28
 */
public interface TaskExecutor {

    /**
     * Submit a task to the thread pool
     * @param task
     */
    void execute(Runnable task);

    /**
     * Close the thread pool
     */
    void close();
}
