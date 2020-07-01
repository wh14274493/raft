package cn.ttplatform.lc.task;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:28
 */
public interface TaskExecutor {

    <V> Future<V> submit(Callable<V> task);

    void execute(Runnable task);

    void close();
}
