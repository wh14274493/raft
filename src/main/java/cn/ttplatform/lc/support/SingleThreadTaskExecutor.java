package cn.ttplatform.lc.support;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:30
 */
public class SingleThreadTaskExecutor implements TaskExecutor {

    private final ExecutorService executor;

    public SingleThreadTaskExecutor() {
        this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            r -> new Thread(r, "raft-core"));
    }

    @Override
    public <V> Future<V> submit(Callable<V> task) {
        return executor.submit(task);
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
