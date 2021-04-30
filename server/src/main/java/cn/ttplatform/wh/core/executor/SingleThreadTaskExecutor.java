package cn.ttplatform.wh.core.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:30
 */
public class SingleThreadTaskExecutor implements TaskExecutor {

    private final ExecutorService executor;
    private final AtomicInteger index;

    public SingleThreadTaskExecutor() {
        index = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            r -> new Thread(r, "core-" + index.getAndIncrement()));
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
