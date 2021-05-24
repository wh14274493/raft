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
public class SingleThreadExecutor implements TaskExecutor {

    private final ExecutorService executor;
    private final AtomicInteger index;

    public SingleThreadExecutor() {
        index = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(0, 1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
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
