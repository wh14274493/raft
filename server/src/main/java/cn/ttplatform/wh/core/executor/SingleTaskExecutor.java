package cn.ttplatform.wh.core.executor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/5/15 0:05
 */
@Slf4j
public class SingleTaskExecutor implements TaskExecutor {

    private final ExecutorService executor;
    private final AtomicInteger index;

    public SingleTaskExecutor() {
        index = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(0, 1, 60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1),
            r -> new Thread(r, "snapshotTask-" + index.getAndIncrement()),
            (r, e) -> log.error("There is currently an executing task, reject this operation."));
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
