package cn.ttplatform.wh.scheduler;

import cn.ttplatform.wh.config.ServerProperties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:28
 */
public class SingleThreadScheduler implements Scheduler {

    private final ServerProperties properties;
    private final ScheduledExecutorService executor;

    public SingleThreadScheduler(ServerProperties properties) {
        this.properties = properties;
        this.executor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "scheduler"));
    }

    @Override
    public ScheduledFuture<?> scheduleElectionTimeoutTask(Runnable task) {
        int maxElectionTimeout = properties.getMaxElectionTimeout();
        int minElectionTimeout = properties.getMinElectionTimeout();
        int timeout = ThreadLocalRandom.current().nextInt(maxElectionTimeout - minElectionTimeout) + minElectionTimeout;
        return executor.schedule(task, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleLogReplicationTask(Runnable task) {
        long delay = properties.getLogReplicationDelay();
        long interval = properties.getLogReplicationInterval();
        return executor.scheduleWithFixedDelay(task, delay, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

}
