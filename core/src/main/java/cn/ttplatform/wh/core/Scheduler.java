package cn.ttplatform.wh.core;

import java.util.concurrent.ScheduledFuture;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Scheduler {

    ScheduledFuture<?> scheduleElectionTimeoutTask(Runnable task);

    ScheduledFuture<?> scheduleLogReplicationTask(Runnable task);

}
