package cn.ttplatform.lc.node;

import cn.ttplatform.lc.node.store.Log;
import cn.ttplatform.lc.rpc.Connector;
import cn.ttplatform.lc.schedule.Scheduler;
import cn.ttplatform.lc.task.TaskExecutor;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:39
 */
public class NodeContext {

    private Scheduler scheduler;
    private Connector connector;
    private TaskExecutor taskExecutor;
    private Log log;
    private Cluster cluster;

    public Scheduler scheduler() {
        return scheduler;
    }

    public Connector connector() {
        return connector;
    }

    public TaskExecutor executor() {
        return taskExecutor;
    }

    public Log log() {
        return log;
    }

    public Cluster cluster() {
        return cluster;
    }
}
