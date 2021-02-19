package cn.ttplatform.lc.core;

import cn.ttplatform.lc.config.ServerProperties;
import cn.ttplatform.lc.core.rpc.Connector;
import cn.ttplatform.lc.support.Scheduler;
import cn.ttplatform.lc.core.store.NodeState;
import cn.ttplatform.lc.core.store.log.Log;
import cn.ttplatform.lc.support.TaskExecutor;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:39
 */
public class NodeContext {

    private Scheduler scheduler;
    private Connector connector;
    private TaskExecutor taskExecutor;
    private NodeState nodeState;
    private Log log;
    private Cluster cluster;
    private ServerProperties properties;

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

    public NodeState nodeState() {
        return nodeState;
    }

    public ServerProperties config() {
        return properties;
    }
}
