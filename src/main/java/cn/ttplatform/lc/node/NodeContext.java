package cn.ttplatform.lc.node;

import cn.ttplatform.lc.rpc.Connector;
import cn.ttplatform.lc.schedule.Scheduler;
import cn.ttplatform.lc.task.TaskExecutor;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:39
 */
public class NodeContext {

    private Scheduler scheduler;
    private NodeContext nodeContext;
    private Connector connector;
    private TaskExecutor taskExecutor;
}
