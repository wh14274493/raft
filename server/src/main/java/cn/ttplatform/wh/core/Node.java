package cn.ttplatform.wh.core;

import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.Listener;
import cn.ttplatform.wh.server.NioListener;
import lombok.Getter;
import lombok.Setter;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:16
 **/
@Getter
@Setter
public class Node {

    private String selfId;
    private Role role;
    private final NodeContext context;
    private boolean start;
    private boolean stop;
    private final Listener listener;

    public Node(NodeContext context) {
        this.context = context;
        this.selfId = context.getProperties().getNodeId();
        this.listener = new NioListener(context);
    }

    public synchronized void start() {
        if (!start) {
            NodeState nodeState = context.getNodeState();
            this.role = Follower.builder()
                .scheduledFuture(context.electionTimeoutTask())
                .term(nodeState.getCurrentTerm())
                .voteTo(nodeState.getVoteTo())
                .preVoteCounts(1)
                .build();
            this.listener.listen();
        }
        start = true;
    }

    public synchronized void stop(){
        if (!stop){
            listener.stop();
            context.close();
        }
    }

    public int getTerm(){
        return role.getTerm();
    }

}
