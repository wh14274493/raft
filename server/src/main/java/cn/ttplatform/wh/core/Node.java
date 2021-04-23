package cn.ttplatform.wh.core;

import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Role;
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

    public Node(NodeContext context) {
        this.context = context;
        this.selfId = context.getProperties().getNodeId();
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
            new NioListener(context).listen();
        }
        start = true;
    }

    public int getTerm(){
        return role.getTerm();
    }

}
