package cn.ttplatform.lc.rpc;

import cn.ttplatform.lc.node.NodeEndpoint;
import cn.ttplatform.lc.rpc.message.AppendEntries;
import cn.ttplatform.lc.rpc.message.AppendEntriesResult;
import cn.ttplatform.lc.rpc.message.RequestVote;
import cn.ttplatform.lc.rpc.message.RequestVoteResult;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Connector {

    void sendRequestVote(RequestVote msg, List<NodeEndpoint> endpoints);

    void replyRequestVote(RequestVoteResult msg, NodeEndpoint endpoint);

    void sendAppendEntries(AppendEntries msg, List<NodeEndpoint> endpoints);

    void replyAppendEntries(AppendEntriesResult msg, NodeEndpoint endpoint);
}
