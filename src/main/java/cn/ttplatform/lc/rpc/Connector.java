package cn.ttplatform.lc.rpc;

import cn.ttplatform.lc.node.Endpoint;
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

    void sendRequestVote(RequestVote msg, List<Endpoint> endpoints);

    void replyRequestVote(RequestVoteResult msg, Endpoint endpoint);

    void sendAppendEntries(AppendEntries msg, List<Endpoint> endpoints);

    void replyAppendEntries(AppendEntriesResult msg, Endpoint endpoint);
}
