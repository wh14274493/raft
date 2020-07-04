package cn.ttplatform.lc.rpc;

import cn.ttplatform.lc.node.Endpoint;
import cn.ttplatform.lc.rpc.message.AppendEntries;
import cn.ttplatform.lc.rpc.message.AppendEntriesResult;
import cn.ttplatform.lc.rpc.message.RequestVote;
import cn.ttplatform.lc.rpc.message.RequestVoteResult;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:21
 */
public class NettyConnector implements Connector {

    @Override
    public void sendRequestVote(RequestVote msg, List<Endpoint> endpoints) {

    }

    @Override
    public void replyRequestVote(RequestVoteResult msg, Endpoint endpoint) {

    }

    @Override
    public void sendAppendEntries(AppendEntries msg, List<Endpoint> endpoints) {

    }

    @Override
    public void replyAppendEntries(AppendEntriesResult msg, Endpoint endpoint) {

    }
}
