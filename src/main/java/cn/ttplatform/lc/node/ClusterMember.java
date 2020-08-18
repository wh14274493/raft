package cn.ttplatform.lc.node;

import java.net.InetSocketAddress;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * @author : wang hao
 * @description : ClusterMember
 * @date :  2020/8/15 21:41
 **/
@Data
@Builder
@ToString
@AllArgsConstructor
public class ClusterMember {

    private String nodeId;
    private String host;
    private int port;
    private int matchIndex;
    private int nextIndex;
    private long lastHeartBeat;
    private boolean replicating;

    public String getNodeId() {
        return this.nodeId;
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(host, port);
    }

    public void startReplicate() {
        setReplicating(true);
    }

    public void stopReplication() {
        setReplicating(false);
    }

    public int getMatchIndex() {
        return this.matchIndex;
    }

    public int getNextIndex() {
        return this.nextIndex;
    }

    public void updateReplicationState(int matchIndex, int nextIndex) {
        setMatchIndex(matchIndex);
        setNextIndex(nextIndex);
    }

}
