package cn.ttplatform.wh.core;

import cn.ttplatform.wh.common.EndpointMetaData;
import java.net.InetSocketAddress;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author : wang hao
 * @date :  2020/8/15 21:41
 **/
@Data
@ToString
public class Endpoint implements Comparable<Endpoint> {

    @Getter
    @Setter
    @ToString
    public static class ReplicationState {

        private int matchIndex;
        private int nextIndex;

    }

    private EndpointMetaData metaData;
    private ReplicationState replicationState;
    private long snapshotOffset;
    private long lastHeartBeat;
    private boolean replicating;

    public Endpoint(EndpointMetaData metaData) {
        this.metaData = metaData;
        this.replicationState = new ReplicationState();
    }

    public int getMatchIndex() {
        return replicationState.getMatchIndex();
    }

    public int getNextIndex() {
        return replicationState.getNextIndex();
    }

    public String getNodeId() {
        return metaData.getNodeId();
    }

    public InetSocketAddress getAddress() {
        return metaData.getAddress();
    }

    public void updateReplicationState(int matchIndex) {
        replicationState.setMatchIndex(matchIndex);
        replicationState.setNextIndex(matchIndex + 1);
    }

    public void backoffNextIndex() {
        int nextIndex = replicationState.getNextIndex();
        if (nextIndex > 0) {
            replicationState.setNextIndex(--nextIndex);
        }
    }

    public void resetReplicationState(int nextIndex) {
        replicationState.setNextIndex(nextIndex);
        replicationState.setMatchIndex(0);
    }

    @Override
    public int compareTo(Endpoint o) {
        return replicationState.getMatchIndex() - replicationState.getMatchIndex();
    }
}
