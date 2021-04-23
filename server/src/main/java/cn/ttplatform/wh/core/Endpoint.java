package cn.ttplatform.wh.core;

import cn.ttplatform.wh.common.EndpointMetaData;
import java.net.InetSocketAddress;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * @author : wang hao
 * @date :  2020/8/15 21:41
 **/
@Data
@Builder
@ToString
@AllArgsConstructor
public class Endpoint implements Comparable<Endpoint> {

    private EndpointMetaData endpointMetaData;
    private int matchIndex;
    private int nextIndex;
    private long snapshotOffset;
    private long lastHeartBeat;
    private boolean replicating;

    public String getNodeId() {
        return endpointMetaData.getNodeId();
    }

    public InetSocketAddress getAddress() {
        return endpointMetaData.getAddress();
    }

    public void updateReplicationState(int matchIndex) {
        setMatchIndex(matchIndex);
        setNextIndex(matchIndex + 1);
    }

    public void backoffNextIndex() {
        if (nextIndex > 0) {
            nextIndex--;
        }
    }

    public void resetReplicationState(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
    }

    @Override
    public int compareTo(Endpoint o) {
        return this.getMatchIndex() - o.getMatchIndex();
    }
}
