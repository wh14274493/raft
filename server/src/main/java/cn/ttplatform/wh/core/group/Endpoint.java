package cn.ttplatform.wh.core.group;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/15 21:41
 **/
@Data
@Slf4j
@ToString
public class Endpoint implements Comparable<Endpoint> {

    private EndpointMetaData metaData;
    private int matchIndex;
    private int nextIndex;
    private long snapshotOffset;
    private long lastHeartBeat;
    private boolean snapshotReplicating;

    public Endpoint(String metaData) {
        this.metaData = new EndpointMetaData(metaData);
    }

    public Endpoint(EndpointMetaData metaData) {
        this.metaData = metaData;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public String getNodeId() {
        return metaData.getNodeId();
    }

    public void updateReplicationState(int matchIndex) {
        this.matchIndex = matchIndex;
        this.nextIndex = matchIndex + 1;
        log.debug("update {} replicationState[matchIndex={},nextIndex={}]", metaData.getNodeId(), matchIndex, nextIndex);
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
        return this.matchIndex - o.matchIndex;
    }
}
