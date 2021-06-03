package cn.ttplatform.wh.group;

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
    private QuickMatchHelper quickMatchHelper;
    private int matchIndex;
    private int nextIndex;
    private long snapshotOffset;
    private boolean matched;
    private long lastHeartBeat;
    private boolean replicating;

    public Endpoint(String metaData) {
        this.metaData = new EndpointMetaData(metaData);
    }

    public Endpoint(EndpointMetaData metaData) {
        this.metaData = metaData;
    }

    public String getNodeId() {
        return metaData.getNodeId();
    }

    public boolean updateReplicationState(int matchIndex) {
        if (!matched) {
            quickMatchNextIndex(true);
        }
        if (this.matchIndex == matchIndex) {
            log.debug("No change in matchIndex, stop replication.");
            replicating = false;
            return false;
        }
        this.matchIndex = matchIndex;
        this.nextIndex = matchIndex + 1;
        log.debug("update {} replicationState[matchIndex={},nextIndex={}]", metaData.getNodeId(), matchIndex,
            nextIndex);
        return true;
    }

    public void quickMatchNextIndex(boolean lastMatched) {
        log.debug("{} match helper is {}", this.metaData, quickMatchHelper);
        if (!matched) {
            matched = quickMatchHelper.isMatched();
        }
        if (matched) {
            if (nextIndex > 0) {
                nextIndex--;
            }
            // at this point, we should use log snapshots to synchronize followerd logs
        } else {
            quickMatchHelper.update(lastMatched);
            nextIndex = quickMatchHelper.getIndex();
        }
    }

    public void resetReplicationState(int initLeftEdge, int initRightEdge) {
        this.quickMatchHelper = new QuickMatchHelper(initLeftEdge, initRightEdge);
        this.nextIndex = quickMatchHelper.getIndex();
        this.matched = quickMatchHelper.isMatched();
        this.matchIndex = 0;

        if (log.isDebugEnabled()) {
            log.debug("reset {} ReplicationState.", metaData.getNodeId());
            log.debug("create QuickMatchHelper {}", quickMatchHelper);
            log.debug("init nextIndex {}", nextIndex);
            log.debug("init matched {}", matched);
            log.debug("init matchIndex {}", matchIndex);
        }
    }

    @Override
    public int compareTo(Endpoint o) {
        return this.matchIndex - o.matchIndex;
    }

    /**
     * Use the binary search method to quickly locate the matchIndex and nextIndex of the follower
     */
    static class QuickMatchHelper {

        int initLeftEdge;
        int initRightEdge;
        int leftEdge;
        int rightEdge;

        QuickMatchHelper(int initLeftEdge, int initRightEdge) {
            this.initLeftEdge = initLeftEdge;
            this.initRightEdge = initRightEdge;
            this.leftEdge = initLeftEdge;
            this.rightEdge = initRightEdge;
        }

        public boolean isMatched() {
            return leftEdge >= rightEdge;
        }

        public int getIndex() {
            if (leftEdge >= rightEdge) {
                return leftEdge;
            }
            return (rightEdge - leftEdge) / 2 + leftEdge;
        }

        public void update(boolean lastMatched) {
            int mid = (rightEdge - leftEdge) / 2 + leftEdge;
            if (lastMatched) {
                // means that nextIndex must be located between mid and rightEdge
                leftEdge = mid + 1;
            } else {
                // means that nextIndex must be located between leftEdge and mid
                rightEdge = mid - 1;
            }
        }

        @Override
        public String toString() {
            return "QuickMatchHelper{" +
                "initLeftEdge=" + initLeftEdge +
                ", initRightEdge=" + initRightEdge +
                ", leftEdge=" + leftEdge +
                ", rightEdge=" + rightEdge +
                '}';
        }
    }
}