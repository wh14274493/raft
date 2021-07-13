package cn.ttplatform.wh.group;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/15 21:41
 **/
@Getter
@Setter
@Slf4j
@EqualsAndHashCode
@ToString
public class Endpoint implements Comparable<Endpoint> {

    private EndpointMetaData metaData;
    private QuickMatchHelper quickMatchHelper;
    /**
     * replication status for log
     */
    private int matchIndex;
    private int nextIndex;
    /**
     * replication status for snapshot
     */
    private long snapshotOffset;

    private long lastHeartBeat;
    private boolean replicating;
    private boolean matchComplete;

    public Endpoint(String metaData) {
        this.metaData = new EndpointMetaData(metaData);
    }

    public Endpoint(EndpointMetaData metaData) {
        this.metaData = metaData;
    }

    public String getNodeId() {
        return metaData.getNodeId();
    }

    public void resetReplicationState(int initLeftEdge, int initRightEdge) {
        this.quickMatchHelper = new QuickMatchHelper(initLeftEdge, initRightEdge);
        this.matchComplete = false;
        this.matchIndex = initLeftEdge;
        this.nextIndex = initRightEdge;

        if (log.isDebugEnabled()) {
            log.debug("reset {} ReplicationState.", metaData.getNodeId());
            log.debug("create QuickMatchHelper {}", quickMatchHelper);
            log.debug("init nextIndex {}", nextIndex);
            log.debug("init matchComplete {}", matchComplete);
            log.debug("init matchIndex {}", matchIndex);
        }
    }

    public void backoffNextIndex() {
        if (nextIndex > 0) {
            nextIndex--;
        }
    }

    public int getNextIndex() {
        if (quickMatchHelper == null || matchComplete) {
            return nextIndex;
        }
        return quickMatchHelper.getIndex();
    }

    public boolean updateReplicationState(int matchIndex) {
        matchComplete = true;
        this.nextIndex = matchIndex + 1;
        if (this.matchIndex != matchIndex) {
            this.matchIndex = matchIndex;
            log.debug("update {} replicationState[matchIndex={},nextIndex={}]", metaData.getNodeId(), matchIndex, nextIndex);
            replicating = true;
            return true;
        }
        replicating = false;
        return false;
    }

    public void updateMatchHelperState(boolean matched) {
        if (quickMatchHelper.canContinue()) {
            quickMatchHelper.updateEdge(matched);
        } else {
            quickMatchHelper.complete(matched);
            matchComplete = true;
        }
    }

    @Override
    public int compareTo(Endpoint o) {
        return this.matchIndex - o.matchIndex;
    }

    /**
     * Use the binary search method to quickly locate the matchIndex and nextIndex of the follower
     */
    class QuickMatchHelper {

        int initLeftEdge;
        int initRightEdge;
        int leftEdge;
        int rightEdge;
        boolean lastMatched;
        int lastMatchIndex;

        QuickMatchHelper(int initLeftEdge, int initRightEdge) {
            this.initLeftEdge = initLeftEdge;
            this.initRightEdge = initRightEdge;
            this.leftEdge = initLeftEdge;
            this.rightEdge = initRightEdge;
        }

        public int getIndex() {
            return (rightEdge - leftEdge) / 2 + leftEdge;
        }

        public boolean canContinue() {
            return leftEdge < rightEdge;
        }

        public void updateEdge(boolean matched) {
            lastMatched = matched;
            int mid = (rightEdge - leftEdge) / 2 + leftEdge;
            if (lastMatched) {
                lastMatchIndex = mid;
            }
            if (matched) {
                leftEdge = mid + 1;
            } else {
                rightEdge = mid - 1;
            }
        }

        public void complete(boolean matched) {
            assert leftEdge == rightEdge;
            if (matched) {
                matchIndex = leftEdge;
                nextIndex = leftEdge + 1;
                return;
            }
            if (lastMatched) {
                matchIndex = lastMatchIndex;
                nextIndex = lastMatchIndex + 1;
            } else {
                matchIndex = 0;
                nextIndex = 1;
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
