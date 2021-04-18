package cn.ttplatform.wh.core;

import cn.ttplatform.wh.constant.ExceptionMessage;
import cn.ttplatform.wh.exception.ClusterConfigException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:46
 */
public class Cluster {

    private static final int MIN_CLUSTER_SIZE = 3;
    private final String selfId;
    private final Map<String, ClusterMember> memberMap;
    private int activeSize;

    public Cluster(ClusterMember member) {
        this(Collections.singleton(member), member.getNodeId());
    }

    public Cluster(Collection<ClusterMember> members, String selfId) {
        this.memberMap = buildMap(members);
        this.activeSize = memberMap.size();
        this.selfId = selfId;
    }

    public void resetReplicationStates(int nextIndex) {
        memberMap.values().forEach(member -> member.resetReplicationState(nextIndex));
    }

    private Map<String, ClusterMember> buildMap(Collection<ClusterMember> members) {
        Map<String, ClusterMember> map = new HashMap<>((int) (members.size() / 0.75f + 1));
        members.forEach(member -> map.put(member.getNodeId(), member));
        return map;
    }

    public ClusterMember find(String nodeId) {
        return memberMap.get(nodeId);
    }

    public int countAll() {
        return memberMap.size();
    }

    public int countOfActive() {
        return activeSize;
    }

    /**
     * List all endpoint except self
     *
     * @return endpoint list
     */
    public List<ClusterMember> listAllEndpointExceptSelf() {
        List<ClusterMember> result = new ArrayList<>();
        memberMap.forEach((s, member) -> {
            if (!selfId.equals(s)) {
                result.add(member);
            }
        });
        return result;
    }

    public void remove(String nodeId) {
        memberMap.remove(nodeId);
        activeSize--;
        if (activeSize < MIN_CLUSTER_SIZE) {
            throw new ClusterConfigException(ExceptionMessage.CLUSTER_SIZE_ERROR);
        }
    }

    public int getNewCommitIndex() {
        List<ClusterMember> members = listAllEndpointExceptSelf();
        int size = members.size();
        if (size < MIN_CLUSTER_SIZE - 1) {
            throw new ClusterConfigException(ExceptionMessage.CLUSTER_SIZE_ERROR);
        }
        Collections.sort(members);
        return members.get(size >> 1).getMatchIndex();
    }
}
