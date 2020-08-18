package cn.ttplatform.lc.node;

import cn.ttplatform.lc.constant.ExceptionMessage;
import cn.ttplatform.lc.exception.ClusterConfigException;

import java.util.*;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:46
 */
public class Cluster {

    private final int MIN_CLUSTER_SIZE = 3;
    private final String selfId;
    private final Map<String, ClusterMember> memberMap;

    public Cluster(ClusterMember member) {
        this(Collections.singleton(member), member.getNodeId());
    }

    public Cluster(Collection<ClusterMember> members, String selfId) {
        this.memberMap = buildMap(members);
        this.selfId = selfId;
    }

    private Map<String, ClusterMember> buildMap(Collection<ClusterMember> members) {
        Map<String, ClusterMember> map = new HashMap<>((int) (members.size() / 0.75f + 1));
        members.forEach(member -> map.put(member.getNodeId(), member));
        return map;
    }

    public ClusterMember find(String nodeId) {
        return memberMap.get(nodeId);
    }

    /**
     * List all endpoint except self
     *
     * @return endpoint list
     */
    public List<ClusterMember> listAllEndpointExceptSelf() {
        List<ClusterMember> result = new ArrayList<>();
        memberMap.forEach((s, member) -> {
            if (!Objects.equals(selfId, s)) {
                result.add(member);
            }
        });
        return result;
    }

    public void remove(String nodeId) {
        memberMap.remove(nodeId);
    }

    public int getNeedToCommitIndex() {
        List<ClusterMember> members = listAllEndpointExceptSelf();
        int size = members.size();
        if (size < MIN_CLUSTER_SIZE - 1) {
            throw new ClusterConfigException(ExceptionMessage.CLUSTER_SIZE_ERROR);
        }
        members.sort(IndexComparator.INSTANCE);
        return size % 2 == 0 ? members.get(size / 2 - 1).getMatchIndex() : members.get(size / 2).getMatchIndex();
    }

    public static class IndexComparator implements Comparator<ClusterMember> {

        static final IndexComparator INSTANCE = new IndexComparator();

        @Override
        public int compare(ClusterMember o1, ClusterMember o2) {
            return o2.getMatchIndex() - o1.getMatchIndex();
        }
    }
}
