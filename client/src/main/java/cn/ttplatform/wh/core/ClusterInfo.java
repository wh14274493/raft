package cn.ttplatform.wh.core;

import java.util.Deque;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/4/20 23:12
 */
public class ClusterInfo {

    private String masterId;
    private final Deque<MemberInfo> memberInfos;

    public ClusterInfo(Deque<MemberInfo> memberInfos) {
        this.memberInfos = memberInfos;
        if (memberInfos.isEmpty()) {
            throw new IllegalStateException("cluster info must not be null.");
        }
        this.masterId = memberInfos.peekFirst().getNodeId();
    }

    public synchronized void update(List<MemberInfo> source, String leaderId) {
        if (!leaderId.equals(masterId)) {
            masterId = leaderId;
            memberInfos.clear();
            source.forEach(memberInfo -> {
                if (leaderId.equals(memberInfo.getNodeId())) {
                    memberInfos.offerFirst(memberInfo);
                } else {
                    memberInfos.offerLast(memberInfo);
                }
            });
        }
    }

    public synchronized MemberInfo getMaster() {
        return memberInfos.peekFirst();
    }

    public synchronized void removeInvalidMaster(String masterId) {
        if (this.masterId.equals(masterId)) {
            MemberInfo memberInfo = memberInfos.pollFirst();
            memberInfos.offerLast(memberInfo);
            assert memberInfos.peekFirst() != null;
            this.masterId = memberInfos.peekFirst().getNodeId();
        }
    }

    public synchronized int getSize() {
        return memberInfos.size();
    }
}
