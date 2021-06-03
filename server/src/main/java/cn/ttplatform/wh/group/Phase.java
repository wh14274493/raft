package cn.ttplatform.wh.group;

/**
 * @author Wang Hao
 * @date 2021/4/26 11:15
 */
public enum Phase {
    // Syncing logs to the newly added node
    SYNCING,
    /*
     Log synchronization has been completed, and Coldnew logs have been added to the cluster.
     At this stage, all logs need to pass the agreement of the majority of the new configuration
     and the majority of the old configuration at the same time before they can be submitted.
     */
    OLD_NEW,
    /*
    Coldnew logs have been committed, and Cnew logs have been added to the cluster. At this stage,
    all logs can be submitted only with the approval of the majority of the new configuration
     */
    NEW,
    /*
    Cnew logs have been committed, the cluster is stable. At this time, nodes that are no longer
    in the new configuration will automatically go offline
     */
    STABLE

}
