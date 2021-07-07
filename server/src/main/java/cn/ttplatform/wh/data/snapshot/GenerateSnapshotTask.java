package cn.ttplatform.wh.data.snapshot;

import cn.ttplatform.wh.GlobalContext;
import cn.ttplatform.wh.StateMachine;
import cn.ttplatform.wh.data.DataManager;
import cn.ttplatform.wh.exception.OperateFileException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/5/30 23:53
 */
@Slf4j
public class GenerateSnapshotTask implements Runnable {

    private final GlobalContext context;
    private final int lastIncludeIndex;
    private final SnapshotBuilder snapshotBuilder;

    public GenerateSnapshotTask(GlobalContext context, int lastIncludeIndex) {
        this.context = context;
        this.lastIncludeIndex = lastIncludeIndex;
        DataManager dataManager = context.getDataManager();
        this.snapshotBuilder = new SnapshotBuilder(context.getProperties().getBase(), context.getByteBufferPool(),
                dataManager.getSnapshotFileMetadataRegion(), dataManager.getGeneratingSnapshotFileMetadataRegion());
    }

    @Override
    public void run() {
        log.info("log file size more than SnapshotGenerateThreshold then generate snapshot");
        StateMachine stateMachine = context.getStateMachine();
        byte[] snapshotData = stateMachine.generateSnapshotData();
        log.info("had generated snapshot data, size is {}.", snapshotData.length);
        try {
            DataManager dataManager = context.getDataManager();
            int lastIncludeTerm = dataManager.getTermOfLog(lastIncludeIndex);
            snapshotBuilder.setBaseInfo(lastIncludeIndex, lastIncludeTerm, context.getNode().getSelfId());
            snapshotBuilder.append(snapshotData);
            dataManager.completeBuildingSnapshot(snapshotBuilder, lastIncludeTerm, lastIncludeIndex);
            log.info("generate snapshot that contains {} bytes lastIncludeIndex is {} and lastIncludeTerm is {}",
                    snapshotData.length, lastIncludeIndex, lastIncludeTerm);
        } catch (OperateFileException e) {
            log.error("generate snapshot failed, error detail is {}.", e.getMessage());
            stateMachine.stopGenerating();
        }
    }

}
