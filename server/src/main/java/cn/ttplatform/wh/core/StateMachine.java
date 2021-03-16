package cn.ttplatform.wh.core;

import cn.ttplatform.wh.core.common.ChannelCache;
import cn.ttplatform.wh.domain.command.GetResponseCommand;
import cn.ttplatform.wh.domain.command.SetCommand;
import cn.ttplatform.wh.domain.command.SetResponseCommand;
import cn.ttplatform.wh.domain.entry.LogEntry;
import io.netty.channel.Channel;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.util.HashMap;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:16
 */
public class StateMachine {

    private Node node;
    private Data data = new Data();
    private int lastApplied;

    public void register(Node node) {
        this.node = node;
    }

    public void apply(LogEntry entry) {
        if (entry.getIndex() <= lastApplied) {
            return;
        }
//        if (node.getEntrySize() > 10240) {
//            Schema<Data> schema = RuntimeSchema.getSchema(Data.class);
//            LinkedBuffer buffer = LinkedBuffer.allocate();
//            byte[] content = ProtobufIOUtil.toByteArray(data, schema, buffer);
//            node.getContext().log().generateSnapshot(entry.getIndex(), entry.getTerm(), content);
//        }
        byte[] command = entry.getCommand();
        Schema<SetCommand> setCommandSchema = RuntimeSchema.getSchema(SetCommand.class);
        SetCommand setCommand = new SetCommand();
        ProtobufIOUtil.mergeFrom(command, setCommand, setCommandSchema);
        data.put(setCommand.getKey(), setCommand.getValue());
        lastApplied = entry.getIndex();
        Channel channel = ChannelCache.getChannel(setCommand.getId());
        channel.writeAndFlush(SetResponseCommand.builder().id(setCommand.getId()).result(true).build())
            .addListener(future -> {
                if (future.isSuccess()) {
                    node.getPendingTasks(entry.getIndex()).forEach(task -> {
                        Channel getChannel = ChannelCache.getChannel(task.getId());
                        getChannel.writeAndFlush(
                            GetResponseCommand.builder().id(task.getId()).value(data.get(task.getKey()))
                                .build());
                    });
                }
            });
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public byte[] getSnapshotData() {
        Schema<Data> schema = RuntimeSchema.getSchema(Data.class);
        LinkedBuffer buffer = LinkedBuffer.allocate();
        return ProtobufIOUtil.toByteArray(data, schema, buffer);
    }

    public static class Data extends HashMap<String, String> {

    }

}
