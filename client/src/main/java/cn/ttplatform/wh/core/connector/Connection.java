package cn.ttplatform.wh.core.connector;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.core.support.Future;
import cn.ttplatform.wh.core.support.GenericListener;
import cn.ttplatform.wh.core.support.RequestRecord;
import io.netty.channel.Channel;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/20 21:50
 */
@Slf4j
public class Connection {

    private final BlockingDeque<RequestRecord<?>> requestRecords;
    private final MemberInfo memberInfo;
    private final Channel channel;

    public Connection(MemberInfo memberInfo, Channel channel) {
        this.memberInfo = memberInfo;
        this.channel = channel;
        this.requestRecords = new LinkedBlockingDeque<>();
        registerCloseListener();
    }

    private void registerCloseListener() {
        channel.closeFuture().addListener(future -> {
            while (!requestRecords.isEmpty()) {
                requestRecords.pollFirst().cancel();
            }
        });
    }

    public Future<Command> send(Command cmd) {
        Future<Command> future = new Future<>();
        RequestRecord<Command> requestRecord = new RequestRecord<>(cmd, future);
        requestRecords.offerLast(requestRecord);
        channel.writeAndFlush(cmd).addListener(f -> {
            if (f.isSuccess()) {
                log.debug("send message {} success", cmd);
            } else {
                requestRecord.cancel();
                requestRecords.remove(requestRecord);
                log.debug("send message {} failed", cmd);
            }
        });
        future.addListener(() -> requestRecords.remove(requestRecord));
        return future;
    }

    public boolean isValid() {
        return channel.isOpen();
    }
}
