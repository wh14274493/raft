package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.cmd.Command;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author Wang Hao
 * @date 2021/4/20 15:49
 */
@Data
@Builder
@AllArgsConstructor
public class RequestRecord<T> {

    private Command command;
    private Future<T> future;

    public String getId() {
        return command.getId();
    }

    public void cancel() {
        future.interruptAllWaiters();
    }
}
