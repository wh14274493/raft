package cn.ttplatform.wh.core.handler;

import cn.ttplatform.wh.cmd.RedirectCommand;
import cn.ttplatform.wh.core.ClientContext;
import cn.ttplatform.wh.core.MemberInfo;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.support.MessageHandler;
import java.util.List;

/**
 * @author Wang Hao
 * @date 2021/4/20 13:49
 */
public class RedirectCommandHandler implements MessageHandler {

    private final ClientContext context;

    public RedirectCommandHandler(ClientContext context) {
        this.context = context;
    }

    @Override
    public void handle(Message message) {
        RedirectCommand cmd = (RedirectCommand) message;
        String leaderId = cmd.getLeaderId();
        List<MemberInfo> memberInfos = cmd.getMembers();
        context.updateClusterInfo(memberInfos, leaderId);
        context.redirect(cmd.getId());
    }
}
