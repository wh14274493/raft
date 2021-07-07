package cn.ttplatform.wh.log4j;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Layout;

/**
 * @author Wang Hao
 * @date 2021/5/26 23:15
 */
@Slf4j
public class DailyRollingFileAppenderWrapper extends DailyRollingFileAppender {

    public DailyRollingFileAppenderWrapper() {
        super();
        addShutdownHook();
    }

    public DailyRollingFileAppenderWrapper(Layout layout, String filename, String datePattern) throws IOException {
        super(layout, filename, datePattern);
        addShutdownHook();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            qw.flush();
            log.debug("flush {} buffer.", qw.toString());
        }, "log4j-thread"));
    }
}
