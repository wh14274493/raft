package cn.ttplatform.wh.core.support;

import cn.ttplatform.wh.core.ClientContext;
import cn.ttplatform.wh.core.connector.Connection;
import cn.ttplatform.wh.core.connector.NioConnector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/20 21:52
 */
@Slf4j
public class ConnectionPool {

    private final AtomicInteger index;
    private final int capacity;
    private final NioConnector connector;
    private final Connection[] connections;
    private final Object[] locks;

    public ConnectionPool(ClientContext context) {
        this.connector = context.getConnector();
        this.capacity = context.getProperties().getMaxConnections();
        this.connections = new Connection[capacity];
        this.locks = new Object[capacity];
        for (int i = 0; i < capacity; i++) {
            locks[i] = new Object();
        }
        this.index = new AtomicInteger();
    }

    public Connection getConnection() {
        int idx = index.getAndIncrement();
        Connection connection = connections[idx % capacity];
        if (connection == null || !connection.isValid()) {
            synchronized (locks[idx % capacity]) {
                if (connection == null || !connection.isValid()) {
                    connection = connector.createConnection();
                    connections[idx % capacity] = connection;
                }
            }
        }
        return connection;
    }

}
