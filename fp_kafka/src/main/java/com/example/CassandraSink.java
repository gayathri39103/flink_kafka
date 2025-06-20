package com.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;

public class CassandraSink implements SinkFunction<UserEvent> {
    private transient CqlSession session;

    @Override
    public void invoke(UserEvent event, Context context) {
        if (session == null) {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                    .withKeyspace("analytics")
                    .withLocalDatacenter("datacenter1")
                    .build();
        }

        session.execute("INSERT INTO user_actions (user_id, timestamp, action) VALUES (?, ?, ?)",
                event.user_id, event.timestamp, event.action);
    }
}
