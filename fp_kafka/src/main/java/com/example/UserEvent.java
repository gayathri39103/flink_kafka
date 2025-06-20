package com.example;

public class UserEvent {
    public String user_id;
    public String action;
    public long timestamp;

    // Required: empty constructor for Flink
    public UserEvent() {}

    public UserEvent(String user_id, String action, long timestamp) {
        this.user_id = user_id;
        this.action = action;
        this.timestamp = timestamp;
    }
}
