package com.blue.flink.demo;

import java.sql.Timestamp;

public class Event {
    public String id;
    public String url;
    public Long ts;

    public Event(String id, String url, long ts) {

        this.id = id;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", url='" + url + '\'' +
                ", ts='" + ts + '\'' +
                ", ts_str='" + new Timestamp(ts) + '\'' +
                '}';
    }
}