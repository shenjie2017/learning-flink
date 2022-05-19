package com.blue.mall.data.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private String id;
    private String url;
    private String time;
    private long ts;
}
