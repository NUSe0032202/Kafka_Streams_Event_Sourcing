package com.benjamin.ang.cognizant.kafka.event.sourcing.events;


import com.google.gson.Gson;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
//Event Wrapper Class
public class EventWrapper {
    private UUID eventID;
    private String eventType;
    private String eventPayload;
}
