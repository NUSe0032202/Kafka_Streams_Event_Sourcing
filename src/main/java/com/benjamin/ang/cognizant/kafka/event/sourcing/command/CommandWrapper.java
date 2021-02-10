package com.benjamin.ang.cognizant.kafka.event.sourcing.command;


import com.google.gson.Gson;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
//Command Wrapper Class
public class CommandWrapper {
    private String commandType;
    private String commandPayload;
}
