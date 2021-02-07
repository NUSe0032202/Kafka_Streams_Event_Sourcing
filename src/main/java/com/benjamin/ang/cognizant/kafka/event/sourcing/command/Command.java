package com.benjamin.ang.cognizant.kafka.event.sourcing.command;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Command {
    private String commandType;
}
