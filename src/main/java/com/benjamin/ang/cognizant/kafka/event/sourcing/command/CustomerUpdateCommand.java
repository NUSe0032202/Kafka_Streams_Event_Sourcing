package com.benjamin.ang.cognizant.kafka.event.sourcing.command;

import lombok.*;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CustomerUpdateCommand extends Command {
    private UUID   id;
    private String firstName;
    private String lastName;
    private String phoneNumber;
    private String email;
}
