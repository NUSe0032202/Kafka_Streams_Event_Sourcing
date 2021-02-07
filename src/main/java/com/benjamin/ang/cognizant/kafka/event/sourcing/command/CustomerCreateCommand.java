package com.benjamin.ang.cognizant.kafka.event.sourcing.command;


import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CustomerCreateCommand {
    private String firstName;
    private String lastName;
    private String phoneNumber;
    private String email;
}
