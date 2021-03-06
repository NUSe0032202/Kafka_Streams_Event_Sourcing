package com.benjamin.ang.cognizant.kafka.event.sourcing.events;

import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CustomerCreateCommand;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import lombok.*;

import java.time.LocalDateTime;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CustomerCreatedEvent {
    private String firstName;
    private String lastName;
    private String phoneNumber;
    private String email;
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime createdAt;

    public CustomerCreatedEvent(CustomerCreateCommand command) {
        this.firstName = command.getFirstName();
        this.lastName = command.getLastName();
        this.phoneNumber = command.getPhoneNumber();
        this.email = command.getEmail();
        this.createdAt = LocalDateTime.now();
    }
}
