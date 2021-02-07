package com.benjamin.ang.cognizant.kafka.event.sourcing.aggregates;


import com.benjamin.ang.cognizant.kafka.event.sourcing.events.CustomerCreatedEvent;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class CustomerAggregate {
    private String firstName;
    private String lastName;
    private String phoneNumber;
    private String email;

    public CustomerAggregate(CustomerCreatedEvent event) {
        this.firstName = event.getFirstName();
        this.lastName = event.getLastName();
        this.phoneNumber = event.getPhoneNumber();
        this.email = event.getEmail();
    }
}
