package com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers;

import com.benjamin.ang.cognizant.kafka.event.sourcing.aggregates.CustomerAggregate;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CustomerCreateCommand;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.CustomerCreatedEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class CustomSerdes {

    static public final class EventSerde extends Serdes.WrapperSerde<CustomerCreatedEvent> {
        public EventSerde() {
            super(new JsonSerializer<>(CustomerCreatedEvent.class)
                    ,new JsonDeserializer<>(CustomerCreatedEvent.class));
        }
    }

    static public final class AggregateSerde extends Serdes.WrapperSerde<CustomerAggregate> {
        public AggregateSerde() {
            super(new JsonSerializer<>(CustomerAggregate.class)
                    ,new JsonDeserializer<>(CustomerAggregate.class));
        }
    }

    static public final class CommandSerde extends Serdes.WrapperSerde<CustomerCreateCommand> {
        public CommandSerde() {
            super(new JsonSerializer<>(CustomerCreateCommand.class)
                    ,new JsonDeserializer<>(CustomerCreateCommand.class));
        }
    }

    public static Serde<CustomerCreatedEvent> Event () {
        return new CustomSerdes.EventSerde();
    }

    public static Serde<CustomerAggregate> Aggregate () {
        return new CustomSerdes.AggregateSerde();
    }

    public static Serde<CustomerCreateCommand> Command() {
        return new CustomSerdes.CommandSerde();
    }
}
