package com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers;

import com.benjamin.ang.cognizant.kafka.event.sourcing.aggregates.CustomerAggregate;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CommandWrapper;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CustomerCreateCommand;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.CustomerCreatedEvent;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.EventWrapper;
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

    //Event Wrapper Serializer/Deserializer
    static public final class EventWrapperSerde extends Serdes.WrapperSerde<EventWrapper> {
        public EventWrapperSerde() {
            super(new JsonSerializer<>(EventWrapper.class)
                    ,new JsonDeserializer<>(EventWrapper.class));
        }
    }

    static public final class CommandWrapperSerde extends Serdes.WrapperSerde<CommandWrapper> {
        public CommandWrapperSerde() {
            super(new JsonSerializer<>(CommandWrapper.class)
                    ,new JsonDeserializer<>(CommandWrapper.class));
        }
    }

    public static Serde<CommandWrapper> CommandWrapper() {return new CustomSerdes.CommandWrapperSerde();}

    public static Serde<EventWrapper> EventWrapper() {return  new CustomSerdes.EventWrapperSerde();}

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
