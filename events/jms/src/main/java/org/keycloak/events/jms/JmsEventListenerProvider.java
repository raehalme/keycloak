package org.keycloak.events.jms;

import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.logging.Logger;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.util.JsonSerialization;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author <a href="mailto:thomas.raehalme@aitiofinland.com">Thomas Raehalme</a>
 */
public class JmsEventListenerProvider implements EventListenerProvider {

    private static final Logger log = Logger.getLogger(JmsEventListenerProvider.class);

    private String connectionFactoryName;
    private Set<EventType> includedEvents;
    private String eventDestinationName;
    private String adminEventDestinationName;

    public JmsEventListenerProvider(String connectionFactoryName, Set<EventType> includedEvents) {
        this.connectionFactoryName = connectionFactoryName;
        this.includedEvents = includedEvents;
    }

    public String getEventDestinationName() {
        return eventDestinationName;
    }

    public void setEventDestinationName(String eventDestinationName) {
        this.eventDestinationName = eventDestinationName;
    }

    public String getAdminEventDestinationName() {
        return adminEventDestinationName;
    }

    public void setAdminEventDestinationName(String adminEventDestinationName) {
        this.adminEventDestinationName = adminEventDestinationName;
    }

    @Override
    public void onEvent(Event event) {
        if (eventDestinationName == null) {
            log.debug("No JMS destination for events");
            return;
        }

        EventType eventType = event.getType();
        if (includedEvents.contains(eventType)) {
            log.debugf("Sending JMS message for event of type %s", eventType);
            Map<String,String> props = new HashMap();
            props.put("type", Event.class.getName());
            props.put("realmId", event.getRealmId());
            props.put("userId", event.getUserId());
            props.put("eventType", eventType.name());
            sendMessage(eventDestinationName, event, props);
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        if (adminEventDestinationName == null) {
            log.debug("No JMS destination for admin events");
            return;
        }

        log.debugf("Sending JMS message for admin event");
        Map<String,String> props = new HashMap();
        props.put("type", AdminEvent.class.getName());
        props.put("realmId", event.getRealmId());
        props.put("operationType", event.getOperationType().name());
        sendMessage(adminEventDestinationName, event, props);
    }

    protected void sendMessage(String destinationName, Object event, Map<String,String> properties) {
        try {
            ConnectionFactory connectionFactory = lookup(connectionFactoryName);
            Destination destination = lookup(destinationName);
            try (Connection connection = connectionFactory.createConnection()) {
                try (Session session = connection.createSession()) {
                    try (MessageProducer producer = session.createProducer(destination)) {
                        String body = serializeEvent(event);
                        Message message = createMessage(session, body);
                        for (String property : properties.keySet()) {
                            String value = properties.get(property);
                            message.setStringProperty(property, value);
                        }
                        producer.send(message);
                        log.debugf("Sent event as JMS message: %s", body);
                    }
                }
            }
        }
        catch (IOException | JMSException e) {
            log.error("Failed to send JMS message", e);
        }
    }

    private <T> T lookup(String name) {
        try {
            log.debugf("Performing a lookup for %s", name);
            return InitialContext.doLookup(name);
        }
        catch (NamingException e) {
            log.errorf("JNDI lookup failed for '%s'", name);
            throw new RuntimeException(e);
        }

    }

    protected String serializeEvent(Object event) throws IOException {
        ObjectMapper objectMapper = getObjectMapper();
        StringWriter out = new StringWriter();
        objectMapper.writeValue(out, event);
        return out.toString();
    }

    protected Message createMessage(Session session, String json) throws JMSException {
        return session.createTextMessage(json);
    }

    private ObjectMapper getObjectMapper() {
        return JsonSerialization.prettyMapper;
    }

    @Override
    public void close() {
    }
}
