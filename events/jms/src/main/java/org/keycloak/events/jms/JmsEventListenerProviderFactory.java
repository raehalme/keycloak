package org.keycloak.events.jms;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.events.EventType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:thomas.raehalme@aitiofinland.com">Thomas Raehalme</a>
 */
public class JmsEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger log = Logger.getLogger(JmsEventListenerProviderFactory.class);

    public static final String DEFAULT_JNDI_CONNECTION_FACTORY = "java:/ConnectionFactory";

    private static final Set<EventType> SUPPORTED_EVENTS = new HashSet();
    private static final String DEFAULT_JNDI_EVENT_DESTINATION = "java:/jms/KeycloakEvents";

    static {
        Collections.addAll(SUPPORTED_EVENTS, EventType.UPDATE_PROFILE);
    }

    private String connectionFactoryName;
    private String eventDestinationName;
    private String adminEventDestinationName;
    private Set<EventType> includedEvents = new HashSet();

    @Override
    public EventListenerProvider create(KeycloakSession keycloakSession) {
        JmsEventListenerProvider provider = new JmsEventListenerProvider(connectionFactoryName, includedEvents);
        provider.setEventDestinationName(eventDestinationName);
        provider.setAdminEventDestinationName(adminEventDestinationName);
        return provider;
    }

    @Override
    public void init(Config.Scope config) {
        connectionFactoryName = config.get("connection-factory-name");
        if (connectionFactoryName == null) {
            connectionFactoryName = DEFAULT_JNDI_CONNECTION_FACTORY;
        }

        eventDestinationName = config.get("event-destination-name");
        if (eventDestinationName == null) {
            eventDestinationName = DEFAULT_JNDI_EVENT_DESTINATION;
        }

        adminEventDestinationName = config.get("admin-event-destination-name");

        String[] include = config.getArray("include-events");
        if (include != null) {
            for (String i : include) {
                includedEvents.add(EventType.valueOf(i.toUpperCase()));
            }
        } else {
            includedEvents.addAll(SUPPORTED_EVENTS);
        }

        String[] exclude = config.getArray("exclude-events");
        if (exclude != null) {
            for (String e : exclude) {
                includedEvents.remove(EventType.valueOf(e.toUpperCase()));
            }
        }
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {

    }

    @Override
    public void close() {
    }

    @Override
    public String getId() {
        return "jms";
    }

}
