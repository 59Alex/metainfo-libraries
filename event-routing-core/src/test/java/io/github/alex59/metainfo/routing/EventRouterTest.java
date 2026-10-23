package io.github.alex59.metainfo.routing;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventRouterTest {
    @Test
    void routesOnlyEnabledMatchingTopicsAndPreservesSourcePosition() {
        EventRouter router = new EventRouter(List.of(
                new TopicRoute("first", "orders", "orders-audit", true),
                new TopicRoute("disabled", "orders", "unused", false),
                new TopicRoute("other", "payments", "payment-audit", true)));
        StreamEvent<String, String> event = new StreamEvent<>("orders", 2, 42, "id", "payload", Map.of("trace", "a"));

        List<Dispatch<String, String>> dispatches = router.route(event);

        assertEquals(1, dispatches.size());
        assertEquals("orders-audit", dispatches.get(0).destinationTopic());
        assertSame(event, dispatches.get(0).source());
        assertEquals(42, dispatches.get(0).source().offset());
    }
}
