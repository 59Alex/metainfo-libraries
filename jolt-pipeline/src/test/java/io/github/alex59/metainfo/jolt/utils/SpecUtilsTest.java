package io.github.alex59.metainfo.jolt.utils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SpecUtilsTest {
    @Test
    void resolvesNestedPlaceholdersAndPreservesStructuredValues() {
        Map<String, Object> input = Map.of("plain", "Hello ${name}", "object", List.of("${details}"));
        Map<String, Object> variables = Map.of("name", "Ada", "details", Map.of("active", true));

        Map<String, Object> result = SpecUtils.resolvePlaceHolders(input, variables);

        assertEquals("Hello Ada", result.get("plain"));
        assertEquals(List.of(Map.of("active", true)), result.get("object"));
    }
}
