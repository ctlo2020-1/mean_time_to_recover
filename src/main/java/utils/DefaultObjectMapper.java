package utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.log4j.Log4j2;

import static com.fasterxml.jackson.databind.DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS;

@Log4j2
public class DefaultObjectMapper {

    private static volatile ObjectMapper objectMapper = null;
    private static final Object lock = new Object();

    /**
     * @deprecated use {@link DefaultObjectMapper#get()}
     */
    @VisibleForTesting // otherwise use CommonConfig.objectMapper() bean
    @Deprecated
    public static ObjectMapper getObjectMapper() {
        return get();
    }

    public static ObjectMapper get() {
        if (objectMapper != null) {
            return objectMapper;
        }
        synchronized (lock) {
            if (objectMapper != null) {
                return objectMapper;
            }
            objectMapper = buildObjectMapper();
            return objectMapper;
        }
    }

    private static ObjectMapper buildObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Don't throw an exception when json has extra fields you are
        // not serializing on. This is useful when you want to use a pojo
        // for deserialization and only care about a portion of the json
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        mapper.registerModule(new JavaTimeModule()); // support for Instant
        // serialize and deserialize Instant to epoch in ms (and not nano) similarly to Date - making them interchangeable
        mapper.disable(WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        mapper.disable(READ_DATE_TIMESTAMPS_AS_NANOSECONDS);
        return mapper;
    }

    public static String writeAsPrettyJson(Object o) {
        try {
            return get().writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            log.warn("Could not write value as JSON string", e);
        }
        return null;
    }

    public static void prettyPrint(Object o) {
        System.out.println(writeAsPrettyJson(o));
    }
}
