package nl.info.flume.serialization;

import com.google.common.base.Charsets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import nl.info.flume.serialization.JavaLogAvroEventSerializer.JavaEvent;
import org.apache.flume.serialization.EventSerializer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * ...
 *
 * @author daan.debie
 */
@Slf4j
public class JavaLogAvroEventSerializer extends AbstractAvroEventSerializer<JavaEvent> {

    private static final DateTimeFormatter dateFmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private static final Schema SCHEMA = new Schema.Parser().parse("" +
            "{ \"type\": \"record\", \"name\": \"JavaEvent\", \"namespace\": \"nl.info.flume\", \"fields\": [" +
            " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
            " { \"name\": \"original\",  \"type\": \"string\" }," +
            " { \"name\": \"timestamp\", \"type\": \"long\" }," +
            " { \"name\": \"datetime\",  \"type\": \"string\" }," +
            " { \"name\": \"loglevel\",  \"type\": \"string\" }," +
            " { \"name\": \"appservername\",  \"type\": \"string\" }," +
            " { \"name\": \"classname\",  \"type\": \"string\" }," +
            " { \"name\": \"thread\",  \"type\": \"string\" }," +
            " { \"name\": \"message\",   \"type\": \"string\" }" +
            " ] }");

    private final OutputStream out;

    public JavaLogAvroEventSerializer(OutputStream out) throws IOException {
        this.out = out;
    }

    @Override
    protected OutputStream getOutputStream() {
        return out;
    }

    @Override
    protected Schema getSchema() {
        return SCHEMA;
    }

    // very simple rfc3164 parser
    @Override
    protected JavaEvent convert(Event event) {
        JavaEvent jve = new JavaEvent();

        // Stringify body so it's easy to parse.
        // This is a pretty inefficient way to do it.
        String logline = new String(event.getBody(), Charsets.UTF_8);
        jve.setHeaders(event.getHeaders());
        jve.setOriginal(logline);

        // parser read pointer
        int seek = 0;

        // Put seek on character beyond first pipe
        int nextMarker = logline.indexOf('|', seek);
        seek = nextMarker + 1;

        // Look for datetime and parse it
        nextMarker = logline.indexOf('|', seek);
        if (nextMarker > -1) {
            String dateTime = logline.substring(seek, nextMarker);
            try {
                DateTime dt = dateFmt.parseDateTime(dateTime);
                jve.setDatetime(dt.toString("yyyy-MM-dd HH:mm:ss"));
                jve.setTimestamp(dt.getMillis());
            } catch (IllegalArgumentException e) {
                // ignore the exception gracefully
                log.warn("Date parse failed on ({}). Skipping...", dateTime);
            }

            seek = nextMarker + 1;
        }

        // look for loglevel
        nextMarker = logline.indexOf('|', seek);
        if (nextMarker > -1) {
            String logLevel = logline.substring(seek, nextMarker);
            jve.setLoglevel(logLevel);
            seek = nextMarker + 1;
        }

        // look for appservername
        nextMarker = logline.indexOf('|', seek);
        if (nextMarker > -1) {
            String appServerName = logline.substring(seek, nextMarker);
            jve.setAppservername(appServerName);
            seek = nextMarker + 1;
        }

        // look for classname
        nextMarker = logline.indexOf('|', seek);
        if (nextMarker > -1) {
            String classname = logline.substring(seek, nextMarker);
            jve.setClassname(classname);
            seek = nextMarker + 1;
        }

        // look for thread
        nextMarker = logline.indexOf('|', seek);
        if (nextMarker > -1) {
            String thread = logline.substring(seek, nextMarker);
            jve.setThread(thread);
            seek = nextMarker + 1;
        }

        // everything else is the message, up to the last |#]
        nextMarker = logline.indexOf('|', seek);
        String actualMessage;
        if(nextMarker > -1) {
            actualMessage = logline.substring(seek, nextMarker);
        } else {
            actualMessage = logline.substring(seek);
        }

        jve.setMessage(actualMessage);

        // log.debug("Serialized event as: {}", jve);

        return jve;
    }

    public static class Builder implements EventSerializer.Builder {

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            JavaLogAvroEventSerializer writer = null;
            try {
                writer = new JavaLogAvroEventSerializer(out);
                writer.configure(context);
            } catch (IOException e) {
                log.error("Unable to parse schema file. Exception follows.", e);
            }
            return writer;
        }

    }

    @Getter
    @Setter
    public static class JavaEvent {
        private Map<String, String> headers;
        private String original = "";
        private long timestamp;
        private String datetime = "";
        private String loglevel = "";
        private String appservername = "";
        private String classname = "";
        private String thread = "";
        private String message = "";

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{ Timestamp: ").append(timestamp).append(", ");
            builder.append(" DateTime: ").append(datetime).append(", ");
            builder.append(" LogLevel: ").append(loglevel).append(", ");
            builder.append(" ApplicationServerName: ").append(appservername).append(", ");
            builder.append(" ClassName: ").append(classname).append(", ");
            builder.append(" Thread: ").append(thread).append(", ");
            builder.append(" Message: \"").append(message).append("\" }");
            return builder.toString();
        }
    }
}
