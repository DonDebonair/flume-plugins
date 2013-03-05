package nl.info.flume.serialization;

/**
 * ...
 *
 * @author daan.debie
 */
import com.google.common.base.Charsets;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import nl.info.flume.serialization.SyslogAvroEventSerializer.SyslogEvent;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This class exists to give an idea of how to use the AvroEventWriter
 * and is not intended for inclusion in the Flume core.<br/>
 * Problems with it are:<br/>
 * (1) assumes very little parsing is done at the first hop (more TBD)<br/>
 * (2) no field has been defined for use as a UUID for deduping<br/>
 * (3) tailored to syslog messages but not specific to any application<br/>
 * (4) not efficient about data copying from an implementation perspective<br/>
 * Often, it makes more sense to parse your (meta-)data out of the message part
 * itself and then store that in an application-specific Avro schema.
 */
@Slf4j
public class SyslogAvroEventSerializer
        extends AbstractAvroEventSerializer<SyslogEvent> {

    private static final DateTimeFormatter dateFmt1 =
            DateTimeFormat.forPattern("MMM dd HH:mm:ss").withZoneUTC();
    private static final DateTimeFormatter dateFmt2 =
            DateTimeFormat.forPattern("MMM  d HH:mm:ss").withZoneUTC();

    private static final Schema SCHEMA = new Schema.Parser().parse("" +
            "{ \"type\": \"record\", \"name\": \"SyslogEvent\", \"namespace\": \"org.apache.flume\", \"fields\": [" +
            " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
            " { \"name\": \"original\",  \"type\": \"string\" }," +
            " { \"name\": \"timestamp\", \"type\": \"long\" }," +
            " { \"name\": \"datetime\",  \"type\": \"string\" }," +
            " { \"name\": \"hostname\",  \"type\": \"string\" }," +
            " { \"name\": \"message\",   \"type\": \"string\" }" +
            " ] }");

    private final OutputStream out;

    public SyslogAvroEventSerializer(OutputStream out) throws IOException {
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
    protected SyslogEvent convert(Event event) {
        SyslogEvent sle = new SyslogEvent();

        // Stringify body so it's easy to parse.
        // This is a pretty inefficient way to do it.
        String logline = new String(event.getBody(), Charsets.UTF_8);
        Map<String, String> headers = event.getHeaders();
        sle.setOriginal(logline);

        // parser read pointer
        int seek = 0;

        // parse the timestamp
        String timestampStr = logline.substring(seek, seek + 15);
        long ts = parseRfc3164Date(timestampStr);
        if (ts != 0) {
            sle.setTimestamp(ts);
            sle.setDatetime(new DateTime(ts).toString("yyyy-MM-dd HH:mm:ss"));
            seek += 15 + 1; // space after timestamp
        }

        // parse the hostname
        int nextSpace = logline.indexOf(' ', seek);
        if (nextSpace > -1) {
            String hostname = logline.substring(seek, nextSpace);
            sle.setHostname(hostname);
            if(headers.containsKey("host")) {
                headers.put("host", hostname);
            }
            seek = nextSpace + 1;
        }

        // everything else is the message
        String actualMessage = logline.substring(seek);
        sle.setMessage(actualMessage);

        sle.setHeaders(headers);
        // log.debug("Serialized event as: {}", sle);

        return sle;
    }

    /**
     * Returns epoch time in millis, or 0 if the string cannot be parsed.
     * We use two date formats because the date spec in rfc3164 is kind of weird.
     * <br/>
     * <b>Warning:</b> logic is used here to determine the year even though it's
     * not part of the timestamp format, and we assume that the machine running
     * Flume has a clock that is at least close to the same day as the machine
     * that generated the event. We also assume that the event was generated
     * recently.
     */
    private static long parseRfc3164Date(String in) {
        DateTime date = null;
        try {
            date = dateFmt1.parseDateTime(in);
        } catch (IllegalArgumentException e) {
            // ignore the exception, we act based on nullity of date object
            // log.debug("Date parse failed on ({}), trying single-digit date", in);
        }

        if (date == null) {
            try {
                date = dateFmt2.parseDateTime(in);
            } catch (IllegalArgumentException e) {
                // ignore the exception, we act based on nullity of date object
                log.debug("2nd date parse failed on ({}), unknown date format", in);
            }
        }

        // hacky stuff to try and deal with boundary cases, i.e. new year's eve.
        // rfc3164 dates are really dumb.
        // NB: cannot handle replaying of old logs or going back to the future
        if (date != null) {
            DateTime now = new DateTime();
            int year = now.getYear();
            DateTime corrected = date.withYear(year);

            // flume clock is ahead or there is some latency, and the year rolled
            if (corrected.isAfter(now) && corrected.minusMonths(1).isAfter(now)) {
                corrected = date.withYear(year - 1);
                // flume clock is behind and the year rolled
            } else if (corrected.isBefore(now) && corrected.plusMonths(1).isBefore(now)) {
                corrected = date.withYear(year + 1);
            }
            date = corrected;
        }

        if (date == null) {
            log.warn("Parsing error in date: {}", in);
            return 0;
        }

        return date.getMillis();
    }

    public static class Builder implements EventSerializer.Builder {

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            SyslogAvroEventSerializer writer = null;
            try {
                writer = new SyslogAvroEventSerializer(out);
                writer.configure(context);
            } catch (IOException e) {
                log.error("Unable to parse schema file. Exception follows.", e);
            }
            return writer;
        }

    }

    // This class would ideally be generated from the avro schema file,
    // but we are letting reflection do the work instead.
    // There's no great reason not to let Avro generate it.
    @Getter
    @Setter
    public static class SyslogEvent {
        private Map<String, String> headers;
        private String original = "";
        private long timestamp;
        private String datetime = "";
        private String hostname = "";
        private String message = "";

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{ Timestamp: ").append(timestamp).append(", ");
            builder.append(" DateTime: ").append(datetime).append(", ");
            builder.append(" Hostname: ").append(hostname).append(", ");
            builder.append(" Message: \"").append(message).append("\" }");
            return builder.toString();
        }
    }
}