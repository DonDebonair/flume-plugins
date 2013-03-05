package nl.info.flume.serialization;

/**
 * ...
 *
 * @author daan.debie
 */
import com.google.common.base.Charsets;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import nl.info.flume.serialization.SyslogAvroEventSerializer.SyslogEvent;
import static nl.info.flume.serialization.SyslogAvroEventSerializer.Constants.*;
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

    private final String path;
    private final String customerHeader;
    private final String hostHeader;
    private static final String UNKNOWN_CUSTOMER = "UNKNOWN";
    private Map<String, String> hostToCustomerMap;

    private static final DateTimeFormatter dateFmt1 =
            DateTimeFormat.forPattern("MMM dd HH:mm:ss").withZoneUTC();
    private static final DateTimeFormatter dateFmt2 =
            DateTimeFormat.forPattern("MMM  d HH:mm:ss").withZoneUTC();

    private static final Schema SCHEMA = new Schema.Parser().parse("" +
            "{ \"type\": \"record\", \"name\": \"SyslogEvent\", \"namespace\": \"nl.info.flume\", \"fields\": [" +
            " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
            " { \"name\": \"original\",  \"type\": \"string\" }," +
            " { \"name\": \"timestamp\", \"type\": \"long\" }," +
            " { \"name\": \"datetime\",  \"type\": \"string\" }," +
            " { \"name\": \"hostname\",  \"type\": \"string\" }," +
            " { \"name\": \"message\",   \"type\": \"string\" }" +
            " ] }");

    private final OutputStream out;

    public SyslogAvroEventSerializer(OutputStream out, String path, String customerHeader, String hostHeader) throws IOException {
        this.out = out;
        this.path = path;
        this.customerHeader = customerHeader;
        this.hostHeader = hostHeader;
    }

    @Override
    public void configure(Context context) {
        super.configure(context);
        File customerHostsFile = new File(path);
        try {
            hostToCustomerMap = buildCustomerToHostMapFromFile(customerHostsFile);
        } catch (FileNotFoundException e) {
            log.warn("Could not find file: {}", path);
        } catch (IOException e) {
            log.warn("File IO error in: {}", path);
        }
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
        boolean expectedFormat = false;

        // Stringify body so it's easy to parse.
        // This is a pretty inefficient way to do it.
        String logline = new String(event.getBody(), Charsets.UTF_8);
        Map<String, String> headers = event.getHeaders();
        sle.setOriginal(logline);

        // This could be an unknown format
        if(logline.length() < 15) {
            sle.setHeaders(headers);
            return sle;
        }

        // parser read pointer
        int seek = 0;

        // parse the timestamp
        String timestampStr = logline.substring(seek, seek + 15);
        long ts = parseRfc3164Date(timestampStr);
        if (ts != 0) {
            sle.setTimestamp(ts);
            sle.setDatetime(new DateTime(ts).toString("yyyy-MM-dd HH:mm:ss"));
            seek += 15 + 1; // space after timestamp
            expectedFormat = true;
        }

        // parse the hostname
        int nextSpace = logline.indexOf(' ', seek);
        String hostname;
        if (nextSpace > -1 && expectedFormat) {
            hostname = logline.substring(seek, nextSpace);
            sle.setHostname(hostname);
            headers.put(hostHeader, hostname);
            seek = nextSpace + 1;
        } else {
            hostname = headers.get(hostHeader);
        }

        // Get customer
        headers.put(customerHeader, UNKNOWN_CUSTOMER);
        if(hostname != null) {
            String customer = hostToCustomerMap.get(hostname.toLowerCase());
            if(customer != null) {
                headers.put(customerHeader, customer);
            }
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

    public static Map<String, String> buildCustomerToHostMapFromFile(File file) throws FileNotFoundException, IOException {

        Map<String, String> hostToCustomerMap = new HashMap<String, String>();
        String line;
        String customer;
        String[] hosts;
        Pattern pattern = Pattern.compile("(?i)^(.*):(.*)$");

        FileReader fileReader = new FileReader(file);

        BufferedReader bufferedReader = new BufferedReader(fileReader);
        line = bufferedReader.readLine();
        while(line != null) {
            Matcher m = pattern.matcher(line);
            if (m.matches()) {
                customer = m.group(1);
                hosts = m.group(2).trim().split("\\s");
                for(String host : hosts) {
                    host = host.trim().toLowerCase();
                    if(host.contains(".")) {
                        host = host.substring(0, host.indexOf("."));
                    }
                    hostToCustomerMap.put(host, customer);
                }
            }
            line = bufferedReader.readLine();
        }
        bufferedReader.close();

        return hostToCustomerMap;
    }

    public static class Builder implements EventSerializer.Builder {

        private String customerHeader;
        private String hostHeader;
        private String path;

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            path = context.getString(PATH, PATH_DEFAULT);
            customerHeader = context.getString(CUSTOMER_HEADER, CUSTOMER_HEADER_DEFAULT);
            hostHeader = context.getString(HOST_HEADER, HOST_HEADER_DEFAULT);
            SyslogAvroEventSerializer writer = null;
            try {
                writer = new SyslogAvroEventSerializer(out, path, customerHeader, hostHeader);
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

    public static class Constants {

        public static final String PATH = "path";
        public static final String PATH_DEFAULT = "/etc/flume-ng/conf/customerhosts.conf";

        public static final String CUSTOMER_HEADER = "customerHeader";
        public static final String CUSTOMER_HEADER_DEFAULT = "customer";

        public static final String HOST_HEADER = "hostHeader";
        public static final String HOST_HEADER_DEFAULT = "host";
    }
}