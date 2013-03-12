package nl.info.flume.serialization;

import com.google.common.base.Charsets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.info.flume.serialization.ApacheLogAvroEventSerializer.ApacheEvent;

/**
 * ...
 *
 * @author daan.debie
 */
@Slf4j
public class ApacheLogAvroEventSerializer extends AbstractAvroEventSerializer<ApacheEvent> {

    private static final String REGEXP =
            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"([^ ]*) ([^ ]*) ([^ ]*)\" (\\d{3}) (\\d+|-) \"([^\"]+)\" \"([^\"]+)\" (\\S+)? \"(\\S+)\" (\\d+|-)? (\\d+|-)? ([+|\\-|X])? \"([^\"]+)\"? \"([^\"]+)\"? (\\d+|-)? (\\d+|-)?";


    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{ \"type\":\"record\", \"name\": \"ApacheEvent\", \"namespace\": \"nl.info.flume\", \"fields\": [" +
                    " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
                    " {\"name\": \"original\", \"type\": \"string\" }," +
                    " {\"name\": \"ip\", \"type\": \"string\" }," +
                    " {\"name\": \"identd\", \"type\": \"string\" }," +
                    " {\"name\": \"user\", \"type\": \"string\" }," +
                    " {\"name\": \"time\", \"type\": \"string\" }," +
                    " {\"name\": \"method\", \"type\": \"string\" }," +
                    " {\"name\": \"uri\", \"type\": \"string\" }," +
                    " {\"name\": \"protocol\", \"type\": \"string\" }," +
                    " {\"name\": \"statuscode\", \"type\": \"int\" }," +
                    " {\"name\": \"bytesSend\", \"type\": \"string\" }," +
                    " {\"name\": \"referer\", \"type\": \"string\" }," +
                    " {\"name\": \"useragent\", \"type\": \"string\" }," +
                    " {\"name\": \"servername\", \"type\": \"string\" }," +
                    " {\"name\": \"extraservername\", \"type\": \"string\" }," +
                    " {\"name\": \"timeSecond\", \"type\": \"string\" }," +
                    " {\"name\": \"timeMicro\", \"type\": \"string\" }," +
                    " {\"name\": \"connectionstatus\", \"type\": \"string\" }," +
                    " {\"name\": \"connectiontype\", \"type\": \"string\" }," +
                    " {\"name\": \"sessioncookie\", \"type\": \"string\" }," +
                    " {\"name\": \"bytesIn\", \"type\": \"string\" }," +
                    " {\"name\": \"bytesOut\", \"type\": \"string\" }" +
                    "] }");

    private final OutputStream out;

    public ApacheLogAvroEventSerializer(OutputStream out) throws IOException {
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

    @Override
    protected ApacheEvent convert(Event event) {
        ApacheEvent apacheEvent = new ApacheEvent();

        String logline = new String(event.getBody(), Charsets.UTF_8);
        apacheEvent.setHeaders(event.getHeaders());
        apacheEvent.setOriginal(logline);

        Pattern pattern = Pattern.compile(REGEXP);
        Matcher m = pattern.matcher(logline);
        if (m.matches()){
            apacheEvent.setIp(m.group(1));
            apacheEvent.setIdentd(m.group(2));
            apacheEvent.setUser(m.group(3));
            apacheEvent.setTime(m.group(4));
            apacheEvent.setMethod(m.group(5));
            apacheEvent.setUri(m.group(6));
            apacheEvent.setProtocol(m.group(7));
            apacheEvent.setStatuscode(Integer.valueOf(m.group(8)));
            apacheEvent.setBytesSend(m.group(9));
            apacheEvent.setReferer(m.group(10));
            apacheEvent.setUseragent(m.group(11));
            apacheEvent.setServername(m.group(12));
            apacheEvent.setExtraservername(m.group(13));
            apacheEvent.setTimeSecond(m.group(14));
            apacheEvent.setTimeMicro(m.group(15));
            apacheEvent.setConnectionstatus(m.group(16));
            apacheEvent.setConnectiontype(m.group(17));
            apacheEvent.setSessioncookie(m.group(18));
            apacheEvent.setBytesIn(m.group(19));
            apacheEvent.setBytesOut(m.group(20));
        } else {
            log.warn("The event doesn't match the Apache LogFormat!");
        }

        // log.debug("Serialized event as: {}", apacheEvent);

        return apacheEvent;
    }

    public static class Builder implements EventSerializer.Builder {

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            ApacheLogAvroEventSerializer writer = null;
            try {
                writer = new ApacheLogAvroEventSerializer(out);
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
    @Setter
    @Getter
    public static class ApacheEvent {
        private Map<String, String> headers;
        private String original = "";
        private String ip = "";
        private String identd = "";
        private String user = "";
        private String time = "";
        private String method = "";
        private String uri = "";
        private String protocol = "";
        private int statuscode;
        private String bytesSend = "";
        private String referer = "";
        private String useragent = "";
        private String servername = "";
        private String extraservername = "";
        private String timeSecond = "";
        private String timeMicro = "";
        private String connectionstatus = "";
        private String connectiontype = "";
        private String sessioncookie = "";
        private String bytesIn = "";
        private String bytesOut = "";

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{ Original: ").append(original).append(", ");
            builder.append(" IP: ").append(ip).append(", ");
            builder.append(" Identd: ").append(identd).append(", ");
            builder.append(" User: ").append(user).append(", ");
            builder.append(" Time: ").append(time).append(", ");
            builder.append(" Method: ").append(method).append(", ");
            builder.append(" URI: ").append(uri).append(", ");
            builder.append(" Protocol: ").append(protocol).append(", ");
            builder.append(" Statuscode: ").append(statuscode).append(", ");
            builder.append(" BytesSend: ").append(bytesSend).append(", ");
            builder.append(" Referer: ").append(referer).append(", ");
            builder.append(" User-Agent: ").append(useragent).append(", ");
            builder.append(" Servername: ").append(servername).append(", ");
            builder.append(" Extra Servername: ").append(servername).append(", ");
            builder.append(" TimeSecond: ").append(timeSecond).append(", ");
            builder.append(" TimeMicro: ").append(timeMicro).append(", ");
            builder.append(" Connection-status: ").append(connectionstatus).append(", ");
            builder.append(" Connection-type: ").append(connectiontype).append(", ");
            builder.append(" Sessioncookie: ").append(sessioncookie).append(", ");
            builder.append(" BytesIn: ").append(bytesIn).append(", ");
            builder.append(" BytesOut: \"").append(bytesOut).append("\" }");
            return builder.toString();
        }
    }
}

