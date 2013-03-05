package nl.info.flume.serialization;

import com.google.common.base.Charsets;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;

import java.io.OutputStream;
import java.util.Map;

/**
 * ...
 *
 * @author daan.debie
 */
public class FlumeEventStringAvroEventSerializer extends AbstractAvroEventSerializer<FlumeEventStringAvroEventSerializer.Container> {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{ \"type\":\"record\", \"name\": \"Event\", \"fields\": [" +
                    " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
                    " {\"name\": \"body\", \"type\": \"string\" } ] }");

    private final OutputStream out;

    private FlumeEventStringAvroEventSerializer(OutputStream out) {
        this.out = out;
    }

    @Override
    protected Schema getSchema() {
        return SCHEMA;
    }

    @Override
    protected OutputStream getOutputStream() {
        return out;
    }

    /**
     * A no-op for this simple, special-case implementation
     * @param event
     * @return
     */
    @Override
    protected Container convert(Event event) {
        return new Container(event.getHeaders(), new String(event.getBody(), Charsets.UTF_8));
    }

    public static class Builder implements EventSerializer.Builder {

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            FlumeEventStringAvroEventSerializer writer = new FlumeEventStringAvroEventSerializer(out);
            writer.configure(context);
            return writer;
        }

    }

    public static class Container {
        private final Map<String, String> headers;
        private final String body;
        public Container(Map<String, String> headers, String body) {
            super();
            this.headers = headers;
            this.body = body;
        }
    }
}
