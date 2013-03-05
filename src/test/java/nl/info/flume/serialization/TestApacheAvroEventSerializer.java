package nl.info.flume.serialization;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * ...
 *
 * @author daan.debie
 */
@Slf4j
public class TestApacheAvroEventSerializer {

    File testFile = new File("src/test/resources/ApacheEvents.avro");

    private static List<Event> generateApacheEvents() {
        List<Event> list = Lists.newArrayList();

        Event e;

        // generate some events
        e = EventBuilder.withBody("80.79.194.3 - - [01/Mar/2013:11:23:26 +0100] \"GET /graphs/tabledata.pl HTTP/1.1\" 200 3132 \"https://noc.info.nl/graphs/alert.pl\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16\" noc.info.nl \"noc.info.nl\" 1 1135203 + \"text/html\" \"-\" 754 3785", Charsets.UTF_8);
        list.add(e);

        e = EventBuilder.withBody("80.79.194.3 - - [01/Mar/2013:11:23:56 +0100] \"GET /graphs/rotator.pl HTTP/1.1\" 200 - \"https://noc.info.nl/graphs/alert.pl\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16\" noc.info.nl \"noc.info.nl\" 0 55535 + \"text/html\" \"-\" 754 390", Charsets.UTF_8);
        list.add(e);

        TestApacheAvroEventSerializer.log.info("Event: {}", e);

        return list;
    }

    @Test
    public void test() throws FileNotFoundException, IOException {

        // create the file, write some data
        OutputStream out = new FileOutputStream(testFile);
        String builderName = ApacheLogAvroEventSerializer.Builder.class.getName();

        Context ctx = new Context();
        ctx.put("syncInterval", "4096");

        EventSerializer serializer =
                EventSerializerFactory.getInstance(builderName, ctx, out);
        serializer.afterCreate(); // must call this when a file is newly created

        List<Event> events = generateApacheEvents();
        for (Event e : events) {
            serializer.write(e);
        }
        serializer.flush();
        serializer.beforeClose();
        out.flush();
        out.close();

        // now try to read the file back

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> fileReader =
                new DataFileReader<GenericRecord>(testFile, reader);

        GenericRecord record = new GenericData.Record(fileReader.getSchema());
        int numEvents = 0;
        while (fileReader.hasNext()) {
            fileReader.next(record);
            String ip = record.get("ip").toString();
            String uri = record.get("uri").toString();
            Integer statuscode = (Integer) record.get("statuscode");
            String original = record.get("original").toString();
            String connectionstatus = record.get("connectionstatus").toString();

            Assert.assertEquals("Ip should be 80.79.194.3", "80.79.194.3", ip);
            System.out.println("IP " + ip + " requested: " + uri + " with status code " + statuscode + " and connectionstatus: " + connectionstatus);
            System.out.println("Original logline: " + original);
            numEvents++;
        }

        fileReader.close();
        Assert.assertEquals("Should have found a total of 3 events", 2, numEvents);

        FileUtils.forceDelete(testFile);
    }

}
