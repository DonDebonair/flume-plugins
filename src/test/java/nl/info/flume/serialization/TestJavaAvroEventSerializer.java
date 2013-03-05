package nl.info.flume.serialization;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * ...
 *
 * @author daan.debie
 */
public class TestJavaAvroEventSerializer {

    File testFile = new File("src/test/resources/JavaEvents.avro");

    private static List<Event> generateJavaEvents() {
        List<Event> list = Lists.newArrayList();

        Event e;

        e = EventBuilder.withBody("[#|2013-03-01T11:23:50.765+0100|WARNING|sun-appserver2.1|nl.info.bva.model.service.impl.UserServiceImpl|_ThreadID=28;_ThreadName=TP-Processor5;_RequestID=3287e2db-90cc-4fa8-9bc5-2cb0ecdc1da5;|Invocation of method nl.info.bva.model.service.impl.UserServiceImpl.findUsersByDetails({<null>,<null>,ealkoo,<null>,<null>,<null>,<null>,<null>,<null>,<null>}) exceeded threshold! Duration was 585 msec and the threshold was set at 500 msec.|#]", Charsets.UTF_8);
        list.add(e);

        e = EventBuilder.withBody("[#|2013-03-01T11:25:00.017+0100|INFO|sun-appserver2.1|nl.info.bva.model.service.impl.schedule.AbstractContextAwareQuartzJobBean|_ThreadID=23;_ThreadName=taskExecutor-5;|Job 'SendLotOpeningAndClosingNotificationsJob' started|#]", Charsets.UTF_8);
        list.add(e);

        // generate a "raw" syslog event
        e = EventBuilder.withBody("[#|2013-03-01T11:25:12.944+0100|WARNING|sun-appserver2.1|nl.info.bva.model.service.impl.UserServiceImpl|_ThreadID=25;_ThreadName=TP-Processor2;_RequestID=66ca112d-55d0-4ba6-9503-0c70d5f4c606;|Invocation of method nl.info.bva.model.service.impl.UserServiceImpl.findUsersByDetails({Steijn-koopman,<null>,<null>,<null>,<null>,<null>,<null>,<null>,<null>,<null>}) exceeded threshold! Duration was 536 msec and the threshold was set at 500 msec.|#]", Charsets.UTF_8);
        list.add(e);

        return list;
    }

    @Test
    public void test() throws FileNotFoundException, IOException {

        // create the file, write some data
        OutputStream out = new FileOutputStream(testFile);
        String builderName = JavaLogAvroEventSerializer.Builder.class.getName();

        Context ctx = new Context();
        ctx.put("syncInterval", "4096");

        EventSerializer serializer =
                EventSerializerFactory.getInstance(builderName, ctx, out);
        serializer.afterCreate(); // must call this when a file is newly created

        List<Event> events = generateJavaEvents();
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
            long timestamp = (Long) record.get("timestamp");
            String datetime = record.get("datetime").toString();
            String classname = record.get("classname").toString();
            String message = record.get("message").toString();

            System.out.println(classname + ": " + message + " (at " + datetime + ")");
            numEvents++;
        }

        fileReader.close();
        Assert.assertEquals("Should have found a total of 3 events", 3, numEvents);

        FileUtils.forceDelete(testFile);
    }
}
