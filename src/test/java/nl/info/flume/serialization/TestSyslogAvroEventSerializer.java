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
import org.apache.flume.source.SyslogUtils;
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
public class TestSyslogAvroEventSerializer {

    File testFile = new File("src/test/resources/SyslogEvents.avro");

    private static List<Event> generateSyslogEvents() {
        List<Event> list = Lists.newArrayList();

        Event e;

        e = EventBuilder.withBody("Mar  1 11:23:24 wtrip01 Security: WTRIP01\\mapadm: User Logoff: User Name: mapadm Domain: WTRIP01 Logon ID: (0x0,0x26656B32) Logon Type: 10", Charsets.UTF_8);
        list.add(e);

        e = EventBuilder.withBody("Mar  1 11:23:26 fac01 dhcpd: DHCPREQUEST for 10.10.222.195 from 00:1c:c4:59:d7:06 via eth1", Charsets.UTF_8);
        list.add(e);

        // generate a "raw" syslog event
        e = EventBuilder.withBody("Mar  1 11:23:29 wtrip01 ASP.NET 2.0.50727.0: N/A: Event code: 3005 Event message: An unhandled exception has occurred. Event time: 3/1/2013 11:23:26 AM Event time (UTC): 3/1/2013 10:23:26 AM Event ID: acc0d7461ace44be97a422c9f92b103b Event sequence: 3873 Event occurrence: 3872 Event detail code: 0 Application information: Application domain: /LM/W3SVC/1475270397/Root-1-130057224209375000 Trust level: Full Application Virtual Path: / Application Path: D:\\tileserver_locatienet_com_2.0\\root\\ Machine name: WTRIP01 Process information: Process ID: 3060 Process name: w3wp.exe Account name: NT AUTHORITY\\NETWORK SERVICE Exception information: Exception type: HttpException Exception message: The remote host closed the connection. The error code is 0x80072746. Request information: Request URL: http://tileserver.locatienet.com/image.ashx?FORMAT=image%2Fgif&CRS=EPSG%3A900913&LAYERS=nl&TRANSPARENT=TRUE&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=&SRS=EPSG%3A900913&BBOX=-1760467.37458!\n" +
                "1,4419255.7510106,2642305.4540324,8088233.1081884&WID", Charsets.UTF_8);
        list.add(e);

        return list;
    }

    @Test
    public void test() throws FileNotFoundException, IOException {

        // create the file, write some data
        OutputStream out = new FileOutputStream(testFile);
        String builderName = SyslogAvroEventSerializer.Builder.class.getName();

        Context ctx = new Context();
        ctx.put("syncInterval", "4096");

        EventSerializer serializer =
                EventSerializerFactory.getInstance(builderName, ctx, out);
        serializer.afterCreate(); // must call this when a file is newly created

        List<Event> events = generateSyslogEvents();
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
            String hostname = record.get("hostname").toString();
            String message = record.get("message").toString();

            System.out.println(datetime + ": " + message);
            numEvents++;
        }

        fileReader.close();
        Assert.assertEquals("Should have found a total of 3 events", 3, numEvents);

        FileUtils.forceDelete(testFile);
    }
}
