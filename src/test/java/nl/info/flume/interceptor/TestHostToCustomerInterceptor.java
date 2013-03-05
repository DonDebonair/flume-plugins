package nl.info.flume.interceptor;

import junit.framework.Assert;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * ...
 *
 * @author daan.debie
 */
public class TestHostToCustomerInterceptor {

    @Test
    public void testHashMapFromCustomerToHostsFile() throws IOException {
        File file = new File("src/test/resources/customerToHostsFile.txt");
        Map<String, String> testMap = HostToCustomerInterceptor.buildCustomerToHostMapFromFile(file);
        Assert.assertEquals("'logmft02p' should belong to customer 'nietinfo'", "nietinfo", testMap.get("logmft02p"));
        Assert.assertEquals("'localhost' should belong to customer 'info'", "info", testMap.get("localhost"));
    }
}
