package nl.info.flume.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static nl.info.flume.interceptor.HostToCustomerInterceptor.Constants.*;

/**
 * ...
 *
 * @author daan.debie
 */
@Slf4j
public class HostToCustomerInterceptor implements Interceptor {

    private final String path;
    private final String customerHeader;
    private final String hostHeader;
    private static final String UNKNOWN_CUSTOMER = "UNKNOWN";
    private Map<String, String> hostToCustomerMap;

    /**
     * Only {@link StaticFileInterceptor.Builder} can build me
     */
    private HostToCustomerInterceptor(String path, String customerHeader, String hostHeader) {
        this.path = path;
        this.customerHeader = customerHeader;
        this.hostHeader = hostHeader;
    }

    @Override
    public void initialize() {
        File customerHostsFile = new File(path);
        try {
            hostToCustomerMap = buildCustomerToHostMapFromFile(customerHostsFile);
        } catch (FileNotFoundException e) {
            log.warn("Could not find file: {}", path);
        } catch (IOException e) {
            log.warn("File IO error in: {}", path);
        }
    }

    /**
     * Modifies events in-place.
     */
    @Override
    public Event intercept(Event event) {
        String customer;
        String shortHost;
        Map<String, String> headers = event.getHeaders();
        if(!headers.containsKey(hostHeader)) {
            // log.warn("No host found in header!");
            headers.put(customerHeader, UNKNOWN_CUSTOMER);
            return event;
        } else {
            String host = headers.get(hostHeader);
            if(host.contains(".")) {
                shortHost = host.substring(0, host.indexOf("."));
            } else {
                shortHost = host;
            }
            customer = hostToCustomerMap.get(shortHost.toLowerCase());
        }

        if(customer != null) {
            headers.put(customerHeader, customer);
        } else {
            headers.put(customerHeader, UNKNOWN_CUSTOMER);
        }
        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // no-op
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

    /**
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private String customerHeader;
        private String hostHeader;
        private String path;

        @Override
        public void configure(Context context) {
            path = context.getString(PATH, PATH_DEFAULT);
            customerHeader = context.getString(CUSTOMER_HEADER, CUSTOMER_HEADER_DEFAULT);
            hostHeader = context.getString(HOST_HEADER, HOST_HEADER_DEFAULT);
        }

        @Override
        public Interceptor build() {
            log.info(String.format(
                    "Creating HostToCustomerInterceptor: path=%s, customerHeader=%s", path, customerHeader));
            return new HostToCustomerInterceptor(path, customerHeader, hostHeader);
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
