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
import java.util.List;
import java.util.Map;

import static nl.info.flume.interceptor.StaticFileInterceptor.Constants.*;

/**
 * ...
 *
 * @author daan.debie
 */
@Slf4j
public class StaticFileInterceptor implements Interceptor {

    private final boolean preserveExisting;
    private final String path;
    private File staticFile;
    private Map<String, String> statics;

    /**
     * Only {@link StaticFileInterceptor.Builder} can build me
     */
    private StaticFileInterceptor(boolean preserveExisting, String path) {
        this.preserveExisting = preserveExisting;
        this.path = path;

    }

    @Override
    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        staticFile = new File(path);
        FileReader fileReader;
        String line;
        int count = 0;

        try {
            fileReader = new FileReader(staticFile);
        } catch (FileNotFoundException e) {
            log.warn("Could not find file: {}", path);
            return event;
        }

        BufferedReader bufferedReader = new BufferedReader(fileReader);
        try {
            String[] keyValues;
            String key;
            String value;
            line = bufferedReader.readLine();
            count++;
            while(line != null) {
                keyValues = line.split("=");
                key = keyValues[0];
                value = keyValues[1];
                if (preserveExisting && headers.containsKey(key)) {
                    continue;
                }
                headers.put(key, value);
                line = bufferedReader.readLine();
            }
            bufferedReader.close();
        } catch (IOException e) {
            log.warn("File IO error in: {}", path);
        } catch (IndexOutOfBoundsException e) {
            log.warn("Static key-value pair not properly formatted on line {} of {}", count, path);
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

    /**
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private boolean preserveExisting;
        private String path;

        @Override
        public void configure(Context context) {
            preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DEFAULT);
            path = context.getString(PATH, PATH_DEFAULT);
        }

        @Override
        public Interceptor build() {
            log.info(String.format(
                    "Creating StaticFileInterceptor: preserveExisting=%s,path=%s",
                    preserveExisting, path));
            return new StaticFileInterceptor(preserveExisting, path);
        }


    }

    public static class Constants {

        public static final String PATH = "path";
        public static final String PATH_DEFAULT = "/etc/flume-ng/conf/statics.conf";

        public static final String PRESERVE = "preserveExisting";
        public static final boolean PRESERVE_DEFAULT = true;
    }

}
