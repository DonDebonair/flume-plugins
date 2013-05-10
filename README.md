# Info Flume Plugins

These are a bunch of extensions to Flume that I developed at Info.nl with the help of [Remmelt Pit](https://github.com/remmelt). These plugins are mainly used at Info.nl to help with the collection and storage of logs from all the different applications that are developed at Info.nl. Currently we process Java application logs, Apache httpd logs and Syslogs.
Below you can find installation instructions and a short overview of the different components.

**NOTE: These plugins are built for Flume-NG and will not work on Flume-OG**
**NOTE 2: These plugins lack comprehensive test coverage. Some unit tests are available for some of the plugins, but assume buggyness :) Of course contributions are welcome to make these plugins more stable and useful**

## Compilation & Installation instructions

You can compile these using [Maven](http://maven.apache.org/) (which you have to install first) by running

```
$ mvn clean package
```

from your command prompt.

Afterwards you should ship the resulting jar `flume-plugins-X.Y-SNAPSHOT.jar` (where X.Y is the current version number) to the machine running Flume, if this is not the machine you're currently working on. You can do this by using `scp` or automate it with something like [Puppet](https://puppetlabs.com/). After you have done this, you should move the jar file to the Flume lib directory. If you use CDH4, this will probably be `/usr/lib/flume-ng/lib`

Now follows a brief overview of each component with usage instructions

## Sources

### MultiLineExecSource

The MultiLineExecSource is used for generating Flume events containing multiple lines in the body, per event. The problem we tried to solve is to be able to [tail](http://en.wikipedia.org/wiki/Tail_(Unix)) a Java logfile and preserving stacktraces or other multiline events as one event in Flume. It does that by looking for a certain character sequence that is used to signify the end of an event (ie. stacktrace). The reason this was built, is because we were using the native Glassfish logging capabilities instead of Log4J, so we coulnd't actually use one of the existing Flume appenders.

The MultiLineExecSource is based on the regular exec source and includes the same parameters. It also adds two additional ones:

* **event.terminator**: This is the character/sequence of chars that determines the end of an event (ie. stacktrace). This parameter is required and the Flume agent will fail to run without it. You should configure your logging to use some sort of char sequence as boundary between events, for this to work.
* **line.terminator** (optional): This is the character/sequence of chars that is used to "glue" the different lines within an event together. It defaults to " § ". This may seem weird, and you'd probably want to use "\n", but we couldn't get that to work as it seems to mess up either Avro, Hive or Hue or a combination of that. Any tips on how to resolve that are welcome!

Example config:

```
agent.sources.javatail.type=nl.info.flume.source.MultiLineExecSource
agent.sources.javatail.command=tail -F /var/log/flume-ng/test.log
agent.sources.javatail.event.terminator=|#]
agent.sources.javatail.channels=mem-channel
agent.sources.javatail.batchSize=1
```

**NOTE: If you want to use this for capturing Java logging events and you're using Log4J in your application, than you're probably better off using [one of the existing](http://logging.apache.org/log4j/2.x/log4j-flume-ng/) [Flume appenders for Log4J](http://archive.cloudera.com/cdh4/cdh/4/flume-ng/FlumeUserGuide.html#log4j-appender).**

## Interceptors

### HostToCustomerInterceptor

This interceptor can be used to add additional information to the headers of an event, based on the machine the agent is running on, or the machine where the events were generated in the first place (if you use it in conjunction with a host interceptor with _preserveExisting_ set to _true_). I built this because we are putting logs from many different machines in our Hadoop cluster, using Flume, and we want to be able to query those logs using the application (which equals customer in our case) that those logs belong to. This is essential because many different machines, with different hostnames, can belong to one application.

The interceptor works by reading a file containing all the applications/customers it expects logs from, _one per line_, each followed by ":" and a space/tab delimited list of the different hostnames of all the machines that belong to that application. For example:

```
application1: host1 host2.info.nl host3
application2: host4 host5
application3: host6
```

The interceptor will read the file at startup and build a reverse hashmap containing the hostnames as keys and the application/customer names as values. During an intercept, it will lookup the hostname in the hashmap to determine what application/customer that host belongs to. This gives a decent performance (which I admittedly haven't tested yet) but it also means that you have to restart the agent after modifying the file.

This interceptor only works if the hostname is available in the header, which can be arranged by using Flume's built-in [host interceptor](http://archive.cloudera.com/cdh4/cdh/4/flume-ng/FlumeUserGuide.html#host-interceptor).

HostToCustomerInterceptor needs three parameters:

```
agent.sources.javatail.interceptors=customer
agent.sources.javatail.interceptors.customer.type=nl.info.flume.interceptor.HostToCustomerInterceptor$Builder
agent.sources.javatail.interceptors.customer.path=/etc/flume-ng/conf/statics.conf # File containing the application/customer names followed by lists of hosts.
agent.sources.javatail.interceptors.customer.customerHeader=customer # (default) what key should be used for the application/customer name in the header
agent.sources.javatail.interceptors.customer.hostHeader=host # (default) what key the hostname can be found in
```

### StaticFileInterceptor

This is a simple interceptor that allows you to add arbitrary key-value pairs to the headers of Flume events, based on a simple property file. It is useful for adding additional information to Flume events, based on the machine the agent is running on. You can specify the key-value pairs in the property file as one pair per line, with the key and value separated by '='. It will try and read the file at each intercept. This will probably not work for high throughputs, but it does give the advantage that you can change the file intermittently without restarting the agent. A better solution would probably be to build a hashmap at startup, as I did with the HostToCustomerInterceptor.

StaticFileInterceptor needs two parameters:

```
agent.sources.javatail.interceptors=static
agent.sources.javatail.interceptors.static.type=nl.info.flume.interceptor.StaticFileInterceptor$Builder
agent.sources.javatail.interceptors.static.path=/etc/flume-ng/conf/statics.conf # File containing the key-value pairs to be added to the headers
agent.sources.javatail.interceptors.static.preserveExisting=true # Will preserve existing keys in headers. Useful when this is not the first hop in the Flume chain
```

## Serializers

These serializers are all extensions to the simple avro event serializer.

### FlumeEventStringAvroEventSerializer

This serializer can be used to serialize a Flume event as an Avro event, whereby the body will be serialized as String instead of Bytes.

Example config:

```
agent.sinks.hdfssink.type=hdfs
agent.sinks.hdfssink.channel=mem-channel
agent.sinks.hdfssink.hdfs.path=/user/cloudera/log
agent.sinks.hdfssink.hdfs.fileType=DataStream
agent.sinks.hdfssink.serializer=nl.info.flume.serialization.FlumeEventStringAvroEventSerializer$Builder
```

### SyslogAvroEventSerializer

This is a serializer for serializing Syslog events as Avro events. It's heavily based on [the example provided in the Flume core](https://github.com/apache/flume/blob/trunk/flume-ng-core/src/test/java/org/apache/flume/serialization/SyslogAvroEventSerializer.java) and as such it **suffers from the same flaws as mentioned there.** Nevertheless it works pretty well for us so far.

Additions:

* Human readable date/time field
* Addition of HostToCustomer functionality that mimics the interceptor with the same name. This was necessary because the originating host with Syslog events isn't actually in the headers of the Flume event because the events captured by Flume at the first hop, are already collected Syslog events from different machines, as is often the case with Syslogs. So now the hostname is parsed from the Syslog message body and application/customer name is determined from that.

Example config:

```
agent.sinks.hdfssink.type=hdfs
agent.sinks.hdfssink.channel=mem-channel
agent.sinks.hdfssink.hdfs.path=/user/cloudera/log
agent.sinks.hdfssink.hdfs.fileType=DataStream
agent.sinks.hdfssink.serializer=nl.info.flume.serialization.SyslogAvroEventSerializer$Builder
```

### ApacheLogAvroEventSerializer

This serializer will parse Apache httpd logs that are generated by the following log config in Apache:

```
LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %v %T %D %X \"%{Content-Type}o\" \"%{cookie}n\" %I %O"
```

Parsing is done using regex, so if your logformat is different, it's easy to adapt the serializer. All parts of the logline are separated into distinct Avro fields for easy querying!

Example config:

```
agent.sinks.hdfssink.type=hdfs
agent.sinks.hdfssink.channel=mem-channel
agent.sinks.hdfssink.hdfs.path=/user/cloudera/log
agent.sinks.hdfssink.hdfs.fileType=DataStream
agent.sinks.hdfssink.serializer=nl.info.flume.serialization.ApacheLogAvroEventSerializer$Builder
```

### JavaLogAvroEventSerializer

This serializer is built to parse a Java log event into separate fields in an Avro event as much as possible. Of course, Java log formats vary a lot, so you will have to adjust the parsing to your own needs. The serializer is based upon the example Syslog serializer in the Flume core and suffers from the same shortcomings. You can find our Java log format in the unit tests.

Example config:

```
agent.sinks.hdfssink.type=hdfs
agent.sinks.hdfssink.channel=mem-channel
agent.sinks.hdfssink.hdfs.path=/user/cloudera/log
agent.sinks.hdfssink.hdfs.fileType=DataStream
agent.sinks.hdfssink.serializer=nl.info.flume.serialization.JavaLogAvroEventSerializer$Builder
```
