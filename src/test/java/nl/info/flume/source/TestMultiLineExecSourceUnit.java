package nl.info.flume.source;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMultiLineExecSourceUnit {
	@Mock
	private ChannelProcessor channelProcessor;

	@Mock
	private CounterGroup counterGroup;

	@Mock
	private BufferedReader bufferedReader;

	@Mock
	private MultiLineExecSource.StderrReader stderrReader;

	private String[] command = "theCommand".split("\\s+");

	@Spy
	@InjectMocks
	private MultiLineExecSource.ExecRunnable execRunnable = new MultiLineExecSource.ExecRunnable(command[0], "|#]", channelProcessor, counterGroup, false, 0L, false, 1000, Charset.defaultCharset());

	private List<List<String>> eventLines = new ArrayList<List<String>>();
	private List<String> lines = new ArrayList<String>();

	@Before
	public void setUpBeforeClass() throws IOException {
		doReturn(null).when(execRunnable).startedCommandProcessBuilder(anyListOf(String.class));
		doReturn(bufferedReader).when(execRunnable).getBufferedReader();
		doReturn(stderrReader).when(execRunnable).getStderrReader();
		final int[] counter = {0};

		when(bufferedReader.readLine()).thenAnswer(new Answer<String>() {
			@Override
			public String answer(InvocationOnMock invocationOnMock) throws Throwable {
				return lines.get(counter[0]++);
			}
		}
		);
	}

	@Test
	public void testRunMultiLineExecSource() {
		List<String> eventLines1;

		eventLines1 = Arrays.asList("[#|a2013-01-18T15:53:41.685+0100|INFO|sun-appserver2.1|javax.enterprise.system.container.web|_ThreadID=206;_ThreadName=RMI TCP Connection(23924)-127.0.0.1;Sun GlassFish Enterprise Server v2.1.1;8080;|WEB0713: Stopping Sun GlassFish Enterprise Server v2.1.1 HTTP/1.1 on 8080|#]", "");
		eventLines.add(eventLines1);
		eventLines1 = Arrays.asList(
				  "[#|b2013-01-18T15:51:18.888+0100|WARNING|sun-appserver2.1|javax.enterprise.system.stream.err|_ThreadID=24;_ThreadName=httpWorkerThread-4848-2;_RequestID=dcd03b9d-c158-4118-97ed-2d5102b1cc64;|server.PerInterface.invoke(PerInterface.java:120)",
				  "\tat com.sun.jmx.mbeanserver.MBeanSupport.invoke(MBeanSupport.java:262)",
				  "\tat com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.invoke(DefaultMBeanServerInterceptor.java:836)",
				  "\tat com.sun.jmx.mbeanserver.JmxMBeanServer.invoke(JmxMBeanServer.java:761)",
				  "\tat javax.management.remote.rmi.RMIConnectionImpl.doOperation(RMIConnectionImpl.java:1427)",
				  "\tat javax.management.remote.rmi.RMIConnectionImpl.access$200(RMIConnectionImpl.java:72)",
				  "\tat javax.management.remote.rmi.RMIConnectionImpl$PrivilegedOperation.run(RMIConnectionImpl.java:1265)",
				  "\tat javax.management.remote.rmi.RMIConnectionImpl.doPrivilegedOperation(RMIConnectionImpl.java:1360)",
				  "\tat javax.management.remote.rmi.RMIConnectionImpl.invoke(RMIConnectionImpl.java:788)",
				  "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
				  "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)",
				  "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)",
				  "\tat java.lang.reflect.Method.invoke(Method.java:597)",
				  "\tat sun.rmi.server.UnicastServerRef.dispatch(UnicastServerRef.java:303)",
				  "\tat sun.rmi.transport.Transport$1.run(Transport.java:159)",
				  "\tat java.security.AccessController.doPrivileged(Native Method)",
				  "\tat sun.rmi.transport.Transport.serviceCall(Transport.java:155)",
				  "\tat sun.rmi.transport.tcp.TCPTransport.handleMessages(TCPTransport.java:535)",
				  "\tat sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.run0(TCPTransport.java:790)",
				  "\tat sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.run(TCPTransport.java:649)",
				  "\tat java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)",
				  "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)",
				  "\tat java.lang.Thread.run(Thread.java:662)",
				  "|#]",
				  "");
		eventLines.add(eventLines1);
		eventLines1 = Arrays.asList("[#|c2013-01-18T15:53:41.685+0100|INFO|sun-appserver2.1|javax.enterprise.system.container.web|_ThreadID=206;_ThreadName=RMI TCP Connection(23924)-127.0.0.1;Sun GlassFish Enterprise Server v2.1.1;8080;|WEB0713: Stopping Sun GlassFish Enterprise Server v2.1.1 HTTP/1.1 on 8080|#]", "");
		eventLines.add(eventLines1);
		eventLines1 = Arrays.asList("[#|d2013-01-18T15:53:42.113+0100|INFO|sun-appserver2.1|javax.enterprise.system.container.web|_ThreadID=206;_ThreadName=RMI TCP Connection(23924)-127.0.0.1;Sun GlassFish Enterprise Server v2.1.1;8181;|WEB0713: Stopping Sun GlassFish Enterprise Server v2.1.1 HTTP/1.1 on 8181|#]", "");
		eventLines.add(eventLines1);

		for (List<String> event : eventLines) {
			lines.addAll(event);
		}
		lines.add(null);

		execRunnable.run();

		ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
		verify(channelProcessor).processEventBatch(argument.capture());
		deepComparison(argument.getValue());
	}

	private void deepComparison(List values) {
		assertEquals(eventLines.size(), values.size());

		int counter = 0;
		for (List<String> event : eventLines) {
			String expectedLinesJoined = StringUtils.join(event.subList(0, event.size() - 1).toArray(), "\n");

			byte[] value = ((Event) values.get(counter++)).getBody();
			assertEquals(expectedLinesJoined, new String(value));
			assertEquals(expectedLinesJoined.length(), value.length);
		}
	}

}
