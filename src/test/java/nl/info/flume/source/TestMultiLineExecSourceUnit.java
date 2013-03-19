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

		eventLines1 = Arrays.asList("[#|2013-03-19T13:00:00.413+0100|INFO|oracle-glassfish3.1.2|nl.info.bva.model.service.impl.schedule.AbstractContextAwareQuartzJobBean|_ThreadID=36;_ThreadName=Thread-2;|Job 'ClangUserExportJob' started|#]", "");
        eventLines.add(eventLines1);
        eventLines1 = Arrays.asList("[#|2013-03-19T13:00:00.424+0100|WARNING|oracle-glassfish3.1.2|nl.info.bva.model.service.impl.schedule.AbstractContextAwareQuartzJobBean|_ThreadID=36;_ThreadName=Thread-2;|Job 'ClangUserExportJob' finished with JobExecutionException: [CLANG] Clang token 'null' is not valid...|#]", "");
        eventLines.add(eventLines1);
        eventLines1 = Arrays.asList(
                "[#|2013-03-19T13:00:00.425+0100|INFO|oracle-glassfish3.1.2|org.quartz.core.JobRunShell|_ThreadID=36;_ThreadName=Thread-2;|Job site.clangUserExportJob threw a JobExecutionException:",
                        "org.quartz.JobExecutionException: org.quartz.JobExecutionException: [CLANG] Clang token 'null' is not valid... [See nested exception: org.quartz.JobExecutionException: [CLANG] Clang token 'null' is not valid...]",
                        "\tat nl.info.bva.model.service.impl.schedule.AbstractContextAwareQuartzJobBean.executeInternal(AbstractContextAwareQuartzJobBean.java:44)",
                        "\tat org.springframework.scheduling.quartz.QuartzJobBean.execute(QuartzJobBean.java:113)",
                        "\tat org.quartz.core.JobRunShell.run(JobRunShell.java:213)",
                        "\tat java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:895)",
                        "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:918)",
                        "\tat java.lang.Thread.run(Thread.java:662)",
                        "Caused by: org.quartz.JobExecutionException: [CLANG] Clang token 'null' is not valid...",
                        "\tat nl.info.bva.clang.job.ClangUserExportJob.executeJobUsingService(ClangUserExportJob.java:26)",
                        "\tat nl.info.bva.clang.job.ClangUserExportJob.executeJobUsingService(ClangUserExportJob.java:13)",
                        "\tat nl.info.bva.model.service.impl.schedule.AbstractContextAwareQuartzJobBean.executeInternal(AbstractContextAwareQuartzJobBean.java:39)",
                        "\t... 5 more",
                        "|#]"
        );
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
