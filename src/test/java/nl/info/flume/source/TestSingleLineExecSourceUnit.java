package nl.info.flume.source;

/*
@RunWith(MockitoJUnitRunner.class)
public class TestSingleLineExecSourceUnit {
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
	private ExecSource.ExecRunnable execRunnable = new ExecSource.ExecRunnable(command[0], channelProcessor, counterGroup, false, 0L, false, 10, Charset.defaultCharset());

	@Before
	public void setUpBeforeClass() throws IOException {
		doReturn(null).when(execRunnable).startedCommandProcessBuilder(anyListOf(String.class));
		doReturn(bufferedReader).when(execRunnable).getBufferedReader();
		doReturn(stderrReader).when(execRunnable).getStderrReader();
	}

	@Test
	public void testRunSingleLineExecSource() throws IOException {
		final int[] counter = {0};
		final List<String> lines = Arrays.asList("a111", "b", "b", "asld", null);

		when(bufferedReader.readLine()).thenAnswer(new Answer<String>() {
			@Override
			public String answer(InvocationOnMock invocationOnMock) throws Throwable {
				return lines.get(counter[0]++);
			}
		}
		);

		execRunnable.run();

		ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
		verify(channelProcessor).processEventBatch(argument.capture());


		deepComparison(argument.getValue(), lines);
		Event e1 = (Event) argument.getValue().get(0);
		assertEquals((byte) lines.get(0).charAt(0), e1.getBody()[0]);

		e1 = (Event) argument.getValue().get(1);
		assertEquals((byte) 'b', e1.getBody()[0]);
	}

	private void deepComparison(List values, List<String> lines) {
		assertEquals(lines.size() - 1, values.size());
		int counter = 0;

		for (String line : lines) {
			if (line == null) {
				continue;
			}

			byte[] value = ((Event) values.get(counter++)).getBody();
			assertEquals(line.length(), value.length);
			assertEquals(line, new String(value));
		}
	}
}
*/
