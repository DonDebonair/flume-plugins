/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package nl.info.flume.source;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.CHARSET;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.CONFIG_BATCH_SIZE;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.CONFIG_LOG_STDERR;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.CONFIG_RESTART;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.DEFAULT_CHARSET;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.DEFAULT_LOG_STDERR;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.DEFAULT_RESTART;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE;
import static nl.info.flume.source.MultiLineExecSourceConfigurationConstants.DEFAULT_LINE_TERMINATOR;

/**
 * <p>
 * An implementation that executes a Unix process and turns each
 * group of lines of text, terminated by a certain line-terminator into an event.
 * It will
 */
public class MultiLineExecSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger logger = LoggerFactory.getLogger(nl.info.flume.source.MultiLineExecSource.class);

	private String command;
    private String eventTerminator;
	private String lineTerminator;
	private CounterGroup counterGroup;
	private ExecutorService executor;
	private Future<?> runnerFuture;
	private long restartThrottle;
	private boolean restart;
	private boolean logStderr;
	private Integer bufferCount;
	private ExecRunnable runner;
	private Charset charset;

	@Override
	public void start() {
		logger.info("Multi Line Exec source starting with command: {}, event terminator: {}", command, eventTerminator);

		executor = Executors.newSingleThreadExecutor();
		counterGroup = new CounterGroup();

		runner = new ExecRunnable(command, eventTerminator, lineTerminator, getChannelProcessor(), counterGroup,
				  restart, restartThrottle, logStderr, bufferCount, charset);

		// FIXME: Use a callback-like executor / future to signal us upon failure.
		runnerFuture = executor.submit(runner);

    /*
     * NB: This comes at the end rather than the beginning of the method because
     * it sets our state to running. We want to make sure the executor is alive
     * and well first.
     */
		super.start();

		logger.debug("Multi Line Exec source started");
	}

	@Override
	public void stop() {
		logger.info("Stopping Multi Line exec source with command: {}, event terminator: {}", command, eventTerminator);

		if (runner != null) {
			runner.setRestart(false);
			runner.kill();
		}
		if (runnerFuture != null) {
			logger.debug("Stopping Multi Line exec runner");
			runnerFuture.cancel(true);
			logger.debug("Multi Line Exec runner stopped");
		}

		executor.shutdown();

		while (!executor.isTerminated()) {
			logger.debug("Waiting for Multi Line exec executor service to stop");
			try {
				executor.awaitTermination(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				logger.debug("Interrupted while waiting for Multi Line exec executor service to stop. Just exiting.");
				Thread.currentThread().interrupt();
			}
		}

		super.stop();

		logger.debug(format("Multi Line Exec source with command: %s, event terminator: %s stopped. Metrics: %s", command, eventTerminator, counterGroup));
	}

	@Override
	public void configure(Context context) {
		command = context.getString("command");
		eventTerminator = context.getString("event.terminator");
        lineTerminator = context.getString("line.terminator", DEFAULT_LINE_TERMINATOR);

		Preconditions.checkState(command != null, "The parameter command must be specified");
		Preconditions.checkState(lineTerminator != null, "The parameter line.terminator must be specified");

		restartThrottle = context.getLong(CONFIG_RESTART_THROTTLE, DEFAULT_RESTART_THROTTLE);
		restart = context.getBoolean(CONFIG_RESTART, DEFAULT_RESTART);
		logStderr = context.getBoolean(CONFIG_LOG_STDERR, DEFAULT_LOG_STDERR);
		bufferCount = context.getInteger(CONFIG_BATCH_SIZE, DEFAULT_BATCH_SIZE);
		charset = Charset.forName(context.getString(CHARSET, DEFAULT_CHARSET));
	}

	protected static class ExecRunnable implements Runnable {

		public ExecRunnable(String command, String eventTerminator, String lineTerminator, ChannelProcessor channelProcessor, CounterGroup counterGroup, boolean restart, long restartThrottle, boolean logStderr, int bufferCount, Charset charset) {
			this.command = command;
            this.eventTerminator = eventTerminator;
			this.lineTerminator = lineTerminator;
			this.channelProcessor = channelProcessor;
			this.counterGroup = counterGroup;
			this.restartThrottle = restartThrottle;
			this.bufferCount = bufferCount;
			this.restart = restart;
			this.logStderr = logStderr;
			this.charset = charset;
		}

		private String command;
        private String eventTerminator;
		private String lineTerminator;
		private ChannelProcessor channelProcessor;
		private CounterGroup counterGroup;
		private volatile boolean restart;
		private long restartThrottle;
		private int bufferCount;
		private boolean logStderr;
		private Charset charset;
		private Process process = null;

		@Override
		public void run() {
			do {
				String exitCode;
				BufferedReader reader = null;
				try {
					process = startedCommandProcessBuilder(Arrays.asList(command.split("\\s+")));
					reader = getBufferedReader();

					// StderrLogger dies as soon as the input stream is invalid
					StderrReader stderrReader = getStderrReader();
					stderrReader.setName("StderrReader-[" + command + "]");
					stderrReader.setDaemon(true);
					stderrReader.start();

					String line;
					boolean skipNextEmptyLine = false;
					List<Event> eventList = new ArrayList<Event>();
					List<String> buffer = new ArrayList<String>();
					while ((line = reader.readLine()) != null) {
						if (line.isEmpty() && skipNextEmptyLine) {
							skipNextEmptyLine = false;
							continue;
						}

						buffer.add(line);

						if (line.endsWith(eventTerminator)) {
							counterGroup.incrementAndGet("multi.line.exec.events.read");
							String eventBody = StringUtils.join(buffer.toArray(), lineTerminator);
							buffer.clear();
							eventList.add(EventBuilder.withBody(eventBody.getBytes(charset)));
							skipNextEmptyLine = true;
						}

						if (eventList.size() >= bufferCount) {
							channelProcessor.processEventBatch(eventList);
							eventList.clear();
						}
					}
					if (!eventList.isEmpty()) {
						channelProcessor.processEventBatch(eventList);
					}
				} catch (Exception e) {
					logger.error("Failed while running command: " + command, e);
					if (e instanceof InterruptedException) {
						Thread.currentThread().interrupt();
					}
				} finally {
					if (reader != null) {
						try {
							reader.close();
						} catch (IOException ex) {
							logger.error("Failed to close reader for Multi Line exec source", ex);
						}
					}
					exitCode = String.valueOf(kill());
				}
				if (restart) {
					logger.info("Restarting in {}ms, exit code {}", restartThrottle, exitCode);
					try {
						Thread.sleep(restartThrottle);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				} else {
					logger.info("Command [" + command + "] exited with " + exitCode);
				}
			} while (restart);
		}

		protected StderrReader getStderrReader() {
			return new StderrReader(new BufferedReader(new InputStreamReader(process.getErrorStream(), charset)), logStderr);
		}

		protected BufferedReader getBufferedReader() {
			return new BufferedReader(new InputStreamReader(process.getInputStream(), charset));
		}

		protected Process startedCommandProcessBuilder(List<String> commandArgs) throws IOException {
			return new ProcessBuilder(commandArgs).start();
		}

		public int kill() {
			if (process != null) {
				synchronized (process) {
					process.destroy();
					try {
						return process.waitFor();
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				return Integer.MIN_VALUE;
			}
			return Integer.MIN_VALUE / 2;
		}

		public void setRestart(boolean restart) {
			this.restart = restart;
		}
	}

	protected static class StderrReader extends Thread {
		private BufferedReader input;
		private boolean logStderr;

		protected StderrReader(BufferedReader input, boolean logStderr) {
			this.input = input;
			this.logStderr = logStderr;
		}

		@Override
		public void run() {
			try {
				int i = 0;
				String line;
				while ((line = input.readLine()) != null) {
					if (logStderr) {
						// There is no need to read 'line' with a charset
						// as we do not to propagate it.
						// It is in UTF-16 and would be printed in UTF-8 format.
						logger.info("StderrLogger[{}] = '{}'", ++i, line);
					}
				}
			} catch (IOException e) {
				logger.info("StderrLogger exiting", e);
			} finally {
				try {
					if (input != null) {
						input.close();
					}
				} catch (IOException ex) {
					logger.error("Failed to close stderr reader for Multi Line exec source", ex);
				}
			}
		}

	}
}
