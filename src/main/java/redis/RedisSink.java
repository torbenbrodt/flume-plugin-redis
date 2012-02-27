/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redis;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.nio.ByteBuffer;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 */
public class RedisSink extends EventSink.Base {
	static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
	private BinaryJedis client;
	private byte[] key;
	private String hostname;
	private int port;

	public RedisSink(final String key, final String hostname, int port) {
		this.client   = new BinaryJedis(hostname, port);
		this.key      = key.getBytes();
		this.hostname = hostname;
		this.port     = port;
	}

	@Override
	public void open() throws IOException {
		// Connect to redis
		LOG.info("connect to redis");
		try {
			this.client.connect();
		} catch(JedisException ex) {
			throw new IOException("Failed to connect to redis: " + ex.getMessage());
		}
	}

	@Override
	public void append(Event e) throws IOException {
		// append the event to the output
		com.cloudera.flume.handlers.thrift.ThriftFlumeEvent tfe = RedisSink.toThriftEvent(e);
		byte[] bytes;

		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
		try {
			bytes = serializer.serialize(tfe);
		} catch(org.apache.thrift.TException ex) {
			LOG.error("exception while serializing thriftevent: " + ex);
			throw new IOException(ex);
		}

		// here we are assuming the body is a string
		try {
			this.client.lpush(this.key, bytes);
		} catch (JedisConnectionException cex) {
			// maybe the server closed the connection so we try one
			// reconnect
			LOG.warn("redis server maybe closed the connection, doing _one_ retry: " + cex.getMessage());
			try {
				this.client.disconnect();
				this.client.connect();
			} catch (JedisException dcex) {
				throw new IOException("Failed to append to redis (can't connect for second try): " + dcex.getMessage());
			}

			try {
				this.client.lpush(this.key, bytes);
			} catch (JedisException scex) {
				throw new IOException("Failed to append to redis on second try: " + scex.getMessage());
			}
		} catch (JedisException ex) {
			throw new IOException("Failed to append to redis: " + ex.getMessage());
		}
	}

	@Override
	public void close() {
		LOG.info("disconnect from redis");
		try {
			this.client.disconnect();
		} catch (JedisException ex) {
			// new jedis instance
			this.client = new BinaryJedis(hostname, port);
		}
	}

	public static SinkBuilder builder() {
		return new SinkBuilder() {
			// construct a new parameterized sink
			@Override
			public EventSink build(Context context, String... argv) {
				Preconditions.checkArgument(argv.length >= 2 && argv.length < 4,
						"usage: redisSink(key, hostname [, port = 6379 ]");

				String key      = argv[0];
				String hostname = argv[1];
				int port = 6379;

				if (argv.length == 3) {
					port = Integer.parseInt(argv[2]);
				}


				return new RedisSink(key, hostname, port);
			}
		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this class
	 * as a plugin sink.
	 */
	public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
		List<Pair<String, SinkBuilder>> builders =
			new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("redisSink", builder()));
		return builders;
	}


	private static com.cloudera.flume.handlers.thrift.ThriftFlumeEvent toThriftEvent(Event e) {
		com.cloudera.flume.handlers.thrift.ThriftFlumeEvent evt = new com.cloudera.flume.handlers.thrift.ThriftFlumeEvent();
		evt.timestamp = e.getTimestamp();
		evt.priority = toThriftPriority(e.getPriority());
		ByteBuffer buf = ByteBuffer.wrap(e.getBody());
		evt.body = buf;
		evt.nanos = e.getNanos();
		evt.host = e.getHost();

		Map<String, byte[]> tempMap = e.getAttrs();
		Map<String, ByteBuffer> returnMap = new HashMap<String, ByteBuffer>();
		for (String key : tempMap.keySet()) {
			buf.clear();
			buf = ByteBuffer.wrap(tempMap.get(key));
			returnMap.put(key, buf);
		}

		evt.fields = returnMap;
		return evt;
	}

	private static com.cloudera.flume.handlers.thrift.Priority toThriftPriority(
			com.cloudera.flume.core.Event.Priority p) {
		Preconditions.checkNotNull(p, "Argument must not be null.");

		switch (p) {
			case FATAL:
				return com.cloudera.flume.handlers.thrift.Priority.FATAL;
			case ERROR:
				return com.cloudera.flume.handlers.thrift.Priority.ERROR;
			case WARN:
				return com.cloudera.flume.handlers.thrift.Priority.WARN;
			case INFO:
				return com.cloudera.flume.handlers.thrift.Priority.INFO;
			case DEBUG:
				return com.cloudera.flume.handlers.thrift.Priority.DEBUG;
			case TRACE:
				return com.cloudera.flume.handlers.thrift.Priority.TRACE;
			default:
				throw new IllegalStateException("Unknown value " + p);
		}
	}
}
