package com.tglanz.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import com.tglanz.sources.udp.UdpSource;
import com.tglanz.sources.udp.Entry;;

public class UdpEchoJob {

	public static void main(String[] args) throws Exception {

		System.out.println("Starting a UdpEchoJob");

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		
		if (!params.has("port")){
			System.out.println("No port provided");
			System.out.println("Use --port to specify file input.");
			return;
		}

		if (!params.has("output")) {
			System.out.println("No output provided");
			System.out.println("Use --output to specify file output.");
			return;
		}

		final String outputPath = params.get("output");
		final int port = params.getInt("port");
		final UdpSource udpSource = new UdpSource(port, 256);

		DataStream<Entry> stream = env.addSource(udpSource, "UdpSource");
		
		DataStream<String> lines = stream
			.map((Entry entry)-> entry.dataString);

		final StreamingFileSink<String> sink = StreamingFileSink
			.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
			.build();

		lines.addSink(sink);

		env.execute("Stream, Udp, Echo Job");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}