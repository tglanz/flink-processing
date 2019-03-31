package com.tglanz.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

public class WordCountJob {

	public static class Element {
		public String word;
		public int count;

		public Element(String word, int count){
			this.word = word;
			this.count = count;
		}
	}

	public static void main(String[] args) throws Exception {

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
			System.out.println("Use --outupt to specify file output.");
			return;
		}

		final String outputPath = params.get("output");
		final int port = params.getInt("port");

		// get input data
		DataStream<String> text = env.socketTextStream("localhost", port, "\n");

		DataStream<Element> aggregated = text
			//.flatMap(new Tokenizer())
			.flatMap(new FlatMapFunction<String, Element>() {
				@Override
				public void flatMap(String line, Collector<Element> collector){
					for (String word : line.split("\\s")){
						collector.collect(new Element(word, 1));
					}
				}
			})
			.keyBy((Element element) -> element.word)
			.timeWindow(Time.seconds(10))
			.reduce(new ReduceFunction<Element>(){
				@Override
				public Element reduce(Element left, Element right) throws Exception {
					return new Element(left.word, left.count + right.count);
				}
			});
		
		DataStream<String> lines = aggregated
			.map((Element element)-> String.format("%s=%d", element.word, element.count));

		final StreamingFileSink<String> sink = StreamingFileSink
			.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
			.build();

		lines.addSink(sink);

		env.execute("Stream, Socket, Word Count");
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