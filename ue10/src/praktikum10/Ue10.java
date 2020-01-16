package praktikum10;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import utils_steger.ADSBMessage;
import utils_steger.ADSBMessageToString;
import utils_steger.ADSBSplitter;

/*
 * Main-Class die die Aufgaben ausführt.
 */
public class Ue10 {

	private static final String FILEPATH = "data/";
	private static final String KURZ = "adsb.txt";

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

		preparingEnv(env);
		// DataStream<String> stringStream = env.socketTextStream("193.174.205.49", 50500);
		DataStream<String> stringStream = env.readTextFile(FILEPATH + KURZ);
		DataStream<String> stringStreamFiltered = stringStream
				.filter(i -> i.startsWith("MSG,2") || i.startsWith("MSG,3"));
		DataStream<ADSBMessage> adsbMessageStream = stringStreamFiltered.map(new ADSBSplitter());

		DataStream<ADSBMessage> adsbTimeStream = adsbMessageStream
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ADSBMessage>() {
					@Override
					public long extractAscendingTimestamp(ADSBMessage adsbMessage) {
//						System.out.println("Timestamp " + adsbMessage.getGenUnixEpoch());
						return adsbMessage.getGenUnixEpoch();
					};
				});

//		adsbMessageStream.print();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
//		tableEnv.registerDataStreamInternal("adsbMessageTableA", adsbMessageStream);
		tableEnv.registerDataStream("adsbMessageTable", adsbTimeStream,
				"hexIdent, genUnixEpoch, latitude, longitude, altitude, rowtime.rowtime");

		aufgabeA(tableEnv);

		aufgabeB(tableEnv);

		aufgabeC(tableEnv);

		aufgabeD(tableEnv);

		env.execute();

	}

	private static void aufgabeA(StreamTableEnvironment tableEnv) {
		Table tab = null;
		tab = tableEnv
				.sqlQuery("SELECT hexIdent, genUnixEpoch, latitude, longitude, altitude " + "FROM adsbMessageTable");
		DataStream<Row> appendStream = tableEnv.toAppendStream(tab, Row.class);
		appendStream.print();
		DataStream<String> stringStream = appendStream.map(new ADSBMessageToString());
		stringStream.writeToSocket("localhost", 50123, new SimpleStringSchema());
//		stringStream.print();
//		tableEnv.execEnv();
	}

	private static void aufgabeB(StreamTableEnvironment tableEnv) {
		Table tab = null;
		tab = tableEnv.sqlQuery(
				"SELECT COUNT(DISTINCT hexIdent) AS anzahl, " + "TUMBLE_END(rowtime, INTERVAL '5' SECOND) AS tend "
						+ "FROM adsbMessageTable " + "GROUP BY  TUMBLE(rowtime, INTERVAL '5' SECOND)");
		DataStream<Row> appendStream = tableEnv.toAppendStream(tab, Row.class);
//		appendStream.print();
		DataStream<String> stringStream = appendStream.map(new ADSBMessageToString());
		stringStream.print();
		stringStream.writeToSocket("localhost", 50124, new SimpleStringSchema());
	}

	public static void aufgabeC(StreamTableEnvironment tableEnv) {
		Table tab = tableEnv.sqlQuery("SELECT COUNT(DISTINCT hexIdent) AS Anzahl, "
				+ "HOP_START(rowtime, INTERVAL '1' SECOND, INTERVAL '30' SECOND) AS Intervallstart, "
				+ "HOP_END(rowtime, INTERVAL '1' SECOND, INTERVAL '30' SECOND) AS Intervallend "
				+ "FROM adsbMessageTable " + "GROUP BY " + "HOP(rowtime, INTERVAL '1' SECOND, INTERVAL '30' SECOND)");
		DataStream<String> appendStream = tableEnv.toAppendStream(tab, Row.class).map(new ADSBMessageToString());
		appendStream.print();
		appendStream.writeToSocket("localhost", 50126, new SimpleStringSchema());
	}

	public static void aufgabeD(StreamTableEnvironment tableEnv) {
		Table tab = tableEnv.sqlQuery(
				"SELECT ROUND(latitude, 1) AS lat, ROUND(longitude, 1) AS long, COUNT(DISTINCT hexIdent) AS Anzahl "
				 + " FROM adsbMessageTable" 
				 + " GROUP BY ROUND(latitude, 1), ROUND(longitude, 1)");

		DataStream<String> retractStream = tableEnv.toRetractStream(tab, Row.class)
				.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple2<Boolean, Row> t) throws Exception {
						return t.f0;
					}

				}).map(new MapFunction<Tuple2<Boolean, Row>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String map(Tuple2<Boolean, Row> t) throws Exception {
						return t.f1.toString() + "\n";
					}

				});
		retractStream.print();
		retractStream.writeToSocket("localhost", 50125, new SimpleStringSchema());
	}

	private static void preparingEnv(StreamExecutionEnvironment env) {

		// start a checkpoint every 1000 ms
		env.enableCheckpointing(1000);

		// advanced options:
		// set mode to exactly-once (this is the default)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// make sure 500 ms of progress happen between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

		// checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(3000);

		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(5);

		// enable externalized checkpoints which are retained after job cancellation
		env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

	}

}
