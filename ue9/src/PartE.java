import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PartE {
    public static void minMaxWithAggregate(StreamExecutionEnvironment env, DataStream<CDCTempPress> stream) throws Exception {
        DataStream<Tuple4<Integer, Double, String, String>> transformedStream = stream
                .keyBy(CDCTempPress::getStationid)
                .window(TumblingEventTimeWindows.of(Time.hours(3)))
                .aggregate(new CDCAggregate())
                .filter(z -> Math.abs(z.f1) != 999.0)
                .filter(z -> Math.abs(z.f1) < 7d);

        transformedStream.print();
    }
}
