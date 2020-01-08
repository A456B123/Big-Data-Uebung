import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PartD {
    public static void minTempByStationPerHour(StreamExecutionEnvironment env, DataStream<CDCTempPress> stream) throws Exception {
        DataStream<CDCTempPress> transformedStream = stream
                .keyBy(CDCTempPress::getStationid)
                .window(SlidingEventTimeWindows.of(Time.hours(3), Time.hours(1)))
                .min("temp")
                .filter(z -> z.getTemp() != -999.0);

        transformedStream.print();
    }
}
