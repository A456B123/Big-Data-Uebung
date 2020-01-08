import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PartC {
    public static void minTempByStation(StreamExecutionEnvironment env, DataStream<CDCTempPress> stream) throws Exception {
        DataStream<CDCTempPress> transformedStream = stream
                .keyBy(CDCTempPress::getStationid)
                .window(TumblingEventTimeWindows.of(Time.hours(3)))
                .min("temp")
                .filter(z -> z.getTemp() != -999.0);

        transformedStream.print();
    }
}
