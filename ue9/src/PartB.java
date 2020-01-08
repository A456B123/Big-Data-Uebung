import org.apache.flink.api.java.tuple.Tuple4;
        import org.apache.flink.streaming.api.datastream.DataStream;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartB {
    public static void tempType(StreamExecutionEnvironment env, DataStream<CDCTempPress> stream) throws Exception {
        DataStream<Tuple4<Integer, Double, String, String>> convertedStream = stream.map(new CDCTemperatureMapper()).filter(z -> z.f1 != -999.0);
        convertedStream.print();
    }
}
