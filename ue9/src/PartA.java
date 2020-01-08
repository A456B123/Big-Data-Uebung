import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartA {
    public static DataStream<CDCTempPress> readFromFile(StreamExecutionEnvironment env, String fileName) throws Exception {
        DataStream<String> stringStream = env.readTextFile(fileName);
        DataStream<CDCTempPress> convertedStream = stringStream.map(new CDCSplitter()).map(new CDCTempConverter());

        //stringStream.print();
        //convertedStream.print();

        return convertedStream;
    }
}
