import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class Ue9 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        try {
            //String fileName = "cdckurz.txt";
            String fileName = "cdclang.txt";

            // a)
            DataStream<CDCTempPress> cdcStream = PartA.readFromFile(env, fileName);

            cdcStream = cdcStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CDCTempPress>() {
                @Override
                public long extractAscendingTimestamp(CDCTempPress element) {
                    return element.getTime();
                }
            });

            // b)
            //PartB.tempType(env, cdcStream);

            // c)
            //PartC.minTempByStation(env, cdcStream);

            // d)
            //PartD.minTempByStationPerHour(env, cdcStream);

            // e)
            PartE.minMaxWithAggregate(env, cdcStream);

            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
