import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Date;

public class CDCTemperatureMapper implements MapFunction<CDCTempPress, Tuple4<Integer, Double, String, String>> {

    @Override
    public Tuple4<Integer, Double, String, String> map(CDCTempPress cdcTempPress) {
        String howWarm = "";
        if (cdcTempPress.getTemp() >= 15) {
            howWarm = "warm";
        } else if (cdcTempPress.getTemp() < 15 && cdcTempPress.getTemp() > 0) {
            howWarm = "kalt";
        } else {
            howWarm = "frost";
        }

        Date date = new Date(cdcTempPress.getTime());
        String dateString = date.toString();
        Tuple4<Integer, Double, String, String> tuple = new Tuple4(cdcTempPress.getStationid(), cdcTempPress.getTemp(), howWarm, dateString);
        return tuple;
    }
}
