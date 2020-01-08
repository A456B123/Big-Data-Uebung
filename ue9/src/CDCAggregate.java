import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Date;

public class CDCAggregate implements AggregateFunction<CDCTempPress, CDCAccumulator, Tuple4<Integer, Double, String, String>> {
    @Override
    public CDCAccumulator createAccumulator() {
        CDCAccumulator cdcAccumulator = new CDCAccumulator();
        cdcAccumulator.tempMax = Double.MIN_VALUE;
        cdcAccumulator.tempMin = Double.MAX_VALUE;
        return cdcAccumulator;
    }

    @Override
    public CDCAccumulator add(CDCTempPress cdcTempPress, CDCAccumulator cdcAccumulator) {
        cdcAccumulator.stationId = cdcTempPress.getStationid();

        if (cdcAccumulator.tempMax < cdcTempPress.getTemp()) {
            cdcAccumulator.tempMax = cdcTempPress.getTemp();
            cdcAccumulator.timeTempMax = cdcTempPress.getTime();
        } else if (cdcAccumulator.tempMin > cdcTempPress.getTemp()) {
            cdcAccumulator.tempMin = cdcTempPress.getTemp();
            cdcAccumulator.timeTempMin = cdcTempPress.getTime();
        }
        return cdcAccumulator;
    }

    @Override
    public Tuple4<Integer, Double, String, String> getResult(CDCAccumulator cdcAccumulator) {
        double temperatureDiff = cdcAccumulator.tempMax - cdcAccumulator.tempMin;
        if (cdcAccumulator.timeTempMax < cdcAccumulator.timeTempMin) {
            temperatureDiff *= -1;
        }
        String timeTempMax = new Date(cdcAccumulator.timeTempMax).toString();
        String timeTempMin = new Date(cdcAccumulator.timeTempMin).toString();
        return new Tuple4<>(
                cdcAccumulator.stationId,
                temperatureDiff,
                timeTempMax,
                timeTempMin);
    }

    @Override
    public CDCAccumulator merge(CDCAccumulator cdcAccumulator1, CDCAccumulator cdcAccumulator2) {
        if (cdcAccumulator1.tempMax < cdcAccumulator2.tempMax) {
            cdcAccumulator1.tempMax = cdcAccumulator2.tempMax;
            cdcAccumulator1.timeTempMax = cdcAccumulator2.timeTempMax;
            cdcAccumulator1.stationId = cdcAccumulator2.stationId;
        } else if (cdcAccumulator1.tempMin > cdcAccumulator2.tempMin) {
            cdcAccumulator1.tempMin = cdcAccumulator2.tempMin;
            cdcAccumulator1.timeTempMin = cdcAccumulator2.timeTempMin;
            cdcAccumulator1.stationId = cdcAccumulator2.stationId;
        }

        return cdcAccumulator1;
    }
}
