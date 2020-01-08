import org.apache.flink.api.common.functions.MapFunction;

/**
 * Map function for converting data from a line of CDC 10 minute temperature and
 * pressure values to a CDCTempPress record.
 * 
 * @author nsteger
 *
 */
public class CDCSplitter implements MapFunction<String, CDCTempPress> {
    private static final long serialVersionUID = 0;

    @Override
    public CDCTempPress map(String line) throws Exception {
        CDCTempPress res = new CDCTempPress();
        String[] word = line.split(";");
        res.setStationid(Integer.parseInt(word[0].trim()));
        res.setTime(Long.parseLong(word[1].trim()));
        res.setPressure(Double.parseDouble(word[3].trim()));
        res.setTemp(Double.parseDouble(word[4].trim()));
        return res;
    }

}
