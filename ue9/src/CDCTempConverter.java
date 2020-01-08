import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Convert a date/time value from a CDC record to a unix epoch value.
 * 
 * @author nsteger
 *
 */
public class CDCTempConverter implements
        MapFunction<CDCTempPress, CDCTempPress> {
    private static final long serialVersionUID = 1L;

    @Override
    public CDCTempPress map(CDCTempPress t) {
        long timecdc = t.getTime();
        int year = (int) (timecdc / 100000000);
        int month = (int) (timecdc % 100000000) / 1000000;
        int day = (int) (timecdc % 1000000) / 10000;
        int hour = (int) (timecdc % 10000) / 100;
        int minute = (int) (timecdc % 100);

        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(Calendar.DAY_OF_MONTH, day);
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.HOUR_OF_DAY, hour);

        t.setTime(calendar.getTimeInMillis());

        return t;
    }
}
