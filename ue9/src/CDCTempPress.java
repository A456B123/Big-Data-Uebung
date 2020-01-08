/**
 * Data from german Climate Data Network in 10 minute resolution.
 * 
 * @author nsteger
 *
 */
public class CDCTempPress {
    /**
     * Stationid of CDC
     */
    private int stationid;

    /**
     * Unix epoch timestamp of measurement
     */
    private long time;

    /**
     * Air pressure
     */
    private double pressure;

    /**
     * Air temperature
     */
    private double temp;

    public int getStationid() {
        return stationid;
    }

    public void setStationid(int stationid) {
        this.stationid = stationid;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getPressure() {
        return pressure;
    }

    public void setPressure(double pressure) {
        this.pressure = pressure;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    public String toString() {
        return getStationid() + ", " + getTime() + ", " + getPressure() + ", "
                + getTemp();
    }

}
