package utils_steger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * ADS-B message parsed and stuffed into the fields.
 * 
 * @author Steger
 *
 */
public class ADSBMessage {
    public String msgType;

    public Integer transmissionType;

    public Integer sessionID;

    public Integer aircraftID;

    public String hexIdent;

    public String flightID;

    public String genDateTime;

    public String logDateTime;

    public Long genUnixEpoch;

    public Long logUnixEpoch;

    public Integer callsign;

    public Double altitude;

    public Double groundSpeed;

    public Double track;

    public Double latitude;

    public Double longitude;

    public Double verticalRate;

    public String squawk;

    public String alert;

    public Integer emergency;

    public String spi;

    public String isOnGround;

    public ADSBMessage() {

    }

    public ADSBMessage(List<String> words) {
        // System.out.println(words.size());
        setMsgType(words.get(0));
        setTransmissionType(convertStringToInt(words.get(1)));
        setSessionID(convertStringToInt(words.get(2)));
        setAircraftID(convertStringToInt(words.get(3)));
        setHexIdent(words.get(4));
        setFlightID(words.get(5));
        setGenUnixEpoch(convertToUnixEpoch(words.get(6), words.get(7)));
        setLogUnixEpoch(convertToUnixEpoch(words.get(8), words.get(9)));
        setGenDateTime(words.get(6) + words.get(7));
        if (words.size() > 10) {
            setLogDateTime(words.get(8) + words.get(9));
            setCallsign(convertStringToInt(words.get(10)));
            setAltitude(convertStringToDouble(words.get(11)));
            setGroundSpeed(convertStringToDouble(words.get(12)));
            setTrack(convertStringToDouble(words.get(13)));
            setLatitude(convertStringToDouble(words.get(14)));
            setLongitude(convertStringToDouble(words.get(15)));
            setVerticalRate(convertStringToDouble(words.get(16)));
            setSquawk(words.get(17));
            setAlert(words.get(18));
            setEmergency(convertStringToInt(words.get(19)));
            setSpi(words.get(20));
            setIsOnGround(words.get(21));
        }
    }

    private Integer convertStringToInt(
            String text) {
        if (text == null || text.isEmpty()) {
            return null;
        } else {
            try {
                Integer res = Integer.parseInt(text);
                return res;
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }

    private Double convertStringToDouble(
            String text) {
        if (text == null || text.isEmpty()) {
            return null;
        } else {
            try {
                Double res = Double.parseDouble(text);
                return res;
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }

    private Long convertToUnixEpoch(
            String dateStr,
            String timeStr) {
        if (dateStr == null || dateStr.isEmpty() || timeStr == null
                || timeStr.isEmpty()) {
            return null;
        } else {
            try {
                DateFormat datef = new SimpleDateFormat("yyyy/MM/dd");
                Date date = datef.parse(dateStr);
                DateFormat timef = new SimpleDateFormat("HH:mm:ss.SSS");
                Date time = timef.parse(timeStr);
                // System.out.println(date.getTime() + ", " + time.getTime());
                return date.getTime() + time.getTime();
            } catch (ParseException e) {
                return null;
            }
        }
    }

    private String print(
            Object obj) {
        if (obj == null) {
            return "";
        } else {
            return obj.toString();
        }
    }

    public String toString() {
        return print(msgType) + ", " + print(transmissionType) + ", "
                + print(sessionID) + ", " + print(aircraftID) + ", "
                + print(hexIdent) + ", " + print(flightID) + ", "
                + print(genUnixEpoch) + ", " + print(logUnixEpoch) + ", "
                + print(genDateTime) + ", " + print(logDateTime) + ", "
                + print(callsign) + ", " + print(altitude) + ", " + ", "
                + print(groundSpeed) + ", " + print(latitude) + ", "
                + print(longitude) + ", " + print(verticalRate) + ", "
                + print(squawk) + ", " + print(spi) + ", " + print(isOnGround);
    }

    public String getMsgType() {
        return msgType;
    }

    private void setMsgType(
            String msgType) {
        this.msgType = msgType;
    }

    public Integer getTransmissionType() {
        return transmissionType;
    }

    private void setTransmissionType(
            Integer transmissionType) {
        this.transmissionType = transmissionType;
    }

    public Integer getSessionID() {
        return sessionID;
    }

    private void setSessionID(
            Integer sessionID) {
        this.sessionID = sessionID;
    }

    public Integer getAircraftID() {
        return aircraftID;
    }

    private void setAircraftID(
            Integer aircraftID) {
        this.aircraftID = aircraftID;
    }

    public String getHexIdent() {
        return hexIdent;
    }

    private void setHexIdent(
            String hexIdent) {
        this.hexIdent = hexIdent;
    }

    public String getFlightID() {
        return flightID;
    }

    private void setFlightID(
            String flightID) {
        this.flightID = flightID;
    }

    public Long getGenUnixEpoch() {
        return genUnixEpoch;
    }

    private void setGenUnixEpoch(
            Long genEpoch) {
        this.genUnixEpoch = genEpoch;
    }

    public Long getLogUnixEpoch() {
        return logUnixEpoch;
    }

    private void setLogUnixEpoch(
            Long logEpoch) {
        this.logUnixEpoch = logEpoch;
    }

    public Integer getCallsign() {
        return callsign;
    }

    private void setCallsign(
            Integer callsign) {
        this.callsign = callsign;
    }

    public Double getAltitude() {
        return altitude;
    }

    private void setAltitude(
            Double altitude) {
        this.altitude = altitude;
    }

    public Double getGroundSpeed() {
        return groundSpeed;
    }

    private void setGroundSpeed(
            Double groundSpeed) {
        this.groundSpeed = groundSpeed;
    }

    public Double getTrack() {
        return track;
    }

    private void setTrack(
            Double track) {
        this.track = track;
    }

    public Double getLatitude() {
        return latitude;
    }

    private void setLatitude(
            Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    private void setLongitude(
            Double longitude) {
        this.longitude = longitude;
    }

    public Double getVerticalRate() {
        return verticalRate;
    }

    private void setVerticalRate(
            Double verticalRate) {
        this.verticalRate = verticalRate;
    }

    public String getSquawk() {
        return squawk;
    }

    private void setSquawk(
            String squawk) {
        this.squawk = squawk;
    }

    public String getAlert() {
        return alert;
    }

    private void setAlert(
            String alert) {
        this.alert = alert;
    }

    public Integer getEmergency() {
        return emergency;
    }

    private void setEmergency(
            Integer emergency) {
        this.emergency = emergency;
    }

    public String getSpi() {
        return spi;
    }

    private void setSpi(
            String spi) {
        this.spi = spi;
    }

    public String getIsOnGround() {
        return isOnGround;
    }

    private void setIsOnGround(
            String isOnGround) {
        this.isOnGround = isOnGround;
    }

    public String getGenDateTime() {
        return genDateTime;
    }

    public void setGenDateTime(
            String genDateTime) {
        this.genDateTime = genDateTime;
    }

    public String getLogDateTime() {
        return logDateTime;
    }

    private void setLogDateTime(
            String logDateTime) {
        this.logDateTime = logDateTime;
    }

}
