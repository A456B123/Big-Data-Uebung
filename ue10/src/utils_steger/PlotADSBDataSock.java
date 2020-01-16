package utils_steger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Bounds;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.ScatterChart;
import javafx.scene.chart.XYChart;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.scene.transform.Affine;
import javafx.scene.transform.Scale;
import javafx.scene.transform.Translate;
import javafx.stage.Stage;

/**
 * Plot ADSB Data.
 * 
 * @author Steger
 *
 */

public class PlotADSBDataSock extends Application {
    final static int POSITIONSOCKET = 50123;

    final static int NUMBERAIRCRAFTSOCKET = 50124;

    final static int DENSITYSOCKET = 50125;

    final static int SLIDESOCKET = 50126;

    final static int POSWINDOWSIZE = 5000;

    final static int NUMBERWINDOWSIZE = 200;

    private ReadNumberAircraft numberAircraft = null;

    private ReadPosition position = null;

    private ReadDensity density = null;

    private ReadNumberAircraft slide = null;

    private GraphicsContext gc = null;

    public static void main(
            String[] args) {
        PlotADSBDataSock ad = new PlotADSBDataSock();
        ad.create(args);
    }

    public void create(
            String[] args) {

        launch(args);
    }

    @Override
    public void start(
            Stage primaryStage) throws Exception {
        primaryStage.setTitle("Automatic Dependence Surveillance - Broadcast");

        // Start a separate Thread for each socket that listens to data sent by
        // Flink to the corresponding port.
        numberAircraft = new ReadNumberAircraft(NUMBERAIRCRAFTSOCKET);
        new Thread(numberAircraft).start();

        slide = new ReadNumberAircraft(SLIDESOCKET);
        new Thread(slide).start();

        position = new ReadPosition();
        new Thread(position).start();

        density = new ReadDensity();
        new Thread(density).start();
        
        final LineChart<Date, Number> numChart = createNumberAircraftChart(
                "ADSB number of aircraft", numberAircraft);
        final LineChart<Date, Number> slideChart = createNumberAircraftChart(
                "ADSB moving average of aircraft", slide);
        final ScatterChart<Number, Number> posChart = createAircraftPositionChart();
        final Canvas dense = createDenseCanvas();

        GridPane pane = new GridPane();
        pane.add(numChart, 0, 0);
        pane.add(posChart, 1, 0);
        pane.add(dense, 0, 1);
        pane.add(slideChart, 1, 1);
        pane.setPrefHeight(1200);
        pane.setPrefWidth(1920);

        Scene scene = new Scene(pane);
        primaryStage.setScene(scene);
        primaryStage.setMaximized(true);

        scene.getStylesheets().add("scatterchart.css");

        // show the stage
        primaryStage.show();
    }

    private Canvas createDenseCanvas() {
        Canvas canvas = new Canvas(800, 600);
        Bounds bounds = canvas.getBoundsInLocal();
        double minlat = 47;
        double maxlat = 50;
        double minlon = 9.5;
        double maxlon = 13;
        double sx = bounds.getWidth() / (maxlon - minlon);
        double tx = -sx * minlon;
        double sy = -bounds.getHeight() / (maxlat - minlat);
        double ty = -sy * maxlat;
        gc = canvas.getGraphicsContext2D();
        Affine affine = new Affine();
        affine.append(new Translate(tx, ty));
        affine.append(new Scale(sx, sy));
        gc.setTransform(affine);

        return canvas;
    }

    /**
     * Create a scatter chart displaying positions for aircraft.
     * 
     * @return the chart.
     */
    private ScatterChart<Number, Number> createAircraftPositionChart() {
        final NumberAxis xAxis = new NumberAxis(9.5, 13, 0.5);
        final NumberAxis yAxis = new NumberAxis(47, 50, 0.5);
        xAxis.setLabel("Longitude");
        xAxis.setAnimated(false);
        yAxis.setLabel("Latitude");
        yAxis.setAnimated(false);

        final ScatterChart<Number, Number> posChart = new ScatterChart<>(xAxis,
                yAxis);
        posChart.setPrefHeight(600);
        posChart.setPrefWidth(800);
        XYChart.Series<Number, Number> pos = new XYChart.Series<>();
        pos.setName("Aircraft Positions");
        posChart.getData().add(pos);
        position.setSeries(pos);

        return posChart;
    }

    /**
     * Create a line chart showing the number of aircraft.
     * 
     * @param title
     *            the title of the chart.
     * @param read
     *            the reader to read the data for the chart from a socket.
     * @return the chart.
     */
    private LineChart<Date, Number> createNumberAircraftChart(
            String title,
            ReadNumberAircraft read) {
        final DateAxis xAxis = new DateAxis();
        final NumberAxis yAxis = new NumberAxis();
        xAxis.setLabel("Date and Time");
        xAxis.setAnimated(false);
        yAxis.setLabel("Number of planes in period");
        yAxis.setAnimated(false);

        final LineChart<Date, Number> lineChart = new LineChart<>(xAxis, yAxis);
        lineChart.setTitle(title);
        lineChart.setAnimated(false);
        lineChart.setPrefHeight(600);
        lineChart.setPrefWidth(800);

        // defining a series to display data
        XYChart.Series<Date, Number> series = new XYChart.Series<>();
        series.setName("Aircraft");

        // add series to chart
        lineChart.getData().add(series);
        read.setSeries(series);

        return lineChart;
    }

    /**
     * Start a thread reading the number of aircraft from a socket.
     * 
     * @author Steger
     *
     */
    private class ReadNumberAircraft implements Runnable {
        XYChart.Series<Date, Number> series = null;

        int socket;

        public ReadNumberAircraft(int socket) {
            this.socket = socket;
        }

        public final void setSeries(
                XYChart.Series<Date, Number> series) {
            this.series = series;
        }

        public void run() {
            try {
                System.out.println(
                        "Thread for ReadNumberAircraft started on socket "
                                + socket);
                ServerSocket listener = new ServerSocket(socket);
                Socket sock = listener.accept();
                listener.close();
                InputStream input = sock.getInputStream();
                BufferedReader buf = new BufferedReader(
                        new InputStreamReader(input));
                while (true) {
                    String line = buf.readLine();
                    if (line != null && !line.equals("")) {
                        // System.out.println("NumberAircraft: " + line);
                        String[] words = line.split(",");
                        int cnt = Integer.parseInt(words[0]);
                        SimpleDateFormat formatter = new SimpleDateFormat(
                                "yyyy-MM-dd HH:mm:ss");
                        Date datetime = formatter.parse(words[1]);

                        updateNumberAircraft(cnt, datetime, series);
                    }
                    Thread.sleep(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void updateNumberAircraft(
            int cnt,
            Date datetime,
            XYChart.Series<Date, Number> series) {
        // Update the chart and limit the number of tuples shown
        Platform.runLater(() -> {
            series.getData().add(new XYChart.Data<>(datetime, cnt));
            if (series.getData().size() > NUMBERWINDOWSIZE) {
                series.getData().remove(0);
            }
        });
    }

    /**
     * Start a thread reading the positions of aircraft from a socket.
     * 
     * @author Steger
     *
     */
    private class ReadPosition implements Runnable {
        XYChart.Series<Number, Number> series = null;

        public final void setSeries(
                XYChart.Series<Number, Number> series) {
            this.series = series;
        }

        public void run() {
            try {
                System.out.println("Thread for ReadPosition started");
                ServerSocket listener = new ServerSocket(POSITIONSOCKET);
                Socket sock = listener.accept();
                listener.close();
                InputStream input = sock.getInputStream();
                BufferedReader buf = new BufferedReader(
                        new InputStreamReader(input));
                while (true) {
                    String line = buf.readLine();
                    if (line != null && !line.equals("")) {
                        // System.out.println("PositionAircraft: " + line);
                        String[] words = line.split(",");
                        float lon = Float.parseFloat(words[2]);
                        float lat = Float.parseFloat(words[3]);

                        updateAircraftPosition(lat, lon, series);
                    }
                    Thread.sleep(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void updateAircraftPosition(
            float lat,
            float lon,
            XYChart.Series<Number, Number> series) {
        // Update the chart and limit the number of tuples shown
        Platform.runLater(() -> {
            series.getData().add(new XYChart.Data<>(lat, lon));
            if (series.getData().size() > POSWINDOWSIZE) {
                series.getData().remove(0);
            }
        });
    }

    /**
     * Start a thread reading the density of aircraft from a socket.
     * 
     * @author Steger
     *
     */
    private class ReadDensity implements Runnable {
        HashMap<Double, HashMap<Double, Integer>> map = new HashMap<Double, HashMap<Double, Integer>>();

        private int maxcnt = 0;

        public void run() {
            try {
                System.out.println("Thread for ReadDensity started");
                ServerSocket listener = new ServerSocket(DENSITYSOCKET);
                Socket sock = listener.accept();
                listener.close();
                InputStream input = sock.getInputStream();
                BufferedReader buf = new BufferedReader(
                        new InputStreamReader(input));
                while (true) {
                    String line = buf.readLine();
                    if (line != null && !line.equals("")) {
                        System.out.println("DensityAircraft: " + line);
                        String[] words = line.split(",");
                        float lon = Float.parseFloat(words[1]);
                        float lat = Float.parseFloat(words[0]);
                        int cnt = Integer.parseInt(words[2]);
                        updateDensity(lon, lat, cnt);
                        gc.setStroke(Color.GRAY);
                        gc.setLineWidth(0.005);
                        for (double latv = 47.5; latv<50; latv+=0.5) {
                            gc.strokeLine(10, latv, 13, latv);
                            //gc.fillText(Double.toString(latv), 10, latv);
                        }
                        for (double lonv = 10.5; lonv<13; lonv+=0.5) {
                            gc.strokeLine( lonv, 47, lonv,50);
                        }
                     }
                    Thread.sleep(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void updateDensity(
                double lon,
                double lat,
                int cnt) {
            updateField(lat, lon, cnt);
            if (cnt > maxcnt) {
                maxcnt = cnt;
                plotAllFields();
            } else {
                plotField(lat, lon, cnt);
            }
        }

        private void plotAllFields() {
            for (double lat : map.keySet()) {
                for (double lon : map.get(lat).keySet()) {
                    plotField(lat, lon, map.get(lat).get(lon));
                }
            }
        }

        private void updateField(
                double lat,
                double lon,
                int cnt) {
            if (!map.containsKey(lat)) {
                HashMap<Double, Integer> lonmap = new HashMap<Double, Integer>();
                map.put(lat, lonmap);
            }
            map.get(lat).put(lon, cnt);
        }

        private void plotField(
                double lat,
                double lon,
                int cnt) {
            // gc.setFill(
            // Color.rgb(255 * cnt / maxcnt, 255 * (1 - cnt / maxcnt), 0));
            gc.setFill(Color.grayRgb(255 - (230 * cnt) / maxcnt));
            gc.fillRect(lon, lat, 0.1, 0.1);
        }
    }

}
