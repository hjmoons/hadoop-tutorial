package hadoop.AirlineData;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
    private int year;
    private int month;

    private int arrivalDelayTime = 0;
    private int departureDelayTime = 0;
    private int distance = 0;

    private boolean arrivalDelayAvailable = true;
    private boolean departureDelayAvailable = true;
    private boolean distanceAvailable = true;

    private String uniqueCarrier;

    public AirlinePerformanceParser(Text text){

        try {
            // 한 row를 comma로 split 한다
            String[] columns = text.toString().split(",");
            year = Integer.parseInt(columns[0]); // 운항 년도
            month = Integer.parseInt(columns[1]); // 운항 월
            uniqueCarrier = columns[8]; // 항공사 코드
            if (!columns[15].equals("NA")) // 항공기 출발 지연 시간
                departureDelayTime = Integer.parseInt(columns[15]);
            else
                departureDelayAvailable = false;

            if (!columns[14].equals("NA")) // 항공기 도착 지연 시간
                arrivalDelayTime = Integer.parseInt(columns[14]);
            else
                arrivalDelayAvailable = false;

            if (!columns[18].equals("NA")) // 운항 거리
                distance = Integer.parseInt(columns[18]);
            else
                distanceAvailable = false;
        } catch (Exception ex){
            System.out.println("Error parsing a record: " + ex.getMessage());
        }
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getArrivalDelayTime() {
        return arrivalDelayTime;
    }

    public int getDepartureDelayTime() {
        return departureDelayTime;
    }

    public int getDistance() {
        return distance;
    }

    public boolean isArrivalDelayAvailable() {
        return arrivalDelayAvailable;
    }

    public boolean isDepartureDelayAvailable() {
        return departureDelayAvailable;
    }

    public boolean isDistanceAvailable() {
        return distanceAvailable;
    }

    public String getUniqueCarrier() {
        return uniqueCarrier;
    }
}
