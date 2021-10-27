package hadoop.AirlineData;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 맵 작업의 출력 키
    private Text outputKey = new Text();
    // 맵 작업의 출력 값 -> 항상 1값 (카운트)
    private final static IntWritable outputValue = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

        // 출력키는 년도,월 형식
        outputKey.set(parser.getYear() + "," + parser.getMonth());
        if(parser.getDepartureDelayTime() > 0){
            context.write(outputKey, outputValue);
        }
    }
}
