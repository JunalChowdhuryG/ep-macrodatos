package c1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class Job3Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] kv = value.toString().split("\t", 2);
        if (kv.length != 2) {
            reporter.incrCounter("calidad", "job3_lineas_raras", 1);
            return;
        }
        int campos = kv[1].split(",").length;
        if (campos == 2) {
            output.collect(new Text(kv[0]), new Text("TOTAL:" + kv[1]));
        } else if (campos == 5) {
            String carretera = kv[0].split("\\|", 2)[0];
            output.collect(new Text(carretera), new Text("SITIO:" + kv[1]));
        } else {
            reporter.incrCounter("calidad", "job3_lineas_raras", 1);
        }
    }
}
