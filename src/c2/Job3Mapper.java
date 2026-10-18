package c2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/** C2 - Job 3 (mapper): lee la salida del Job 2 y deja pasar solo los meses con variacion calculable. */
public class Job3Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] kv = value.toString().split("\t", 2);
        if (kv.length != 2 || kv[1].split(",").length != 5) {
            reporter.incrCounter("calidad", "job3_lineas_raras", 1);
            return;
        }
        if (kv[1].split(",")[4].equals("NA")) {
            return; // primer mes o mes tras un hueco: no hay variacion
        }
        output.collect(new Text(kv[0]), new Text(kv[1]));
    }
}
