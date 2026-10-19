package c3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/** C3 - Job E (mapper): manda todos los clusters a UNA llave para calcular las metricas globales. */
public class EvalMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] kv = value.toString().split("\t", 2);
        if (kv.length == 2) {
            output.collect(new Text("all"), new Text(kv[0] + "," + kv[1])); // label,n,sumA,sumS
        }
    }
}
