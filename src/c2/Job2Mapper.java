package c2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * C2 - Job 2 (mapper): lee la salida del Job 1 ("carretera|yyyy-MM TAB n,sumExc") y
 * re-agrupa por CARRETERA, para que el Reducer vea todos sus meses juntos.
 * Valor emitido: "yyyy-MM,n,sumExc"
 */
public class Job2Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] kv = value.toString().split("\t", 2);
        String[] cm = kv.length == 2 ? kv[0].split("\\|", 2) : new String[0];
        if (cm.length != 2) {
            reporter.incrCounter("calidad", "job2_lineas_raras", 1);
            return;
        }
        output.collect(new Text(cm[0]), new Text(cm[1] + "," + kv[1]));
    }
}
