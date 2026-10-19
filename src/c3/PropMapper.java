package c3;

import common.Grid;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * C3 - propagacion de etiquetas (una iteracion). Cada celda densa se envia a si misma
 * ("S:" + estado) y envia su etiqueta a sus 8 vecinas ("L:" + etiqueta).
 */
public class PropMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] kv = value.toString().split("\t", 2);
        if (kv.length != 2) {
            reporter.incrCounter("calidad", "prop_lineas_raras", 1);
            return;
        }
        long id = Long.parseLong(kv[0]);
        String label = kv[1].split(",")[0];
        output.collect(new Text(kv[0]), new Text("S:" + kv[1]));
        for (long nb : Grid.neighbors(id)) {
            output.collect(new Text(Long.toString(nb)), new Text("L:" + label));
        }
    }
}
