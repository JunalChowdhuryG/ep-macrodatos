package c2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;
import common.Sums;

/** C2 - Job 1: (carretera, mes) -> [n, sumExceso]. Reducer/Combiner: common.SumsReducer. */
public class Job1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null) {
            reporter.incrCounter("calidad", "filas_invalidas", 1);
            return;
        }
        String mes = String.format("%04d-%02d", d.anio, d.mes);
        output.collect(new Text(d.carretera + "|" + mes), Sums.of(1, d.exceso));
    }
}
