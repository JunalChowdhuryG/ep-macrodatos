package job0;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Stat;

/** Combiner: fusiona en el mapper, asi viajan ~9 valores por mapper y no 9 por fila. */
public class Job0Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        output.collect(key, Stat.format(Stat.merge(values)));
    }
}
