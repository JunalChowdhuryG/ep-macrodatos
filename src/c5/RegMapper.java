package c5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;
import common.Features;
import common.Mlp;
import common.MlpTrainer;
import common.Sums;

/**
 * C5 - MLP de REGRESION: predice el exceso (km/h), objetivo estandarizado (z-score del Job 0).
 * Se minimiza 0.5*e^2 (delta de salida = e). Vector por mapper, emitido en close():
 *  [0..56] gradiente (solo entrenamiento, modo train) | 57 trN | 58 trSSE(z)
 *  59 teN | 60 teSSE(z) | 61 teSumAbs(km/h) | 62 teSumSq(km/h) | 63 teSumY | 64 teSumY2
 */
public class RegMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    static final int P = Mlp.N_PARAMS;
    static final int LEN = P + 8;

    private double[] w;
    private Features f;
    private boolean entrenar;
    private double mY, sY;
    private final double[] v = new double[LEN];
    private final double[] a = new double[Mlp.H];
    private OutputCollector<Text, Text> out;

    @Override
    public void configure(JobConf job) {
        w = MlpTrainer.parseW(job.get("w"));
        f = new Features(job);
        entrenar = "train".equals(job.get("mode"));
        mY = Double.parseDouble(job.get("st.exceso.mean"));
        sY = Double.parseDouble(job.get("st.exceso.std"));
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null) {
            reporter.incrCounter("calidad", "filas_invalidas", 1);
            return;
        }
        if (d.esCodificacion()) {
            reporter.incrCounter("calidad", "filas_codificacion", 1);
            return;
        }
        out = output;
        double[] x = f.of(d);
        double yz = (d.exceso - mY) / sY;
        double e = Mlp.forward(w, x, a) - yz;

        if (d.esPrueba()) {
            double ekmh = e * sY;
            v[P + 2] += 1;
            v[P + 3] += e * e;
            v[P + 4] += Math.abs(ekmh);
            v[P + 5] += ekmh * ekmh;
            v[P + 6] += d.exceso;
            v[P + 7] += (double) d.exceso * d.exceso;
        } else {
            v[P] += 1;
            v[P + 1] += e * e;
            if (entrenar) {
                Mlp.backward(w, x, a, e, v);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            out.collect(new Text("all"), Sums.of(v));
        }
    }
}
