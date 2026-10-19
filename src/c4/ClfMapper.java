package c4;

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
 * C4 - MLP de CLASIFICACION: severidad grave (exceso > umbral, 30 km/h por defecto).
 * Salida del mapper (UN vector por mapper, emitido en close()):
 *  [0..56] gradiente de la entropia cruzada (solo filas de entrenamiento, modo train)
 *  57 trN | 58 trPerdida | 59 trAciertos
 *  60 teN | 61 tePerdida | 62-65 TP,FP,TN,FN (umbral 0.5) | 66-69 TP,FP,TN,FN (umbral = prevalencia)
 *  70.. histograma de probabilidades de prueba (BINS cajas) de graves, luego de leves -> AUC sin umbral
 */
public class ClfMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    static final int P = Mlp.N_PARAMS;
    static final int BINS = 1000;
    static final int HIST = P + 13;
    static final int LEN = HIST + 2 * BINS;

    private double[] w;
    private Features f;
    private boolean entrenar;
    private double prior;
    private double umbralExceso;
    private final double[] v = new double[LEN];
    private final double[] a = new double[Mlp.H];
    private OutputCollector<Text, Text> out;

    @Override
    public void configure(JobConf job) {
        w = MlpTrainer.parseW(job.get("w"));
        f = new Features(job);
        entrenar = "train".equals(job.get("mode"));
        prior = Double.parseDouble(job.get("prior"));
        umbralExceso = Double.parseDouble(job.get("umbral.exceso", "30"));
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null) {
            reporter.incrCounter("calidad", "filas_invalidas", 1);
            return;
        }
        if (d.esCodificacion()) {
            reporter.incrCounter("calidad", "filas_codificacion", 1);
            return; // solo sirven para las estadisticas por sitio
        }
        out = output;
        double[] x = f.of(d);
        double z = Mlp.forward(w, x, a);
        double p = 1.0 / (1.0 + Math.exp(-z));
        double y = d.exceso > umbralExceso ? 1.0 : 0.0;
        double pc = Math.min(Math.max(p, 1e-12), 1 - 1e-12);
        double perdida = -(y * Math.log(pc) + (1 - y) * Math.log(1 - pc));

        if (d.esPrueba()) {
            v[P + 3] += 1;
            v[P + 4] += perdida;
            conf(P + 5, p >= 0.5, y == 1.0);
            conf(P + 9, p >= prior, y == 1.0);
            v[HIST + (y == 1.0 ? 0 : BINS) + Math.min(BINS - 1, (int) (p * BINS))] += 1;
        } else {
            v[P] += 1;
            v[P + 1] += perdida;
            if ((p >= 0.5) == (y == 1.0)) {
                v[P + 2] += 1;
            }
            if (entrenar) {
                Mlp.backward(w, x, a, p - y, v); // el gradiente ocupa v[0..P)
            }
        }
    }

    /** Suma en v[base..base+3] = TP, FP, TN, FN. */
    private void conf(int base, boolean pred, boolean real) {
        v[base + (pred ? (real ? 0 : 1) : (real ? 3 : 2))] += 1;
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            out.collect(new Text("all"), Sums.of(v));
        }
    }
}
