package c4;

import java.util.Locale;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;

import common.Mlp;
import common.MlpTrainer;
import common.Stats;

/**
 * C4 - MLP 7-8-1 de clasificacion (grave vs leve), descenso de gradiente por lotes, 1 job por epoca.
 * Uso: hadoop jar dist/c4.jar c4.C4Driver <entrada> <base> <dirJob0> <dirCeldasC3> <cellSize> [epocas=40] [lr=0.1] [momento=0.9] [semilla=42]
 */
public class C4Driver {

    public static void main(String[] args) throws Exception {
        MlpTrainer.run(C4Driver.class, new Clf(), args, 0.1);
    }

    static final class Clf implements MlpTrainer.Task {
        private static final int P = ClfMapper.P;

        public Class<? extends Mapper> mapper() {
            return ClfMapper.class;
        }

        public double[] initWeights(Stats s, long seed) {
            double p = s.mean("p_exceso_gt30"); // prevalencia de "grave": sesgo inicial = logit(p)
            return Mlp.init(seed, Math.log(p / (1 - p)));
        }

        public void configure(JobConf c, Stats s) {
            c.set("prior", Double.toString(s.mean("p_exceso_gt30")));
        }

        public double trainN(double[] v) {
            return v[P];
        }

        public String header() {
            return "epoca\ttiempo_s\ttrain_loss\ttrain_acc\ttest_loss\ttest_acc\ttest_precision\ttest_recall\ttest_f1\ttest_auc   (precision/recall/f1 con umbral 0.5)";
        }

        public String row(double[] v) {
            double[] m = metricas(v, P + 5);
            return String.format(Locale.US, "%.5f\t%.4f\t%.5f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f",
                    v[P + 1] / v[P], v[P + 2] / v[P], v[P + 4] / v[P + 3], m[0], m[1], m[2], m[3], auc(v));
        }

        /** {accuracy, precision, recall, f1} desde TP,FP,TN,FN en v[base..base+3]. */
        private static double[] metricas(double[] v, int base) {
            double tp = v[base], fp = v[base + 1], tn = v[base + 2], fn = v[base + 3];
            double acc = (tp + tn) / (tp + fp + tn + fn);
            double prec = tp + fp > 0 ? tp / (tp + fp) : 0;
            double rec = tp + fn > 0 ? tp / (tp + fn) : 0;
            double f1 = prec + rec > 0 ? 2 * prec * rec / (prec + rec) : 0;
            return new double[] {acc, prec, rec, f1};
        }

        /** AUC desde los histogramas de probabilidad (graves | leves); empates dentro de una caja cuentan 1/2. */
        static double auc(double[] v) {
            int h = ClfMapper.HIST, b = ClfMapper.BINS;
            double pos = 0, neg = 0;
            for (int i = 0; i < b; i++) {
                pos += v[h + i];
                neg += v[h + b + i];
            }
            double s = 0, negDebajo = 0;
            for (int i = 0; i < b; i++) {
                s += v[h + i] * (negDebajo + 0.5 * v[h + b + i]);
                negDebajo += v[h + b + i];
            }
            return s / (pos * neg);
        }

        public String finalReport(double[] v) {
            double n = v[P + 3];
            double pos = v[P + 5] + v[P + 8];      // TP + FN (reales graves en prueba)
            double base = Math.max(pos, n - pos) / n;
            double[] a = metricas(v, P + 5), b = metricas(v, P + 9);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(Locale.US, "filas_entrenamiento=%d filas_prueba=%d  graves_en_prueba=%.2f%%\n", (long) v[P], (long) n, 100 * pos / n));
            sb.append(String.format(Locale.US, "LINEA BASE (siempre la clase mayoritaria): accuracy=%.4f   <- el modelo debe superarla\n", base));
            double pi = pos / n;
            sb.append(String.format(Locale.US, "LINEA BASE F1 (marcar TODO como grave): f1=%.4f   <- referencia para F1\n", 2 * pi / (1 + pi)));
            sb.append(String.format(Locale.US, "test_loss=%.5f (linea base, solo la prevalencia: %.5f)   test_AUC=%.4f (azar = 0.5)\n",
                    v[P + 4] / n, -(pi * Math.log(pi) + (1 - pi) * Math.log(1 - pi)), auc(v)));
            sb.append(String.format(Locale.US, "umbral 0.5      : acc=%.4f precision=%.4f recall=%.4f f1=%.4f  (TP=%d FP=%d TN=%d FN=%d)\n",
                    a[0], a[1], a[2], a[3], (long) v[P + 5], (long) v[P + 6], (long) v[P + 7], (long) v[P + 8]));
            sb.append(String.format(Locale.US, "umbral=prevalencia: acc=%.4f precision=%.4f recall=%.4f f1=%.4f  (TP=%d FP=%d TN=%d FN=%d)",
                    b[0], b[1], b[2], b[3], (long) v[P + 9], (long) v[P + 10], (long) v[P + 11], (long) v[P + 12]));
            return sb.toString();
        }
    }
}
