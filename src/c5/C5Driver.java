package c5;

import java.util.Locale;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;

import common.Mlp;
import common.MlpTrainer;
import common.Stats;

/**
 * C5 - MLP 7-8-1 de regresion (exceso en km/h), descenso de gradiente por lotes, 1 job por epoca.
 * Uso: hadoop jar dist/c5.jar c5.C5Driver <entrada> <base> <dirJob0> <dirCeldasC3> <cellSize> [epocas=40] [lr=0.05] [momento=0.9] [semilla=42]
 */
public class C5Driver {

    public static void main(String[] args) throws Exception {
        MlpTrainer.run(C5Driver.class, new Reg(), args, 0.05);
    }

    static final class Reg implements MlpTrainer.Task {
        private static final int P = RegMapper.P;

        public Class<? extends Mapper> mapper() {
            return RegMapper.class;
        }

        public double[] initWeights(Stats s, long seed) {
            return Mlp.init(seed, 0.0); // objetivo estandarizado: media 0
        }

        public void configure(JobConf c, Stats s) {
        }

        public double trainN(double[] v) {
            return v[P];
        }

        public String header() {
            return "epoca\ttiempo_s\ttrain_mse_z\ttest_mse_z\ttest_rmse_kmh\ttest_mae_kmh\ttest_r2";
        }

        /** {mse_z, rmse_kmh, mae_kmh, r2, rmse_base_kmh} */
        private static double[] m(double[] v) {
            double n = v[P + 2];
            double mse = v[P + 3] / n;
            double rmse = Math.sqrt(v[P + 5] / n);
            double mae = v[P + 4] / n;
            double media = v[P + 6] / n;
            double sst = v[P + 7] / n - media * media; // varianza de y en prueba = MSE de predecir la media
            return new double[] {mse, rmse, mae, 1.0 - (v[P + 5] / n) / sst, Math.sqrt(sst)};
        }

        public String row(double[] v) {
            double[] r = m(v);
            return String.format(Locale.US, "%.5f\t%.5f\t%.3f\t%.3f\t%.4f", v[P + 1] / v[P], r[0], r[1], r[2], r[3]);
        }

        public String finalReport(double[] v) {
            double[] r = m(v);
            return String.format(Locale.US,
                    "filas_entrenamiento=%d filas_prueba=%d\n"
                            + "LINEA BASE (predecir siempre la media): RMSE=%.3f km/h   <- el modelo debe superarla\n"
                            + "test_mse_z=%.5f  test_rmse=%.3f km/h  test_mae=%.3f km/h  test_r2=%.4f",
                    (long) v[P], (long) v[P + 2], r[4], r[0], r[1], r[2], r[3]);
        }
    }
}
