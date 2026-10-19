package common;

import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * Entrenamiento por DESCENSO DE GRADIENTE POR LOTES COMPLETOS, una EPOCA = UN job MapReduce:
 *   Mapper : con los pesos actuales (conf "w") calcula forward + backprop de cada fila de
 *            ENTRENAMIENTO y acumula el gradiente EN MEMORIA; emite UN vector por mapper en close()
 *   Reducer: suma los vectores (common.SumsReducer)
 *   Driver : gradiente promedio -> actualizacion con momento -> siguiente epoca
 *
 * Roles de fila (por NRO_DETECCION % 5): 0 = prueba, 1 = codificacion de sitios, 2-4 = entrenamiento.
 * Antes de entrenar corre UN job que calcula las estadisticas por sitio (cluster de la C3) usando
 * solo las filas de codificacion; esas estadisticas alimentan 2 de las 7 variables de entrada.
 * Las metricas de cada epoca usan los pesos de ANTES de actualizar; despues de la ultima epoca un
 * job final (modo "eval") mide los pesos finales. momento = 0 equivale al descenso de gradiente simple.
 */
public final class MlpTrainer {

    private MlpTrainer() {
    }

    /** Lo que cambia entre clasificacion y regresion. */
    public interface Task {
        Class<? extends Mapper> mapper();

        double[] initWeights(Stats s, long seed);

        void configure(JobConf c, Stats s);

        double trainN(double[] v);

        String header();

        String row(double[] v);

        String finalReport(double[] v);
    }

    /** args: <entrada> <base> <dirJob0> <dirCeldasC3> <cellSize> [epocas=40] [lr] [momento=0.9] [semilla=42] */
    public static void run(Class<?> driver, Task t, String[] a, double lrDefecto) throws Exception {
        if (a.length < 5) {
            System.err.println("Uso: <entrada> <base> <dirJob0> <dirCeldasC3> <cellSize> [epocas=40] [lr=" + lrDefecto + "] [momento=0.9] [semilla=42]");
            System.err.println("  dirCeldasC3 = ultimo directorio de propagacion de la C3 (ej. /out/c3/prop_5); cellSize = el usado en la C3");
            System.exit(1);
        }
        String in = a[0], base = a[1], job0 = a[2], celdas = a[3], cellSize = a[4];
        int epocas = a.length > 5 ? Integer.parseInt(a[5]) : 40;
        double lr = a.length > 6 ? Double.parseDouble(a[6]) : lrDefecto;
        double mu = a.length > 7 ? Double.parseDouble(a[7]) : 0.9;
        long semilla = a.length > 8 ? Long.parseLong(a[8]) : 42L;
        String sitios = base + "/sitios";

        Configuration conf = new Configuration();
        Stats st = Stats.load(conf, job0);

        // ---- estadisticas por sitio (solo filas de codificacion) ----
        JobConf s = new JobConf(driver);
        s.setJobName("MLP_estadisticas_sitio");
        s.setOutputKeyClass(Text.class);
        s.setOutputValueClass(Text.class);
        s.setMapperClass(SiteStatsMapper.class);
        s.setReducerClass(SumsReducer.class);
        s.setCombinerClass(SumsReducer.class);
        s.setNumReduceTasks(1);
        s.setInputFormat(TextInputFormat.class);
        s.setOutputFormat(TextOutputFormat.class);
        s.set("cell.size", cellSize);
        s.set("cells.dir", celdas);
        s.set("umbral.exceso", "30");
        FileInputFormat.setInputPaths(s, new Path(in));
        FileOutputFormat.setOutputPath(s, new Path(sitios));
        long t0 = System.nanoTime();
        JobClient.runJob(s);
        double segSitios = (System.nanoTime() - t0) / 1e9;
        System.out.println(String.format(Locale.US, "# sitios con estadisticas: %d (job de sitios: %.1f s)",
                HdfsText.read(conf, sitios).size(), segSitios));

        double[] w = t.initWeights(st, semilla);
        double[] vel = new double[w.length];
        HdfsText.write(conf, base + "/pesos_iniciales.txt", Mlp.join(w) + "\n");

        System.out.println(String.format(Locale.US, "# epocas=%d lr=%s momento=%s semilla=%d variables=%d parametros=%d",
                epocas, lr, mu, semilla, Features.N, w.length));
        System.out.println(t.header());
        double tTotal = 0;
        for (int e = 1; e <= epocas; e++) {
            t0 = System.nanoTime();
            double[] v = job(driver, t, st, in, base + "/epoch_" + e, w, "train", e, cellSize, celdas, sitios);
            double seg = (System.nanoTime() - t0) / 1e9;
            tTotal += seg;
            System.out.println(String.format(Locale.US, "%d\t%.1f\t%s", e, seg, t.row(v)));
            double n = t.trainN(v);
            for (int i = 0; i < w.length; i++) {
                vel[i] = mu * vel[i] - lr * (v[i] / n);
                w[i] += vel[i];
            }
        }

        double[] v = job(driver, t, st, in, base + "/eval", w, "eval", 0, cellSize, celdas, sitios);
        HdfsText.write(conf, base + "/pesos_finales.txt", Mlp.join(w) + "\n");
        System.out.println(String.format(Locale.US, "\n==== Evaluacion FINAL (pesos tras %d epocas) ====", epocas));
        System.out.println(t.finalReport(v));
        System.out.println(String.format(Locale.US, "tiempo_entrenamiento_s=%.1f  tiempo_medio_por_epoca_s=%.1f", tTotal, tTotal / Math.max(1, epocas)));
        System.out.println("pesos finales en " + base + "/pesos_finales.txt");
    }

    private static double[] job(Class<?> driver, Task t, Stats st, String in, String out, double[] w, String modo, int e,
                                String cellSize, String celdas, String sitios) throws Exception {
        JobConf c = new JobConf(driver);
        c.setJobName("MLP_" + modo + "_" + e);
        c.setOutputKeyClass(Text.class);
        c.setOutputValueClass(Text.class);
        c.setMapperClass(t.mapper());
        c.setReducerClass(SumsReducer.class);
        c.setCombinerClass(SumsReducer.class);
        c.setNumReduceTasks(1);
        c.setInputFormat(TextInputFormat.class);
        c.setOutputFormat(TextOutputFormat.class);
        st.putInto(c);
        t.configure(c, st);
        c.set("cell.size", cellSize);
        c.set("cells.dir", celdas);
        c.set("sites.dir", sitios);
        c.set("w", Mlp.join(w));
        c.set("mode", modo);
        FileInputFormat.setInputPaths(c, new Path(in));
        FileOutputFormat.setOutputPath(c, new Path(out));
        JobClient.runJob(c); // si falla lanza excepcion: NO se atrapa
        String linea = HdfsText.read(c, out).get(0);
        return Sums.parse(linea.split("\t")[1]);
    }

    public static double[] parseW(String s) {
        return Sums.parse(s);
    }
}
