package job0;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * Uso: hadoop jar dist/job0.jar job0.Job0Driver <entrada> <salida>
 *
 * A diferencia del lab anterior, main declara "throws Exception" y NO atrapa la
 * excepcion: si el job falla, el proceso termina con error visible. (El try/catch
 * que tragaba el error fue lo que dejo un job encadenado "corriendo" sin correr.)
 */
public class Job0Driver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: Job0Driver <entrada> <salida>");
            System.exit(1);
        }

        JobConf conf = new JobConf(Job0Driver.class);
        conf.setJobName("Job0_Estadisticas");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Job0Mapper.class);
        conf.setCombinerClass(Job0Combiner.class);
        conf.setReducerClass(Job0Reducer.class);
        conf.setNumReduceTasks(1);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        long t0 = System.nanoTime();
        RunningJob job = JobClient.runJob(conf);
        double seg = (System.nanoTime() - t0) / 1e9;

        Counters c = job.getCounters();
        System.out.println("==== Resumen Job 0 ====");
        System.out.println("filas_validas      = " + c.getGroup("calidad").getCounter("filas_validas"));
        System.out.println("filas_invalidas    = " + c.getGroup("calidad").getCounter("filas_invalidas") + "  (esperado: 1, el encabezado)");
        System.out.println("exceso_no_positivo = " + c.getGroup("calidad").getCounter("exceso_no_positivo"));
        System.out.println(String.format(java.util.Locale.US, "tiempo_pared_s     = %.1f", seg));
    }
}
