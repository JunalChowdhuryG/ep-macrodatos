package common;

import java.util.Random;

/**
 * Perceptron multicapa 7-8-1 escrito a mano (sin librerias): capa oculta tanh, salida lineal
 * (la sigmoide o la identidad se aplica fuera, segun clasificacion o regresion).
 * Parametros en UN vector w:  W1[h*IN+i] (8xIN) | b1[h] (8) | W2[h] (8) | b2 (1)  = 8*IN+17.
 */
public final class Mlp {

    public static final int IN = Features.N;
    public static final int H = 8;
    public static final int N_PARAMS = H * IN + H + H + 1;
    private static final int B1 = H * IN;
    private static final int W2 = B1 + H;
    private static final int B2 = W2 + H;

    private Mlp() {
    }

    /** Glorot uniforme con semilla fija (reproducible). El sesgo de salida lo fija el llamador. */
    public static double[] init(long seed, double sesgoSalida) {
        Random r = new Random(seed);
        double[] w = new double[N_PARAMS];
        double l1 = Math.sqrt(6.0 / (IN + H));
        double l2 = Math.sqrt(6.0 / (H + 1));
        for (int i = 0; i < B1; i++) {
            w[i] = (r.nextDouble() * 2 - 1) * l1;
        }
        for (int h = 0; h < H; h++) {
            w[W2 + h] = (r.nextDouble() * 2 - 1) * l2;
        }
        w[B2] = sesgoSalida;
        return w;
    }

    /** Propagacion hacia adelante. Deja las activaciones ocultas en a[] y devuelve z2 (pre-activacion de salida). */
    public static double forward(double[] w, double[] x, double[] a) {
        double z2 = w[B2];
        for (int h = 0; h < H; h++) {
            double z = w[B1 + h];
            for (int i = 0; i < IN; i++) {
                z += w[h * IN + i] * x[i];
            }
            a[h] = Math.tanh(z);
            z2 += w[W2 + h] * a[h];
        }
        return z2;
    }

    /**
     * Retropropagacion de UNA fila: delta2 = dL/dz2 (p - y en clasificacion con entropia
     * cruzada; e en regresion con 0.5*e^2). SUMA el gradiente en grad[0..N_PARAMS).
     */
    public static void backward(double[] w, double[] x, double[] a, double delta2, double[] grad) {
        grad[B2] += delta2;
        for (int h = 0; h < H; h++) {
            grad[W2 + h] += delta2 * a[h];
            double delta1 = delta2 * w[W2 + h] * (1.0 - a[h] * a[h]); // derivada de tanh
            grad[B1 + h] += delta1;
            for (int i = 0; i < IN; i++) {
                grad[h * IN + i] += delta1 * x[i];
            }
        }
    }

    public static String join(double[] w) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < w.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(w[i]);
        }
        return sb.toString();
    }
}
