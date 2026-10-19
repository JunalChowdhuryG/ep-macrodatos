package common;

/** Cuadricula geografica: id de celda, vecinas y distancia aproximada en km. */
public final class Grid {

    // id = (i + OFF) * MULT + (j + OFF), con i = floor(lat/size), j = floor(lon/size).
    // OFF mantiene (j + OFF) en [0, MULT), asi sumar +-1 a j nunca "salta" de fila.
    public static final long OFF = 2000000L;
    public static final long MULT = 10000000L;
    private static final double KM_POR_GRADO = 111.2;

    private Grid() {
    }

    public static long cellId(double lat, double lon, double size) {
        long i = (long) Math.floor(lat / size);
        long j = (long) Math.floor(lon / size);
        return (i + OFF) * MULT + (j + OFF);
    }

    /** Las 8 celdas vecinas (conectividad 8). */
    public static long[] neighbors(long id) {
        long[] r = new long[8];
        int k = 0;
        for (int di = -1; di <= 1; di++) {
            for (int dj = -1; dj <= 1; dj++) {
                if (di != 0 || dj != 0) {
                    r[k++] = id + di * MULT + dj;
                }
            }
        }
        return r;
    }

    /** Distancia equirectangular en km (suficiente a escala de decenas de km). */
    public static double distKm(double lat1, double lon1, double lat2, double lon2) {
        double dx = (lon2 - lon1) * KM_POR_GRADO * Math.cos(Math.toRadians((lat1 + lat2) / 2.0));
        double dy = (lat2 - lat1) * KM_POR_GRADO;
        return Math.sqrt(dx * dx + dy * dy);
    }
}
