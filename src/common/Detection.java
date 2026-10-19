package common;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser UNICO del CSV de detecciones de cinemometros (SUTRAN).
 *
 * Todas las consultas leen las filas a traves de esta clase, asi la validacion
 * vive en un solo lugar (en el laboratorio anterior estaba copiada en cada Mapper).
 *
 * Columnas esperadas (9):
 * NRO_DETECCION, FECHA_PAPELETA, REGION, CARRETERA, LATITUD, LONGITUD,
 * LIMITE_VELOCIDAD, VELOCIDAD_DETECTADA, FECHA_CORTE
 */
public final class Detection {

    /** Separador del CSV. Cambiar AQUI si el archivo usa ';' o tabulador. */
    public static final char SEP = ';';
    public static final int N_CAMPOS = 9;

    // Limites amplios del Peru para descartar coordenadas basura (0,0, etc.)
    private static final double LAT_MIN = -19.0, LAT_MAX = 0.0;
    private static final double LON_MIN = -82.0, LON_MAX = -68.0;
    private static final DateTimeFormatter FECHA = DateTimeFormatter.BASIC_ISO_DATE; // yyyyMMdd

    public final long id;
    public final String region;
    public final String carretera;
    public final double lat;
    public final double lon;
    public final int limite;
    public final int velocidad;
    public final int anio;
    public final int mes;
    public final int diaSemana;     // 1 = lunes ... 7 = domingo
    public final int exceso;        // velocidad - limite, en km/h
    public final double pctExceso;  // exceso / limite (campo decimal derivado)

    private Detection(long id, String region, String carretera, double lat, double lon,
                      int limite, int velocidad, LocalDate fecha) {
        this.id = id;
        this.region = region;
        this.carretera = carretera;
        this.lat = lat;
        this.lon = lon;
        this.limite = limite;
        this.velocidad = velocidad;
        this.anio = fecha.getYear();
        this.mes = fecha.getMonthValue();
        this.diaSemana = fecha.getDayOfWeek().getValue();
        this.exceso = velocidad - limite;
        this.pctExceso = (double) (velocidad - limite) / limite;
    }

    /**
     * Devuelve la deteccion, o null si la fila es invalida (encabezado, numero
     * de columnas distinto de 9, numeros o fecha ilegibles, coordenadas fuera del Peru).
     * El llamador debe CONTAR las filas descartadas (contador de Hadoop), no ignorarlas.
     */
    public static Detection parse(String linea) {
        if (linea == null) {
            return null;
        }
        List<String> f = split(linea);
        if (f.size() != N_CAMPOS) {
            return null;
        }
        try {
            long id = Long.parseLong(f.get(0).trim());
            LocalDate fecha = LocalDate.parse(f.get(1).trim(), FECHA);
            String region = f.get(2).trim();
            String carretera = f.get(3).trim();
            double lat = Double.parseDouble(f.get(4).trim());
            double lon = Double.parseDouble(f.get(5).trim());
            int limite = Integer.parseInt(f.get(6).trim());
            int velocidad = Integer.parseInt(f.get(7).trim());

            if (lat < LAT_MIN || lat > LAT_MAX || lon < LON_MIN || lon > LON_MAX) {
                return null;
            }
            if (limite <= 0 || velocidad <= 0) {
                return null;
            }
            return new Detection(id, region, carretera, lat, lon, limite, velocidad, fecha);
        } catch (RuntimeException e) { // NumberFormatException, DateTimeParseException
            return null;
        }
    }

    /** Particion fija y reproducible: id%5==0 prueba (~20%), id%5==1 codificacion de sitios (~20%), resto entrenamiento (~60%). */
    public boolean esPrueba() {
        return id % 5 == 0;
    }

    /** Filas reservadas SOLO para calcular las estadisticas por sitio (no entrenan ni prueban): evita fuga de etiqueta. */
    public boolean esCodificacion() {
        return id % 5 == 1;
    }

    /** Sitio aproximado de un cinemometro: coordenadas redondeadas a 3 decimales (~110 m). */
    public String sitio() {
        return String.format(java.util.Locale.US, "%.3f|%.3f", lat, lon);
    }

    /** Separa respetando comillas (hay carreteras como "Via Penetracion, PE-36"). */
    static List<String> split(String linea) {
        List<String> out = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        boolean enComillas = false;
        for (int i = 0; i < linea.length(); i++) {
            char c = linea.charAt(i);
            if (c == '"') {
                if (enComillas && i + 1 < linea.length() && linea.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++;
                } else {
                    enComillas = !enComillas;
                }
            } else if (c == SEP && !enComillas) {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out;
    }
}
