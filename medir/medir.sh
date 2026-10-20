#!/bin/bash
# Uso (desde la raiz del proyecto, en el MASTER):
#   bash medir/medir.sh <modo> <archivo_trabajos> [repeticiones=3]
# Ejemplos:  bash medir/medir.sh cluster2 medir/trabajos.txt 3
#            NODES="master worker1" bash medir/medir.sh worker1 medir/trabajos.txt 3
# Para cada trabajo y repeticion: limpia la salida, arranca un muestreador de CPU/memoria en cada
# nodo, mide el tiempo de pared del comando, detiene los muestreadores y trae sus logs.
set -u
MODO="${1:?falta <modo>}"; FILE="${2:?falta <archivo_trabajos>}"; REPS="${3:-3}"
NODES="${NODES:-master worker1 worker2}"
CLEAN="${CLEAN_CMD:-hdfs dfs -rm -r -f}"
OUTBASE="${OUTBASE:-/bench}"
HERE="$(cd "$(dirname "$0")" && pwd)"
RES="resultados/$MODO"
mkdir -p "$RES"

remote() { local n="$1"; shift; if [ "$n" = localhost ]; then bash -c "$*"; else ssh -n -o BatchMode=yes -o ConnectTimeout=5 "$n" "$*"; fi; }
instalar() { local n="$1"; if [ "$n" = localhost ]; then cp "$HERE/sampler.sh" /tmp/bench_sampler.sh; else ssh -o BatchMode=yes "$n" 'cat > /tmp/bench_sampler.sh' < "$HERE/sampler.sh"; fi; }

# ---- preflight: estado del cluster y bloques del dataset (queda como evidencia para el informe) ----
{ echo "# $(date)   modo=$MODO   nodos_muestreados=$NODES"
  command -v yarn >/dev/null && { echo "## yarn node -list"; yarn node -list 2>&1; }
  command -v hdfs >/dev/null && { echo "## hdfs dfsadmin -report"; hdfs dfsadmin -report 2>&1 | grep -E "Live datanodes|Dead datanodes|^Name:|^Hostname:"
                                  echo "## hdfs fsck"; hdfs fsck /sutran/detecciones.csv -files -blocks -locations 2>&1 | tail -30; }
} > "$RES/preflight.txt" 2>&1
echo "preflight -> $RES/preflight.txt  (revisa que yarn vea los NodeManager esperados)"

for n in $NODES; do instalar "$n" || { echo "ERROR: no se pudo instalar el muestreador en $n"; exit 1; }; done
[ -f "$RES/tiempos.csv" ] || echo "job,modo,rep,t0,t1,tiempo_s,rc" > "$RES/tiempos.csv"

declare -A PID
while IFS='|' read -r -u 3 JOB NREP CMD; do
  case "$JOB" in ''|\#*) continue;; esac
  N="${NREP:-$REPS}"
  for REP in $(seq 1 "$N"); do
    RID="${JOB}_r${REP}"
    OUT="$OUTBASE/${MODO}_${RID}"
    $CLEAN "$OUT" >/dev/null 2>&1
    for n in $NODES; do
      PID[$n]=$(remote "$n" "rm -f /tmp/bench_$RID.log; nohup bash /tmp/bench_sampler.sh /tmp/bench_$RID.log >/dev/null 2>&1 & echo \$!")
    done
    sleep 2                                   # unas muestras en reposo antes del trabajo
    T0=$(date +%s.%N)
    bash -c "${CMD//\{OUT\}/$OUT}" > "$RES/$RID.consola.txt" 2>&1
    RC=$?
    T1=$(date +%s.%N)
    sleep 2
    for n in $NODES; do
      remote "$n" "kill ${PID[$n]} 2>/dev/null; cat /tmp/bench_$RID.log; rm -f /tmp/bench_$RID.log" > "$RES/$RID.$n.log"
    done
    SEG=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", b-a}')
    echo "$JOB,$MODO,$REP,$T0,$T1,$SEG,$RC" >> "$RES/tiempos.csv"
    [ "$RC" -ne 0 ] && echo "AVISO: $RID termino con codigo $RC (ver $RES/$RID.consola.txt); se excluira del resumen"
    echo "[$MODO] $RID: ${SEG}s rc=$RC"
  done
done 3< "$FILE"
echo "Listo. Siguiente: python3 medir/resumen.py"
