#!/bin/bash
# Trae de HDFS a ./salidas/ lo que necesita exportar_csv.py. Ejecutar en el MASTER, desde la raiz del repo.
# Uso:  bash powerbi/traer_salidas.sh [modo=cluster2]
# Usa las salidas de la primera repeticion (r1) que dejo medir.sh en /bench. NO borres /bench antes.
set -u
MODO="${1:-cluster2}"; B="/bench/${MODO}"
mkdir -p salidas
hdfs dfs -cat /out/job0/part-00000                > salidas/job0.tsv
hdfs dfs -cat "${B}_c1_r1/job3/part-00000"        > salidas/c1_job3.tsv
hdfs dfs -cat "${B}_c2_r1/job2/part-00000"        > salidas/c2_job2.tsv
hdfs dfs -cat "${B}_c2_r1/job3/part-00000"        > salidas/c2_job3.tsv
hdfs dfs -cat "${B}_c3_r1/metricas/part-00000"    > salidas/c3_metricas.tsv
hdfs dfs -getmerge "${B}_c3_r1/puntos"              salidas/c3_puntos.tsv
ls -la salidas
echo "Si tienes la salida del job 'techo', guardala en salidas/techo_consola.txt (opcional)."
