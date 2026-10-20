MEDICION CLUSTER (2 workers) vs 1 WORKER
Kit probado en local con trabajos falsos de duracion y carga conocidas (tiempos, speedup,
CPU y memoria salen correctos; warmup y corridas con error se excluyen). NO probado todavia en
tu cluster: la primera corrida real es la prueba de verdad.
=====================================================================
Definiciones:  speedup = T(1 worker) / T(2 workers)      eficiencia = speedup / 2
"1 worker" = un worker activo + el master solo coordinando (dilo asi en el informe).

0. REQUISITOS (desde la raiz del proyecto, en el MASTER, con el cluster arriba y los 2 workers)
   - dist/*.jar compilados;  /sutran/detecciones.csv subido con bloques de 4 MB
   - existen /out/job0  y  /out/c3/prop_5   (los usan C4 y C5)
   - misma memoria de YARN en ambos workers; NO la cambies entre modos

1. DESACTIVAR LA EJECUCION ESPECULATIVA (solo en el master, se lee al enviar el job; sin reinicio)
   en ~/hadoop/etc/hadoop/mapred-site.xml agrega dentro de <configuration>:
     <property><name>mapreduce.map.speculative</name><value>false</value></property>
     <property><name>mapreduce.reduce.speculative</name><value>false</value></property>
   (evita tareas duplicadas que meten ruido en los tiempos)

2. MODO 2 WORKERS   (~40 min con 3 repeticiones; usa 2 si vas justo)
   yarn node -list                                   # deben verse 2 NodeManager RUNNING
   bash medir/medir.sh cluster2 medir/trabajos.txt 3
   -> resultados/cluster2/preflight.txt  (estado del cluster + fsck de bloques/replicas)

   CAPTURAS PARA EL INFORME (mientras corre c3 o c4, que son los mas largos):
   - htop en cada nodo (3 terminales SSH: master, worker1, worker2): CPU por nucleo y memoria
   - YARN UI    http://192.168.56.10:8088   (pestana Nodes + lista de aplicaciones con sus tiempos)
   - NameNode UI http://192.168.56.10:9870  (pestana Datanodes: los 2 vivos)
   - la salida de "yarn node -list" con 2 nodos
   (la IP 192.168.56.10 es la red solo-anfitrion de tu guia: se abre desde el navegador de tu PC)

3. PASAR A 1 WORKER
   Antes: hdfs fsck /sutran/detecciones.csv -files -blocks | grep -i "replic"   # cada bloque con 2 replicas
   En worker2:   yarn --daemon stop nodemanager
                 hdfs --daemon stop datanode
   En el master, ESPERA hasta que lo den por muerto (por defecto ~10 min) y verifica:
                 yarn node -list -all                         # worker2 ya no figura RUNNING
                 hdfs dfsadmin -report | grep -E "Live|Dead"  # 1 datanode vivo
   Captura de "yarn node -list" con 1 nodo y de la UI de YARN.
   bash: NODES="master worker1" bash medir/medir.sh worker1 medir/trabajos.txt 3

4. RESTAURAR  (en worker2:  yarn --daemon start nodemanager ; hdfs --daemon start datanode)
   Limpieza opcional:  hdfs dfs -rm -r /bench

5. RESUMEN
   python3 medir/resumen.py --base worker1 --par cluster2 --ratio 2
   -> tabla en pantalla + resumen.csv (tiempo medio, desviacion, min, max, CPU% y memoria MB por nodo)
      + resumen_por_corrida.csv.   resumen.csv se puede cargar en Power BI.

ANTES DE CONFIAR EN LOS NUMEROS
   - todas las corridas con rc=0 (las fallidas se excluyen y se avisa)
   - preflight de cada modo: nodos esperados vivos
   - desviacion pequena entre repeticiones (si es grande, repite o sube las repeticiones)
   - c4 y c5 se miden con 5 epocas: reporta el tiempo POR EPOCA (tiempo / 7 jobs ~ 5 epocas + sitios + eval)

POR QUE EL SPEEDUP PUEDE SER BAJO (explicalo con TUS numeros, no con estos supuestos)
   - cada job paga ~25 s de arranque fijo (medido: 22-28 s hasta con unas pocas lineas): no se paraleliza
   - 160 mil filas = ~13 MB = 4 mappers: poco trabajo repartible; varios jobs usan 1 reducer (parte serie)
   - memoria de 2 GB por VM: el ApplicationMaster ocupa un contenedor y limita cuantos mappers corren a la vez
   - los jobs iterativos (c3, c4, c5) multiplican ese costo fijo por el numero de iteraciones/epocas
   - Ley de Amdahl: speedup maximo = 1 / (f_serie + (1 - f_serie)/2)
   - opcional: repetir un job con el CSV concatenado 5-10 veces para mostrar como crece el speedup con el tamano
     (solo benchmark de tiempos; las metricas de ML usan el dataset original)
