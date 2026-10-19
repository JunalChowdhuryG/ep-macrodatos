# Compila todo una vez y arma UN jar por job/consulta.
# Cada jar lleva el paquete 'common' (parser) + su propio paquete.
#
#   make            -> compila y genera dist/*.jar
#   make clean
SRC  := src
OUT  := build
DIST := dist
# Un nombre por carpeta src/<nombre>/ que tenga su Driver (se ira ampliando)
JOBS := job0 c1 c2 c3 c4 c5

.PHONY: all clean
all:
	mkdir -p $(OUT) $(DIST)
	javac -source 1.8 -target 1.8 -classpath "lib/*" -d $(OUT) $(shell find $(SRC) -name '*.java')
	@for j in $(JOBS); do \
		echo "==> $(DIST)/$$j.jar"; \
		jar -cf $(DIST)/$$j.jar -C $(OUT) common -C $(OUT) $$j; \
	done
	@ls -la $(DIST)

clean:
	rm -rf $(OUT) $(DIST)
