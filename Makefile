
SRC  := src
OUT  := build
DIST := dist 
JOBS := job0 c1

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
