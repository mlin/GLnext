all:
	# build the core applet
	$(MAKE) -C vcfGLuer/
	dx build -f vcfGLuer/
	# generate dxCompiler wrapper for the just-built applet
	cd workflow && java -jar $(DXCOMPILER_JAR) dxni -path "$$(dx pwd)/vcfGLuer" --output extern.wdl -force
	# build high-level workflow
	cd workflow && java -jar $(DXCOMPILER_JAR) compile vcfGLuerRangeScatter.wdl -destination "$$(dx pwd)" -force

.PHONY: all
