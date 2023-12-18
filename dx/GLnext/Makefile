all:
	cd ../..; rm -f target/*.jar; mvn package
	mkdir -p resources/home/dnanexus
	rm -f resources/home/dnanexus/*.jar
	cp ../../target/GLnext-*.jar resources/home/dnanexus

.PHONY: all
