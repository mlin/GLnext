# Copilot Instructions for GLnext

## Overview

GLnext is a scalable tool for gVCF merging and joint variant calling using Apache Spark. It's written in Kotlin and targets Java 11 for compatibility with Spark 3.2+.

## Critical Requirements

- **Java 11** (NOT Java 17+ - breaks Spark compatibility)
- **Apache Spark 3.2.2+** (download and set SPARK_HOME)
- **Maven** for builds
- **bcftools and tabix** for genomic data processing
- **bash-tap** testing framework (submodule)

## Repository Structure

- `src/` - Kotlin source code
- `test/` - Integration tests using bash-tap framework
- `pom.xml` - Maven build configuration
- `.github/workflows/build.yml` - CI pipeline
- `dx/GLnext/` - DNAnexus app

## Build Commands & Timeouts

⚠️ **NEVER CANCEL THESE COMMANDS** - Genomic builds take time!

### Linting (timeout: 2 minutes)
```bash
mvn antrun:run@ktlint
mvn antrun:run@ktlint-format
```

### Unit Tests (timeout: 10 minutes)
```bash
mvn test
```

### Build Package (timeout: 5 minutes)
```bash
mvn package -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=warn
```

### Clean Build (timeout: 10 minutes)
```bash
mvn clean package
```

### Integration Tests (timeout: 15 minutes)
```bash
# Set up Spark environment first
export SPARK_HOME=/path/to/spark-3.2.2-bin-hadoop3.2

# 5% test dataset (~32 seconds)
DV1KGP_5PCT=1 prove -v test/dv1KGP.t

# Full test dataset (~49 seconds)  
prove -v test/dv1KGP.t
```

## Environment Setup

### Required Dependencies
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y bcftools tabix aria2

# Download Spark (required for tests)
aria2c -x 10 -s 10 https://mirrors.huaweicloud.com/apache/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz
tar zxf spark-*.tgz
export SPARK_HOME=$(find $(pwd) -type d -name "spark-*")
```

### Git Submodules
```bash
git submodule update --init --recursive
```

## Testing Strategy

### Manual Validation After Changes
1. **Always run linting first**: `mvn antrun:run@ktlint`
2. **Run unit tests**: `mvn test` (takes ~4 minutes)
3. **Build package**: `mvn package` (takes ~18 seconds)
4. **Run integration tests**: `prove -v test/dv1KGP.t` (takes ~49 seconds full, ~32 seconds with DV1KGP_5PCT=1)

### Integration Test Details
- Downloads real genomic data (dv1KGP ALDH2 dataset)
- Tests full VCF processing pipeline
- Validates Spark integration
- Requires bcftools/tabix for VCF manipulation
- Uses bash-tap testing framework

## Working with Genomic Data

### VCF Processing Pipeline
1. Input: gVCF files (genomic variant call format)
2. Processing: Joint calling with Apache Spark
3. Output: spVCF files (simplified population VCF)
4. Decoding: Convert spVCF back to standard VCF

### Key Concepts
- **gVCF**: Genomic VCF with reference bands
- **spVCF**: Simplified population VCF (one ALT allele per line)
- **Joint calling**: Multi-sample variant analysis
- **Reference bands**: Non-variant regions in gVCF

## Common Issues & Troubleshooting

### Java Version Issues
- **Problem**: Build fails with Java 17+
- **Solution**: Use Java 11 (`export JAVA_HOME=/path/to/java11`)

### Spark Compatibility
- **Problem**: Runtime errors with newer Spark versions
- **Solution**: Use Spark 3.2.2 specifically

### Submodule Issues
- **Problem**: bash-tap submodule missing
- **Solution**: `git submodule update --init --recursive`

### Missing Tools
- **Problem**: Integration tests fail
- **Solution**: Install bcftools and tabix

### Out of Memory
- **Problem**: Tests fail with OOM
- **Solution**: Increase `_JAVA_OPTIONS` heap size and partitioning

## Development Workflow

1. **Before making changes**: Ensure clean build passes
2. **During development**: Run relevant unit tests frequently  
3. **Before committing**: Run full test suite including integration tests
4. **Code style**: Use ktlint formatting (`mvn antrun:run@ktlint-format`)

## Performance Notes

- Unit tests: ~4 minutes (parallel execution)
- Clean build: ~18 seconds (optimized shade plugin)
- Integration tests: ~32-49 seconds (depending on dataset)
- Linting: ~7 seconds (ktlint)

## Dependencies Summary

- Kotlin 1.8.10 (JVM target 11)
- Apache Spark 3.2.0 
- HTSJDK 3.0.5 (genomics)
- kotlinx-spark-api 1.2.3
- Native libraries for x86-64 only