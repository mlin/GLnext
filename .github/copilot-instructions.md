# GLnext - Genomic Variant Calling with Apache Spark

GLnext is a scalable Apache Spark application for gVCF merging and joint variant calling in population-scale genomic sequencing. The application is written in Kotlin and produces spVCF (sparse VCF) output files.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Bootstrap and Build (NEVER CANCEL - 5-7 minutes)
Bootstrap, build, and test the repository:
```bash
# Clean build (if needed, 3 seconds)
mvn clean

# Compile only (22 seconds from clean)
mvn compile

# Full build (NEVER CANCEL: takes 5 minutes, set timeout to 7+ minutes)
mvn package -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=warn

# Unit tests (30 second timeout recommended)  
mvn test

# Linting (30 second timeout recommended)
mvn antrun:run@ktlint

# Code formatting (if needed, 6 seconds)
mvn antrun:run@ktlint-format
```

**CRITICAL BUILD TIMING**: 
- **NEVER CANCEL** the `mvn package` command - it takes 5 minutes and produces a 24MB JAR file
- Set timeouts to 7+ minutes for build commands  
- Clean build: 3 seconds, compile-only: 22 seconds, unit tests: 12 seconds, linting: 12 seconds, formatting: 6 seconds
- Build produces `target/GLnext-*.jar` (exactly 24,494,714 bytes / 24MB)
- **VALIDATED ON**: Java 17.0.16 (OpenJDK Temurin), Maven 3.9.11, Ubuntu 24.04

### Runtime Dependencies Setup
Install required external tools:
```bash
# Install genomics tools (required for tests and validation)
sudo apt-get update
sudo apt-get install -y bcftools tabix

# Download and setup Apache Spark 3.3.4 (required for runtime)
curl -LSs https://dlcdn.apache.org/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz | tar zx
export SPARK_HOME=$(pwd)/spark-3.3.4-bin-hadoop3
```

**NETWORK LIMITATIONS**: Downloads fail due to firewall restrictions (tested: dlcdn.apache.org blocked). If curl commands fail:
- Document the limitation: "Spark download fails due to network restrictions - could not resolve host"
- Look for pre-installed Spark in /opt/ or /usr/local/
- Use mock testing approaches when full integration is not possible
- Maven dependency downloads work (Maven Central accessible)

### Git Submodules (Required for Tests)
```bash
# Initialize test framework submodule
git submodule update --init --recursive
```

**KNOWN ISSUE**: SSH access to git@github.com may be blocked. If submodule init fails, document: "bash-tap test framework unavailable due to SSH restrictions".

## Validation and Testing

### Unit Tests (ALWAYS RUN)
```bash
# Run Kotlin unit tests (12 seconds, 30-second timeout)
mvn test
```
Expected output: 3 tests in 2 test classes, all passing.

### Integration Tests (Requires External Dependencies)
```bash
# Full integration test (requires SPARK_HOME and network access)
export SPARK_HOME=/path/to/spark-3.3.4-bin-hadoop3
prove -v test/dv1KGP.t

# 5% test subset (faster validation)
export DV1KGP_5PCT=1
prove -v test/dv1KGP.t
```

**INTEGRATION TEST TIMING**: 
- **NEVER CANCEL** integration tests - they can take 10+ minutes
- Set timeout to 20+ minutes for integration tests
- Tests download test data (~2MB) and spvcf utility automatically

### Manual Application Testing
Since the JAR requires Spark context, test basic functionality:
```bash
# Test JAR help (requires SPARK_HOME setup)
export SPARK_HOME=/path/to/spark
$SPARK_HOME/bin/spark-submit --help

# Test GLnext help (when Spark is available)
$SPARK_HOME/bin/spark-submit \
    --master 'local[*]' --driver-memory 8G \
    target/GLnext-*.jar --help
```

### Validation Scenarios
When making changes, ALWAYS test these scenarios:
1. **Build validation**: `mvn package` completes successfully
2. **Unit test validation**: `mvn test` passes all tests  
3. **Linting validation**: `mvn antrun:run@ktlint` passes without errors
4. **JAR validation**: Confirm `target/GLnext-*.jar` is created and ~24MB
5. **Integration validation**: Run prove tests if dependencies are available

### CI Validation Commands
Always run these before committing (matches .github/workflows/build.yml):
```bash
# Linting (REQUIRED - CI will fail without this)
mvn antrun:run@ktlint
mvn antrun:run@ktlint-format

# Unit tests
mvn test

# Build 
mvn package -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=warn
```

## Project Structure and Navigation

### Key Directories
```
/home/runner/work/GLnext/GLnext/
├── src/net/mlin/GLnext/          # Main Kotlin source code
│   ├── SparkApp.kt               # Main application entry point
│   ├── data/                     # Data models and parsing
│   ├── joint/                    # Joint calling algorithms
│   └── util/                     # Utility functions
├── src/test/                     # Unit tests (Kotlin)
├── src/resources/                # Configuration files
├── test/                         # Integration tests (bash)
│   ├── dv1KGP.t                 # Main integration test
│   └── bash-tap/                # Test framework (git submodule)
├── dx/GLnext/                   # DNAnexus platform integration
├── target/                      # Build output (JAR files)
└── pom.xml                      # Maven build configuration
```

### Important Files to Check After Changes
- **SparkApp.kt**: Main application logic and CLI interface
- **pom.xml**: Dependencies and build configuration
- **test/dv1KGP.t**: Integration test scenarios
- **.github/workflows/build.yml**: CI pipeline definition

## Common Build Issues and Solutions

### Network/Firewall Issues
- **Symptom**: Maven dependencies fail to download
- **Solution**: Document the specific blocked repositories
- **Workaround**: Use local Maven cache or alternative repositories

### Spark Context Issues  
- **Symptom**: `java.lang.NoClassDefFoundError: org/apache/log4j/Logger`
- **Cause**: JAR run without Spark context
- **Solution**: Always use `spark-submit` wrapper, not direct `java -jar`

### Memory Issues
- **Symptom**: OutOfMemoryError during build or tests
- **Solution**: Increase Maven memory: `export MAVEN_OPTS="-Xmx4g"`

### Submodule Issues
- **Symptom**: bash-tap directory empty, test framework missing
- **Cause**: SSH access blocked or submodule not initialized
- **Solution**: `git submodule update --init --recursive`

### Kotlin Compilation Warnings (Expected)
- **Deprecation warnings**: `Char.toInt()` deprecated, use `Char.code` instead
- **Unused parameter warnings**: Some parameters in joint calling code
- **These are non-blocking**: Build succeeds despite warnings

## Application Runtime Usage

### Local Development Run
```bash
export SPARK_HOME=/path/to/spark-3.3.4-bin-hadoop3

# Single-file test
$SPARK_HOME/bin/spark-submit --master 'local[*]' --driver-memory 8G \
    target/GLnext-*.jar --config DeepVariant.WGS \
    /path/to/sample.gvcf.gz /path/to/output/

# Multi-file manifest
$SPARK_HOME/bin/spark-submit --master 'local[*]' --driver-memory 8G \
    target/GLnext-*.jar --config DeepVariant.WGS \
    --manifest manifest.txt /path/to/output/
```

### Configuration Options
Available in `src/resources/config/`:
- `DeepVariant.WGS`: Whole genome sequencing (DeepVariant.WGS.toml)
- `DeepVariant.WES`: Whole exome sequencing (DeepVariant.WES.toml)  
- `DeepVariant.AllQC.WGS`: WGS with all QC fields (DeepVariant.toml + AllQC)
- `DeepVariant.AllQC.WES`: WES with all QC fields (DeepVariant.toml + AllQC)
- `GLIMPSE`: GLIMPSE-based configuration (GLIMPSE.toml)
- `GxS.AllQC`: GenomicsDB export with all QC (GxS.AllQC.toml)

## Platform-Specific Notes

### System Requirements
- **Platform**: x86-64 Linux or macOS only (native libraries)
- **Java**: JDK 11+ (tested with JDK 17)
- **Memory**: 8GB+ RAM recommended for development
- **Spark**: Version 3.3.x (compatibility with other versions not assured)

### External Tool Dependencies
- **bcftools**: VCF file processing (version 1.19+ recommended)
- **tabix**: VCF indexing (version 1.19+ recommended) 
- **spvcf**: Sparse VCF decoder (downloaded automatically by tests)

### Cloud Platform Support
- **Google Cloud Dataproc**: Supported with specific Spark configurations
- **DNAnexus**: Supported via dx/GLnext/ app definition
- **Local Spark clusters**: Supported with proper memory tuning

## Troubleshooting Quick Reference

| Issue | Command to Check | Solution |
|-------|------------------|----------|
| Build fails | `mvn package` | Check Java version, increase timeout |
| Tests fail | `mvn test` | Check unit test logs, verify dependencies |
| Lint fails | `mvn antrun:run@ktlint` | Run `mvn antrun:run@ktlint-format` |
| No JAR output | `ls target/*.jar` | Rebuild with `mvn clean package` |
| Spark errors | `$SPARK_HOME/bin/spark-submit --version` | Verify SPARK_HOME and version 3.3.x |
| Test data missing | `prove -v test/dv1KGP.t` | Check network access, verify downloads |

**Remember**: Always use appropriate timeouts (7+ minutes for builds, 20+ minutes for integration tests) and NEVER CANCEL long-running operations.