# AGENTS.md

## Mission
Keep GLnext buildable, testable, and understandable in this container with minimal re-orientation.

## Evergreen documentation policy (strong directive)
- **ALWAYS keep `AGENTS.md` and `README.md` up to date in the same PR whenever setup, layout, runtime assumptions, or test workflows change.**
- `AGENTS.md` is the evergreen agent-facing operations guide.
- `README.md` is the evergreen human-facing product and usage guide.
- If you discover drift, fix it immediately instead of deferring it.

## Repository layout (high value paths)
- `src/net/mlin/GLnext/` — Kotlin implementation.
  - `SparkApp.kt` — CLI entrypoint and top-level orchestration.
  - `joint/` — joint calling and genotyping logic.
  - `data/` — core genomic/pVCF data models.
  - `util/` — I/O, BGZF, SQLite, and utility helpers.
- `src/resources/config/` — built-in TOML configs referenced by `--config`.
- `src/test/` — unit tests (Kotlin/JUnit via Maven Surefire).
- `test/dv1KGP.t` — integration test harness (TAP + Spark + bcftools/tabix).
- `.github/workflows/build.yml` — source of truth for CI steps.
- `dx/GLnext/` — DNAnexus app packaging/scripts.

## Local environment checklist
1. Install runtime tools: Maven, `bcftools`, `tabix`, `prove`, and Spark 3.2.4.
2. Ensure submodules are present:
   - `git submodule update --init --recursive`
3. For Spark integration tests, use JDK 11:
   - `export JAVA_HOME=/path/to/jdk-11`
   - `export PATH="$JAVA_HOME/bin:$PATH"`
4. Set Spark path:
   - `export SPARK_HOME=/path/to/spark-3.2.4-bin-hadoop3.2`

## CI parity commands
Run in repo root:

```bash
mvn antrun:run@ktlint
mvn antrun:run@ktlint-format
mvn test
mvn package -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=warn
DV1KGP_5PCT=1 prove -v test/dv1KGP.t
prove -v test/dv1KGP.t
```

## Known gotchas
- Missing `test/bash-tap` submodule causes TAP functions (`plan`, `is`) to fail immediately.
- Spark 3.2.4 + newer JDKs can fail at runtime; use JDK 11 for reliable integration tests.
- If `loadConfig` is changed, avoid `inheritedNames.reversed()` style calls; this path has previously thrown `NoSuchMethodError: java.util.List.reversed()` under JDK 11.
- DRAGEN config sets `discovery.ignoreFilteredInputRecords=true`, which drops any input records with `FILTER` not equal to `.` or `PASS`; toggle via `-Dconfig.override.discovery.ignoreFilteredInputRecords=...`.
- If changing configs or CLI behavior, update both docs files in the same PR.
