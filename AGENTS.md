# AGENTS.md

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

## Evergreen documentation policy
- `README.md` is the evergreen human-facing product and usage guide.
- `AGENTS.md` is the evergreen agent-facing operations guide.
- These documents should provide high-level orientation on working with the product and codebase.
- They should always be kept consistent with the actual code.
- At the same time, they should remain concise, covering only the most important aspects, and shouldn't see churn for every minor code change.

## Local environment checklist
1. Install runtime tools: Maven, `bcftools`, `tabix`, `prove`, and Spark 3.2.4.
2. Ensure submodules are present:
   - `git submodule update --init --recursive`
3. For Spark integration tests, use JDK 11:
   - `export JAVA_HOME=/path/to/jdk-11`
   - `export PATH="$JAVA_HOME/bin:$PATH"`
4. Set Spark path:
   - `export SPARK_HOME=/path/to/spark-3.2.4-bin-hadoop3.2`

## Task completion punch list
- **Always:** ensure `AGENTS.md` and `README.md` are up-to-date, per the aforementioned policy.
- **Always:** run lint and formatter before finalizing code changes:
  - `mvn antrun:run@ktlint`
  - `mvn antrun:run@ktlint-format`
- **Usually:** run unit tests for nontrivial changes:
  - `mvn test`
- **When warranted:** run end-to-end tests:
  - `mvn package -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=warn`
  - `DV1KGP_5PCT=1 prove -v test/dv1KGP.t` (smoke test on 5% of test dataset)
  - `prove -v test/dv1KGP.t`

## Known gotchas
- Missing `test/bash-tap` submodule causes TAP functions (`plan`, `is`) to fail immediately.
- Spark 3.2.4 + newer JDKs can fail at runtime; use JDK 11 for reliable integration tests.
- If `loadConfig` is changed, avoid `inheritedNames.reversed()` style calls; this path has previously thrown `NoSuchMethodError: java.util.List.reversed()` under JDK 11.
- If changing configs or CLI behavior, update both docs files in the same PR.
