# AGENTS

Purpose: AI behavior rules for this repository.

## Project
- Repository: `jcustenborder/kafka-connect-transform-common`
- Target branch: `master`
- GitHub Project: https://github.com/users/jcustenborder/projects/2/views/1

## Build and verification
- Use Java 17.
- Use Maven.
- Run `mvn -B clean verify` before reporting implementation complete.
- Run `scripts/validate-cp-8.3.0-plugins.sh` to validate Confluent Platform 8.3.0 plugin discovery when Docker is available.
- Do not run release/deploy commands locally unless explicitly requested.

## Implementation rules
- Keep diffs minimal and focused on the requested issue.
- Do not refactor unrelated transformation logic.
- Do not edit generated files, IDE files, `.okhttpcache`, or `target/`.
- Do not add dependencies unless required by the issue and verified by build/test failures.
- Do not add Testcontainers unless replacing tracked Docker Compose integration testing.
- Prefer existing project patterns over new abstractions.

## Kafka Connect release rules
- Project versions use `major.minor-SNAPSHOT` on `master`.
- GitHub Actions computes release versions as `major.minor.patch` from existing tags.
- Kafka Connect plugin discovery must support `ServiceLoader` manifests under `src/main/resources/META-INF/services/`.
