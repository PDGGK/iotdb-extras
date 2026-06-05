# Wk5 IoTDBTableAttributesDao — kraken checkpoint

## Checkpoints
**Task:** Implement Wk5 IoTDBTableAttributesDao (real AttributesDao SPI, 11 methods) on entity_attributes IoTDB Table Mode.
**Started:** 2026-06-02T05:39:47Z
**Last Updated:** 2026-06-02T06:00:00Z

### Phase Status
- Phase 1 (Provided stubs): ✓ VALIDATED (compiles, 62 existing tests pass)
- Phase 2 (Unit tests written, failing): ✓ VALIDATED (red: compile error on missing DAO API)
- Phase 3 (DAO + config + configuration wiring): ✓ VALIDATED
- Phase 4 (Unit tests green): ✓ VALIDATED (20 attributes unit tests pass)
- Phase 5 (IT compiles): ✓ VALIDATED (IoTDBTableAttributesDaoIT.class built)
- Phase 6 (spotless + RAT + dependency-analyze + full regression): ✓ VALIDATED

### Validation State
```json
{
  "test_count": 84,
  "tests_passing": 83,
  "tests_skipped": 1,
  "last_test_command": "mvn -o -Dmaven.repo.local=/tmp/codex-m2-wb-v3 clean test",
  "last_test_exit_code": 0,
  "rat_unapproved": 0,
  "spotless_check": "pass"
}
```

### Key implementation notes
- commons-lang3 NOT on module classpath -> added provided stub org.apache.commons.lang3.tuple.Pair; excluded org/apache/commons/** from jar so it never shadows real commons-lang3 at runtime.
- entity_attributes explicit `time TIMESTAMP TIME` written via tablet.addTimestamp(0, lastUpdateTs); read via row.getTimestamp("time").getTime(). No ColumnCategory.TIME.
- DAO wired as explicit @Bean in IoTDBTableConfiguration (not @Repository @ConditionalOnBean) to avoid the @ConditionalOnBean ordering trap proven via ConditionEvaluationReport (attributes DAO scanned before pool @Bean registered -> "did not find any beans"). @Bean param injection guarantees pool first.
- cluster_mode validator in constructor (fail-fast if iotdb.attributes.cluster_mode not in {sticky-routing,disabled}).
- AttributeKvEntity added as EMPTY plain stub (no jakarta.persistence) just so SPI signature resolves; DAO throws UOE before touching it.

### Resume Context
- COMPLETE. All phases validated.
