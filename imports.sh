autoflake --remove-all-unused-imports  snuba/clickhouse/formatter/query.py
sleep 5
autoflake --remove-all-unused-imports  snuba/clickhouse/query_inspector.py
sleep 5
autoflake --remove-all-unused-imports  snuba/clickhouse/query_profiler.py
sleep 5
autoflake --remove-all-unused-imports  snuba/datasets/entity_subscriptions/processors.py
sleep 5
autoflake --remove-all-unused-imports  snuba/datasets/entity_subscriptions/validators.py
sleep 5
autoflake --remove-all-unused-imports  snuba/datasets/plans/query_plan.py
sleep 5
autoflake --remove-all-unused-imports  snuba/pipeline/composite.py
sleep 5
autoflake --remove-all-unused-imports  snuba/pipeline/stages/query_execution.py
sleep 5
autoflake --remove-all-unused-imports  snuba/pipeline/stages/query_processing.py
sleep 5
autoflake --remove-all-unused-imports  snuba/pipeline/utils/storage_finder.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/__init__.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/composite.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/data_source/projects_finder.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/data_source/visitor.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/formatters/tracing.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/indexer/resolver.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/joins/equivalence_adder.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/joins/semi_joins.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/joins/subquery_generator.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/mql/parser.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/parser/__init__.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/parser/exceptions.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/parser/validation/functions.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/processors/logical/filter_in_select_optimizer.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/processors/physical/__init__.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/snql/anonymize.py
sleep 5
autoflake --remove-all-unused-imports  snuba/query/snql/parser.py
sleep 5
autoflake --remove-all-unused-imports  snuba/request/__init__.py
sleep 5
autoflake --remove-all-unused-imports  snuba/request/validation.py
sleep 5
autoflake --remove-all-unused-imports  snuba/subscriptions/data.py
sleep 5
autoflake --remove-all-unused-imports  snuba/web/db_query.py
sleep 5
autoflake --remove-all-unused-imports  tests/datasets/test_context_promotion.py
sleep 5
autoflake --remove-all-unused-imports  tests/datasets/test_group_attributes_join.py
sleep 5
autoflake --remove-all-unused-imports  tests/datasets/test_metrics_processing.py
sleep 5
autoflake --remove-all-unused-imports  tests/datasets/test_nullable_field_casting.py
sleep 5
autoflake --remove-all-unused-imports  tests/pipeline/test_composite_planner.py
sleep 5
autoflake --remove-all-unused-imports  tests/query/formatters/test_query.py
sleep 5
autoflake --remove-all-unused-imports  tests/query/indexer/test_resolver.py
sleep 5
autoflake --remove-all-unused-imports  tests/query/joins/test_semi_join.py
sleep 5
autoflake --remove-all-unused-imports  tests/query/joins/test_subqueries.py
sleep 5
autoflake --remove-all-unused-imports  tests/subscriptions/entity_subscriptions/test_entity_subscriptions.py
sleep 5
autoflake --remove-all-unused-imports  tests/web/test__get_allocation_policy.py
sleep 5
autoflake --remove-all-unused-imports  tests/web/test_project_finder.py
sleep 5
autoflake --remove-all-unused-imports  tests/web/test_tables_collector.py
sleep 5
