# How to Enable Snuba Feature Flags for Local Testing

Snuba uses Redis-based runtime configuration (feature flags) that can be enabled/disabled without restarting services.

## Quick Start

### Method 1: Using Snuba CLI (Recommended)

```bash
# Set a feature flag
snuba config set <key> <value>

# Examples:
snuba config set enable_trace_pagination 1
snuba config set use_distribution_metrics 1
snuba config set my_feature_enabled "true"

# Get current value
snuba config get enable_trace_pagination

# Get all configs
snuba config get-all

# Delete a config
snuba config delete my_feature_enabled
```

### Method 2: Using Redis CLI

```bash
# Connect to Redis
redis-cli -p 6379

# Set a config (stored in hash 'snuba-config')
HSET snuba-config enable_trace_pagination 1

# Get a config
HGET snuba-config enable_trace_pagination

# Get all configs
HGETALL snuba-config

# Delete a config
HDEL snuba-config enable_trace_pagination
```

### Method 3: Using Python in Tests

```python
from snuba import state

# Set config
state.set_config("enable_trace_pagination", 1)

# Get config
value = state.get_int_config("enable_trace_pagination", default=0)

# Set multiple configs
state.set_configs({
    "enable_trace_pagination": 1,
    "use_distribution_metrics": 1
})

# Delete config
state.delete_config("enable_trace_pagination")
```

---

## Common Feature Flags

### Trace/EAP Related

```bash
# Enable pagination for GetTrace endpoint
snuba config set enable_trace_pagination 1

# Apply final rollout for GetTrace
snuba config set EndpointGetTrace.apply_final_rollout_percentage 100

# Max items per page for trace pagination
snuba config set endpoint_get_trace_pagination_max_items 1000
```

### Performance/Routing

```bash
# Enable storage routing
snuba config set enable_storage_routing 1

# Enable flextime routing
snuba config set enable_flextime_routing 1

# Cache expiry time (seconds)
snuba config set cache_expiry_sec 300
```

### Metrics

```bash
# Enable Sentry metrics backend
snuba config set use_sentry_metrics 1

# RPC logging sample rate (0.0 to 1.0)
snuba config set rpc_logging_sample_rate 1.0

# Flush logs immediately
snuba config set rpc_logging_flush_logs 1
```

### Development

```bash
# Enable query logging
snuba config set record_queries 1

# Enable debug mode
snuba config set debug_mode 1

# Sample rate for various features (0.0 to 1.0)
snuba config set my_feature_sample_rate 0.5
```

---

## How Feature Flags Work

### 1. Storage Location

All runtime configs are stored in **Redis** in the hash `snuba-config`:

```
Redis Key: snuba-config
Type: Hash (key-value pairs)
```

### 2. Accessing in Code

Snuba provides type-safe accessors:

```python
from snuba import state

# Integer config
value = state.get_int_config("enable_trace_pagination", default=0)

# Float config
rate = state.get_float_config("sample_rate", default=0.0)

# String config
mode = state.get_str_config("routing_mode", default="normal")

# Generic (auto-type detection, deprecated)
value = state.get_config("my_config", default=None)
```

### 3. Example in EndpointGetTrace

```python
# In snuba/web/rpc/v1/endpoint_get_trace.py

enable_pagination = state.get_int_config(
    "enable_trace_pagination",
    ENABLE_TRACE_PAGINATION_DEFAULT
)

if enable_pagination:
    limit = _get_pagination_limit(in_msg.limit)
else:
    limit = None
```

### 4. Config Caching

Configs are cached for a short period (defined by `CONFIG_MEMOIZE_TIMEOUT`):

- **Production:** Cached for a few seconds
- **Testing:** No caching (`CONFIG_MEMOIZE_TIMEOUT = 0`)

This means changes take effect almost immediately!

---

## Setting Up Redis for Local Testing

### 1. Check Redis is Running

```bash
# Check if Redis is accessible
redis-cli ping
# Expected output: PONG
```

### 2. Default Redis Configuration

Snuba uses these Redis databases by default (in `settings_test.py`):

```python
REDIS_CLUSTERS = {
    "cache": {"db": 2},
    "config": {"db": 6},  # <-- Feature flags stored here
    "rate_limiter": {"db": 3},
    # ... other dbs
}
```

### 3. Start Snuba with devserver

```bash
# This starts all services including Redis
snuba devserver
```

Or manually:

```bash
# Start Redis
redis-server

# Start Snuba API
snuba api

# Start Snuba admin (for web UI)
snuba admin
```

---

## Complete Example Workflow

### Scenario: Enable trace pagination for testing

```bash
# 1. Check current value
snuba config get enable_trace_pagination
# Output: None (not set)

# 2. Enable pagination
snuba config set enable_trace_pagination 1

# 3. Verify it's set
snuba config get enable_trace_pagination
# Output: 1

# 4. Make a request to EndpointGetTrace
# The pagination logic will now be active!

# 5. Test with different value
snuba config set enable_trace_pagination 0

# 6. Verify pagination is disabled
snuba config get enable_trace_pagination
# Output: 0

# 7. Clean up (optional)
snuba config delete enable_trace_pagination
```

### Example in Python Test

```python
import pytest
from snuba import state
from tests.fixtures import get_raw_event

class TestGetTraceWithPagination:
    @pytest.mark.redis_db
    def test_pagination_enabled(self) -> None:
        # Enable pagination for this test
        state.set_config("enable_trace_pagination", 1)
        state.set_config("endpoint_get_trace_pagination_max_items", 10)

        # Make request to GetTrace
        response = self.app.post(
            "/rpc/EndpointGetTrace/v1",
            data=trace_request.SerializeToString(),
        )

        # Verify pagination is working
        assert response.status_code == 200
        result = GetTraceResponse()
        result.ParseFromString(response.data)
        assert result.HasField("page_token")

    def teardown_method(self) -> None:
        # Clean up configs after test
        state.delete_config("enable_trace_pagination")
        state.delete_config("endpoint_get_trace_pagination_max_items")
```

---

## Advanced: Component-Level Configs

Some features use **component-specific configs** with parameters:

### Example: Allocation Policy Config

```python
# In code:
from snuba.configs.configuration import ConfigurableComponent

class MyAllocationPolicy(ConfigurableComponent):
    def config_definitions(self):
        return {
            "max_threads": ConfigDefinition(
                type=int,
                default=10,
                description="Max threads for queries"
            )
        }

    def get_max_threads(self, org_id: int) -> int:
        # Gets config: "MyAllocationPolicy.max_threads.org_id:123"
        return self.get_config_value(
            "max_threads",
            params={"org_id": org_id}
        )
```

### Setting Component Configs

```bash
# Set config for specific org_id
# Key format: ComponentName.config_key.param1:value1,param2:value2
redis-cli HSET snuba-config "MyAllocationPolicy.max_threads.org_id:123" 20

# Or use Python:
policy.set_config_value("max_threads", 20, params={"org_id": 123})
```

---

## Viewing All Current Configs

### Via CLI

```bash
# Human-readable format
snuba config get-all

# JSON format
snuba config get-all --format json

# YAML format (if available)
snuba config get-all --format yaml
```

### Via Python

```python
from snuba import state

# Get all configs as dict
all_configs = state.get_all_configs()
for key, value in all_configs.items():
    print(f"{key} = {value}")
```

### Via Redis

```bash
redis-cli
> SELECT 6  # Switch to config db
> HGETALL snuba-config
```

---

## Common Patterns

### 1. Feature Rollout (Percentage-based)

```python
# In code:
import random
from snuba import state

def should_enable_feature() -> bool:
    percentage = state.get_int_config("my_feature_rollout_pct", 0)
    return random.randint(0, 100) < percentage

# Enable for 50% of requests
if should_enable_feature():
    use_new_logic()
else:
    use_old_logic()
```

```bash
# Set rollout percentage
snuba config set my_feature_rollout_pct 50  # 50% enabled
```

### 2. Feature Toggle (Boolean)

```python
# In code:
from snuba import state

if state.get_int_config("use_new_cache", 0):
    return new_cache_implementation()
else:
    return old_cache_implementation()
```

```bash
# Enable feature
snuba config set use_new_cache 1

# Disable feature
snuba config set use_new_cache 0
```

### 3. Sampling Rate (Float)

```python
# In code:
import random
from snuba import state

sample_rate = state.get_float_config("logging_sample_rate", 0.0)
if random.random() < sample_rate:
    log_detailed_info()
```

```bash
# Set sample rate to 10%
snuba config set logging_sample_rate 0.1
```

### 4. Configuration Value (String/Int)

```python
# In code:
from snuba import state

cache_ttl = state.get_int_config("cache_ttl_seconds", 300)
max_retries = state.get_int_config("max_query_retries", 3)
routing_mode = state.get_str_config("routing_mode", "normal")
```

```bash
# Set configuration values
snuba config set cache_ttl_seconds 600
snuba config set max_query_retries 5
snuba config set routing_mode "aggressive"
```

---

## Troubleshooting

### Config Not Taking Effect

1. **Check Redis connection:**
   ```bash
   redis-cli -p 6379 ping
   ```

2. **Verify config is set:**
   ```bash
   snuba config get your_config_key
   ```

3. **Check cache timeout:**
   - In tests: Should be instant (no caching)
   - In production: Wait a few seconds for cache to expire

4. **Restart services if needed:**
   ```bash
   # Kill and restart devserver
   pkill -f "snuba"
   snuba devserver
   ```

### Type Mismatch Error

```bash
# If you see: "Mismatched types for key..."
# Use --force-type flag
snuba config set my_key "new_value" --force-type
```

### Redis Database Issues

```bash
# Check which database is being used
redis-cli
> CLIENT LIST
> INFO keyspace

# Verify config database (db 6)
> SELECT 6
> KEYS *
> HGETALL snuba-config
```

### Config Not Persisting

Configs persist in Redis until explicitly deleted or Redis is flushed:

```bash
# Check if Redis has persistence enabled
redis-cli CONFIG GET save

# Manually save Redis data
redis-cli SAVE
```

---

## Best Practices

### 1. Use Descriptive Names

```python
# Good
state.get_int_config("enable_trace_pagination", 0)
state.get_float_config("allocation_policy_sample_rate", 0.0)

# Bad
state.get_config("flag1", 0)
state.get_config("x", 0)
```

### 2. Always Provide Defaults

```python
# Good - provides fallback
enabled = state.get_int_config("my_feature", default=0)

# Risky - None can cause issues
enabled = state.get_config("my_feature")
if enabled:  # Could fail if None
    use_feature()
```

### 3. Use Type-Specific Getters

```python
# Good - explicit types
count = state.get_int_config("max_items", 100)
rate = state.get_float_config("sample_rate", 0.5)
mode = state.get_str_config("mode", "normal")

# Deprecated - generic getter
value = state.get_config("max_items", 100)
```

### 4. Clean Up in Tests

```python
def teardown_method(self):
    # Always clean up test configs
    state.delete_config("test_feature_flag")
```

### 5. Document Your Flags

```python
# In code, add comments explaining the flag
# Config: enable_trace_pagination
# Description: Enables pagination for GetTrace endpoint
# Values: 0 (disabled), 1 (enabled)
# Default: 0
enable_pagination = state.get_int_config("enable_trace_pagination", 0)
```

---

## Quick Reference

| Task | Command |
|------|---------|
| Set flag | `snuba config set <key> <value>` |
| Get flag | `snuba config get <key>` |
| Get all | `snuba config get-all` |
| Delete flag | `snuba config delete <key>` |
| Set in Python | `state.set_config(key, value)` |
| Get in Python | `state.get_int_config(key, default)` |
| Redis direct | `redis-cli HSET snuba-config <key> <value>` |

---

## Related Documentation

- **State Module:** `snuba/state/__init__.py`
- **CLI Config Commands:** `snuba/cli/config.py`
- **Config Tests:** `tests/state/test_state.py`
- **Settings:** `snuba/settings/`
