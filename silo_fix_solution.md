# Fix for GitHub Enterprise Integration Config Silo Violation

## Problem Summary
The `get_github_enterprise_integration_config` RPC endpoint is failing with a silo boundary violation because it's trying to access the `Integration` model directly from a REGION silo, but `Integration` is a CONTROL-plane model.

## Root Cause
- The `SeerRpcServiceEndpoint` is marked with `@region_silo_endpoint`
- The `get_github_enterprise_integration_config` method calls `integration_service.get_integration()`
- The underlying `DatabaseBackedIntegrationService` uses direct ORM access: `Integration.objects.get()`
- This violates the silo boundary since REGION services cannot directly access CONTROL models

## Solution

### 1. Update the Integration Service Implementation

Replace the direct database access in the integration service with proper RPC calls to the control silo.

**File**: `src/sentry/integrations/services/integration/impl.py`

```python
# BEFORE (problematic direct access)
def get_integration(
    self,
    integration_id: int,
    provider: str | None = None,
    organization_id: int | None = None,
    status: int | None = None,
) -> Integration | None:
    integration_kwargs = {"id": integration_id}
    if provider is not None:
        integration_kwargs["provider"] = provider
    if organization_id is not None:
        integration_kwargs["organizations"] = organization_id
    if status is not None:
        integration_kwargs["status"] = status

    try:
        # This is the problematic line - direct ORM access from REGION silo
        integration = Integration.objects.get(**integration_kwargs)
        return integration
    except Integration.DoesNotExist:
        return None

# AFTER (proper cross-silo communication)
def get_integration(
    self,
    integration_id: int,
    provider: str | None = None,
    organization_id: int | None = None,
    status: int | None = None,
) -> Integration | None:
    from sentry.silo.base import SiloMode
    from sentry.services.hybrid_cloud.integration import integration_service

    # Check if we're in a region silo
    if SiloMode.get_current_mode() == SiloMode.REGION:
        # Use the hybrid cloud service for cross-silo communication
        return integration_service.get_integration(
            integration_id=integration_id,
            provider=provider,
            organization_id=organization_id,
            status=status,
        )
    else:
        # Direct access is safe in control silo
        integration_kwargs = {"id": integration_id}
        if provider is not None:
            integration_kwargs["provider"] = provider
        if organization_id is not None:
            integration_kwargs["organizations"] = organization_id
        if status is not None:
            integration_kwargs["status"] = status

        try:
            integration = Integration.objects.get(**integration_kwargs)
            return integration
        except Integration.DoesNotExist:
            return None
```

### 2. Alternative Solution: Move the Endpoint to Control Silo

If the integration config retrieval is primarily a control-plane operation, consider moving the endpoint to the control silo:

**File**: `src/sentry/seer/endpoints/seer_rpc.py`

```python
# BEFORE
@region_silo_endpoint
class SeerRpcServiceEndpoint(Endpoint):
    # ...

# AFTER
@control_silo_endpoint  # or @all_silo_endpoint if needed in both
class SeerRpcServiceEndpoint(Endpoint):
    # ...
```

### 3. Use Hybrid Cloud Integration Service (Recommended)

The best approach is to ensure the integration service properly uses the hybrid cloud pattern:

**File**: `src/sentry/integrations/services/integration/impl.py`

```python
from sentry.services.hybrid_cloud.integration.service import IntegrationService
from sentry.silo.base import SiloMode

class DatabaseBackedIntegrationService(IntegrationService):
    def get_integration(
        self,
        integration_id: int,
        provider: str | None = None,
        organization_id: int | None = None,
        status: int | None = None,
    ) -> Integration | None:
        # Always use the hybrid cloud service for cross-silo safety
        from sentry.services.hybrid_cloud.integration import integration_service as hc_service

        # Delegate to the hybrid cloud service which handles silo routing
        return hc_service.get_integration(
            integration_id=integration_id,
            provider=provider,
            organization_id=organization_id,
            status=status,
        )
```

## Implementation Steps

1. **Identify the correct integration service**: Locate the service implementation that's causing the direct database access
2. **Update the service method**: Replace direct ORM calls with hybrid cloud service calls
3. **Test the fix**: Ensure the GitHub Enterprise integration config can be retrieved without silo violations
4. **Update any other similar patterns**: Look for other places where Integration models are accessed directly from region silos

## Verification

After implementing the fix:

1. The RPC call to `get_github_enterprise_integration_config` should succeed
2. No `SiloLimit.AvailabilityError` should be raised
3. The HTTP response should be 200 instead of 400
4. The Autofix process should continue without the `InitializationError`

## Files to Modify

- `src/sentry/integrations/services/integration/impl.py`
- Potentially `src/sentry/seer/endpoints/seer_rpc.py` (if changing silo assignment)
- Any tests that mock the integration service behavior
