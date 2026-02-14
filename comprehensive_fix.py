"""
Comprehensive fix for the GitHub Enterprise Integration Config Silo Violation

This file contains the complete solution to fix the InitializationError caused by
accessing Integration model directly from REGION silo in the get_github_enterprise_integration_config RPC method.
"""

# File: src/sentry/integrations/services/integration/impl.py

import logging
from typing import Optional

from sentry.integrations.services.integration.service import IntegrationService
from sentry.models.integrations import Integration
from sentry.silo.base import SiloMode
from sentry.types.integrations import ExternalProviders

logger = logging.getLogger(__name__)


class DatabaseBackedIntegrationService(IntegrationService):
    """
    Updated implementation that respects silo boundaries.

    This service now checks the current silo mode and routes requests appropriately:
    - In REGION silo: Uses hybrid cloud service for cross-silo communication
    - In CONTROL silo: Uses direct database access (safe)
    """

    def get_integration(
        self,
        integration_id: int,
        provider: Optional[str] = None,
        organization_id: Optional[int] = None,
        status: Optional[int] = None,
    ) -> Optional[Integration]:
        """
        Get integration by ID with proper silo boundary handling.

        Args:
            integration_id: The integration ID to look up
            provider: Optional provider filter
            organization_id: Optional organization ID filter
            status: Optional status filter

        Returns:
            Integration instance if found, None otherwise
        """
        try:
            current_silo = SiloMode.get_current_mode()

            # In REGION silo, we must use hybrid cloud service to avoid silo violations
            if current_silo == SiloMode.REGION:
                logger.debug(
                    f"Using hybrid cloud service for integration lookup in REGION silo. "
                    f"integration_id={integration_id}, provider={provider}"
                )

                # Import here to avoid circular imports
                from sentry.services.hybrid_cloud.integration import (
                    integration_service as hc_service,
                )

                return hc_service.get_integration(
                    integration_id=integration_id,
                    provider=provider,
                    organization_id=organization_id,
                    status=status,
                )

            # In CONTROL silo or MONOLITH, direct access is safe
            else:
                logger.debug(
                    f"Using direct database access in {current_silo.name} silo. "
                    f"integration_id={integration_id}, provider={provider}"
                )

                return self._get_integration_direct(
                    integration_id=integration_id,
                    provider=provider,
                    organization_id=organization_id,
                    status=status,
                )

        except Exception as e:
            logger.error(
                f"Error getting integration {integration_id}: {e}",
                extra={
                    "integration_id": integration_id,
                    "provider": provider,
                    "organization_id": organization_id,
                    "silo_mode": SiloMode.get_current_mode().name,
                },
                exc_info=True,
            )
            raise

    def _get_integration_direct(
        self,
        integration_id: int,
        provider: Optional[str] = None,
        organization_id: Optional[int] = None,
        status: Optional[int] = None,
    ) -> Optional[Integration]:
        """
        Direct database access method for use in CONTROL silo.

        This method performs the actual ORM query and should only be called
        when we're certain we're in a silo where direct access is allowed.
        """
        integration_kwargs = {"id": integration_id}

        if provider is not None:
            integration_kwargs["provider"] = provider

        if organization_id is not None:
            integration_kwargs["organizations"] = organization_id

        if status is not None:
            integration_kwargs["status"] = status

        try:
            integration = Integration.objects.get(**integration_kwargs)
            logger.debug(f"Successfully retrieved integration {integration_id}")
            return integration

        except Integration.DoesNotExist:
            logger.debug(f"Integration {integration_id} not found")
            return None

        except Exception as e:
            logger.error(f"Database error retrieving integration {integration_id}: {e}")
            raise


# Alternative approach: Update the RPC endpoint to use control silo
# File: src/sentry/seer/endpoints/seer_rpc.py

from rest_framework import status
from rest_framework.response import Response
from sentry.api.base import control_silo_endpoint
from sentry.api.bases.integration import IntegrationEndpoint


@control_silo_endpoint  # Changed from @region_silo_endpoint
class SeerRpcServiceEndpoint(IntegrationEndpoint):
    """
    Alternative fix: Move the endpoint to control silo where Integration access is allowed.

    This approach moves the entire RPC endpoint to the control silo, eliminating
    the silo boundary issue entirely for integration-related operations.
    """

    def get_github_enterprise_integration_config(
        self, integration_id: str, organization_id: int
    ) -> dict:
        """
        Get GitHub Enterprise integration configuration.

        Now running in control silo, this method can safely access Integration models.
        """
        try:
            from sentry.integrations.services.integration import integration_service

            # Direct service call is now safe since we're in control silo
            integration = integration_service.get_integration(
                integration_id=int(integration_id),
                provider="github_enterprise",
                organization_id=organization_id,
                status=1,  # ObjectStatus.ACTIVE
            )

            if not integration:
                return {"success": False, "error": "Integration not found"}

            # Extract configuration from the integration
            config = {
                "success": True,
                "base_url": integration.metadata.get("domain_name", ""),
                "api_url": integration.metadata.get("api_url", ""),
                "installation_id": integration.external_id,
                "app_id": integration.metadata.get("app_id", ""),
            }

            return config

        except Exception as e:
            logger.error(
                f"Error getting GitHub Enterprise config for integration {integration_id}: {e}",
                exc_info=True,
            )
            return {"success": False, "error": str(e)}


# Additional fix: Ensure proper service registration
# File: src/sentry/integrations/services/integration/__init__.py

from sentry.integrations.services.integration.impl import (
    DatabaseBackedIntegrationService,
)
from sentry.integrations.services.integration.service import IntegrationService
from sentry.silo.base import SiloMode


def get_integration_service() -> IntegrationService:
    """
    Factory function to get the appropriate integration service based on silo mode.

    This ensures we always get a service that respects silo boundaries.
    """
    # Always use the database-backed service, which now handles silo routing internally
    return DatabaseBackedIntegrationService()


# Register the service
integration_service: IntegrationService = get_integration_service()
