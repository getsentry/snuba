"""
Test cases to verify the silo boundary fix for GitHub Enterprise integration config.

This test ensures that the integration service properly handles cross-silo communication
and doesn't violate silo boundaries when accessing Integration models.
"""

from unittest.mock import MagicMock, patch

import pytest
from sentry.integrations.services.integration.impl import (
    DatabaseBackedIntegrationService,
)
from sentry.models.integrations import Integration
from sentry.silo.base import SiloMode


class TestSiloBoundaryFix:
    """Test cases for the silo boundary violation fix."""

    def setup_method(self):
        """Set up test fixtures."""
        self.service = DatabaseBackedIntegrationService()
        self.integration_id = 261546
        self.organization_id = 1118521
        self.provider = "github_enterprise"

    @patch("sentry.silo.base.SiloMode.get_current_mode")
    @patch("sentry.services.hybrid_cloud.integration.integration_service")
    def test_region_silo_uses_hybrid_cloud_service(self, mock_hc_service, mock_silo_mode):
        """Test that REGION silo uses hybrid cloud service instead of direct DB access."""
        # Arrange
        mock_silo_mode.return_value = SiloMode.REGION
        mock_integration = MagicMock(spec=Integration)
        mock_hc_service.get_integration.return_value = mock_integration

        # Act
        result = self.service.get_integration(
            integration_id=self.integration_id,
            provider=self.provider,
            organization_id=self.organization_id,
        )

        # Assert
        assert result == mock_integration
        mock_hc_service.get_integration.assert_called_once_with(
            integration_id=self.integration_id,
            provider=self.provider,
            organization_id=self.organization_id,
            status=None,
        )

    @patch("sentry.silo.base.SiloMode.get_current_mode")
    @patch("sentry.models.integrations.Integration.objects.get")
    def test_control_silo_uses_direct_access(self, mock_integration_get, mock_silo_mode):
        """Test that CONTROL silo uses direct database access."""
        # Arrange
        mock_silo_mode.return_value = SiloMode.CONTROL
        mock_integration = MagicMock(spec=Integration)
        mock_integration_get.return_value = mock_integration

        # Act
        result = self.service.get_integration(
            integration_id=self.integration_id,
            provider=self.provider,
            organization_id=self.organization_id,
        )

        # Assert
        assert result == mock_integration
        mock_integration_get.assert_called_once_with(
            id=self.integration_id,
            provider=self.provider,
            organizations=self.organization_id,
        )

    @patch("sentry.silo.base.SiloMode.get_current_mode")
    @patch("sentry.services.hybrid_cloud.integration.integration_service")
    def test_region_silo_handles_not_found(self, mock_hc_service, mock_silo_mode):
        """Test that REGION silo properly handles integration not found."""
        # Arrange
        mock_silo_mode.return_value = SiloMode.REGION
        mock_hc_service.get_integration.return_value = None

        # Act
        result = self.service.get_integration(
            integration_id=self.integration_id,
            provider=self.provider,
        )

        # Assert
        assert result is None

    @patch("sentry.silo.base.SiloMode.get_current_mode")
    @patch("sentry.models.integrations.Integration.objects.get")
    def test_control_silo_handles_not_found(self, mock_integration_get, mock_silo_mode):
        """Test that CONTROL silo properly handles integration not found."""
        # Arrange
        mock_silo_mode.return_value = SiloMode.CONTROL
        mock_integration_get.side_effect = Integration.DoesNotExist()

        # Act
        result = self.service.get_integration(
            integration_id=self.integration_id,
            provider=self.provider,
        )

        # Assert
        assert result is None

    @patch("sentry.silo.base.SiloMode.get_current_mode")
    def test_monolith_silo_uses_direct_access(self, mock_silo_mode):
        """Test that MONOLITH silo uses direct database access."""
        # Arrange
        mock_silo_mode.return_value = SiloMode.MONOLITH

        with patch("sentry.models.integrations.Integration.objects.get") as mock_get:
            mock_integration = MagicMock(spec=Integration)
            mock_get.return_value = mock_integration

            # Act
            result = self.service.get_integration(
                integration_id=self.integration_id,
                provider=self.provider,
            )

            # Assert
            assert result == mock_integration
            mock_get.assert_called_once()


class TestGitHubEnterpriseRPCEndpoint:
    """Test the GitHub Enterprise RPC endpoint fix."""

    @patch("sentry.silo.base.SiloMode.get_current_mode")
    @patch("sentry.integrations.services.integration.integration_service")
    def test_github_enterprise_config_success(self, mock_service, mock_silo_mode):
        """Test successful GitHub Enterprise config retrieval."""
        # Arrange
        mock_silo_mode.return_value = SiloMode.CONTROL  # Now safe for direct access

        mock_integration = MagicMock(spec=Integration)
        mock_integration.metadata = {
            "domain_name": "github.enterprise.com",
            "api_url": "https://github.enterprise.com/api/v3",
            "app_id": "12637",
        }
        mock_integration.external_id = "261546"

        mock_service.get_integration.return_value = mock_integration

        # Import the endpoint class (would be in actual test)
        from sentry.seer.endpoints.seer_rpc import SeerRpcServiceEndpoint

        endpoint = SeerRpcServiceEndpoint()

        # Act
        result = endpoint.get_github_enterprise_integration_config(
            integration_id="261546",
            organization_id=1118521,
        )

        # Assert
        assert result["success"] is True
        assert result["base_url"] == "github.enterprise.com"
        assert result["api_url"] == "https://github.enterprise.com/api/v3"
        assert result["app_id"] == "12637"
        assert result["installation_id"] == "261546"

    @patch("sentry.integrations.services.integration.integration_service")
    def test_github_enterprise_config_not_found(self, mock_service):
        """Test GitHub Enterprise config when integration not found."""
        # Arrange
        mock_service.get_integration.return_value = None

        from sentry.seer.endpoints.seer_rpc import SeerRpcServiceEndpoint

        endpoint = SeerRpcServiceEndpoint()

        # Act
        result = endpoint.get_github_enterprise_integration_config(
            integration_id="999999",
            organization_id=1118521,
        )

        # Assert
        assert result["success"] is False
        assert "not found" in result["error"].lower()


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
