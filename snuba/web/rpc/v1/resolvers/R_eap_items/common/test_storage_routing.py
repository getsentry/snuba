import unittest
from unittest.mock import Mock, patch

from snuba.downsampled_storage_tiers import Tier
from snuba.web.rpc.v1.resolvers.R_eap_items.common.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
)


class TestStorageRouting(unittest.TestCase):
    def setUp(self):
        self.routing_context = Mock(spec=RoutingContext)
        self.routing_context.target_tier = None
        self.routing_context.query_settings = None
        self.routing_context.query_result = None
        self.routing_context.extra_info = {}

    def test_successful_tier_selection(self):
        """Test that the tier selected by _decide_tier_and_query_settings is correctly set"""
        strategy = BaseRoutingStrategy()
        expected_tier = Tier.TIER_2

        # Mock the _decide_tier_and_query_settings method to return our expected tier
        strategy._decide_tier_and_query_settings = Mock(
            return_value=(expected_tier, None)
        )

        # Run the query
        strategy.run_query(self.routing_context)

        # Verify the tier was set correctly
        self.assertEqual(self.routing_context.target_tier, expected_tier)

    def test_exception_falls_back_to_tier_1(self):
        """Test that when _decide_tier_and_query_settings raises an exception, we fall back to Tier 1"""
        strategy = BaseRoutingStrategy()

        # Make _decide_tier_and_query_settings raise an exception
        strategy._decide_tier_and_query_settings = Mock(
            side_effect=Exception("Test error")
        )

        # Run the query
        strategy.run_query(self.routing_context)

        # Verify we fell back to Tier 1
        self.assertEqual(self.routing_context.target_tier, Tier.TIER_1)

    def test_tier_selection_with_different_strategies(self):
        """Test that different routing strategies can select different tiers"""

        class TestStrategy1(BaseRoutingStrategy):
            def _decide_tier_and_query_settings(self, context):
                return Tier.TIER_2, None

        class TestStrategy2(BaseRoutingStrategy):
            def _decide_tier_and_query_settings(self, context):
                return Tier.TIER_3, None

        # Test first strategy
        strategy1 = TestStrategy1()
        strategy1.run_query(self.routing_context)
        self.assertEqual(self.routing_context.target_tier, Tier.TIER_2)

        # Reset context
        self.routing_context.target_tier = None

        # Test second strategy
        strategy2 = TestStrategy2()
        strategy2.run_query(self.routing_context)
        self.assertEqual(self.routing_context.target_tier, Tier.TIER_3)

    def test_tier_selection_with_invalid_tier(self):
        """Test that invalid tier selection is handled gracefully"""
        strategy = BaseRoutingStrategy()

        # Mock _decide_tier_and_query_settings to return an invalid tier
        strategy._decide_tier_and_query_settings = Mock(
            return_value=("invalid_tier", None)
        )

        # Run the query
        strategy.run_query(self.routing_context)

        # Verify we fell back to Tier 1
        self.assertEqual(self.routing_context.target_tier, Tier.TIER_1)

    def test_tier_selection_with_none_tier(self):
        """Test that None tier selection is handled gracefully"""
        strategy = BaseRoutingStrategy()

        # Mock _decide_tier_and_query_settings to return None
        strategy._decide_tier_and_query_settings = Mock(return_value=(None, None))

        # Run the query
        strategy.run_query(self.routing_context)

        # Verify we fell back to Tier 1
        self.assertEqual(self.routing_context.target_tier, Tier.TIER_1)


if __name__ == "__main__":
    unittest.main()
