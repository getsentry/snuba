from snuba import settings
from snuba.state import set_config, delete_config
from snuba.web.ast_rollout import (
    is_ast_rolled_out,
    KILLSWITCH_CONFIG,
    ROLLOUT_RATE_CONFIG,
)


def test_rollout() -> None:
    settings.AST_DATASET_ROLLOUT = {"events": 100}
    settings.AST_REFERRER_ROLLOUT = {"transactions": {"something": 100}}
    set_config(ROLLOUT_RATE_CONFIG, 100)

    set_config(KILLSWITCH_CONFIG, 1)
    assert is_ast_rolled_out("events", "something") == False

    set_config(KILLSWITCH_CONFIG, 0)
    assert is_ast_rolled_out("events", "something") == True

    set_config(ROLLOUT_RATE_CONFIG, 0)
    assert is_ast_rolled_out("transactions", "something_else") == False
    assert is_ast_rolled_out("transactions", "something") == True

    settings.AST_REFERRER_ROLLOUT = {"events": {"something": 0}}
    settings.AST_DATASET_ROLLOUT = {"events": 100}
    assert is_ast_rolled_out("events", "something") == False

    assert is_ast_rolled_out("outcomes", "something_else") == False

    set_config(ROLLOUT_RATE_CONFIG, 100)
    assert is_ast_rolled_out("outcomes", "something_else") == True

    delete_config(ROLLOUT_RATE_CONFIG)
    delete_config(KILLSWITCH_CONFIG)
