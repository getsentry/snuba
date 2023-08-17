import re
from typing import Type

from snuba.migrations.columns import LowCardinality, MigrationModifiers
from snuba.migrations.operations import ModifyColumn, SqlOperation
from snuba.utils.schemas import ColumnType, Nullable, String, TypeModifier
from snuba.utils.types import ColumnStatesMapType


class DangerousOperationError(Exception):
    """
    Raised when a migration operation is deemed dangerous
    based on some heuristics.
    """

    pass


def check_dangerous_operation(
    op: SqlOperation, columns_state: ColumnStatesMapType
) -> None:
    """
    Returns True if the operation is deemed dangerous or will cause a full columns rewrite
    based on some heuristics.
    """
    try:
        if isinstance(op, ModifyColumn):
            col = op.get_column()
            table = op.get_table_name()
            nodes = op.get_nodes()
            for node in nodes:
                old_type = columns_state.get(
                    (node.host_name, node.port, table, col.name), None
                )
                if old_type:
                    _check_dangerous(old_type, col.type)
    except DangerousOperationError as err:
        raise DangerousOperationError(
            f"Operation {op.format_sql()} is dangerous: \n{err}"
        ) from err


def _check_dangerous(
    old_type_str: str, new_col_type: ColumnType[MigrationModifiers]
) -> None:
    old_type_str = old_type_str.lower()

    # check nullable is not changed
    _check_modifiers("nullable", Nullable, old_type_str, new_col_type)

    if isinstance(new_col_type, String):
        if "string" not in old_type_str:
            raise DangerousOperationError(
                f"Changing column type from {old_type_str} to {new_col_type} is dangerous"
                "because one doesn't have String type in it. "
                "To attempt to run this migration set blocking=True"
            )
        # check cardinality is not changed
        _check_modifiers("lowcardinality", LowCardinality, old_type_str, new_col_type)

    # check codecs make sense
    _check_codecs(old_type_str, new_col_type)


def _check_codecs(old_type: str, new_col_type: ColumnType[MigrationModifiers]) -> None:
    modifiers = new_col_type.get_modifiers()
    if isinstance(modifiers, MigrationModifiers):

        def _has_codec(codec_str: str, modifiers: MigrationModifiers) -> bool:
            for codec in modifiers.codecs or []:
                if re.match(f"^{codec_str.lower()}[\w\D\(\)]*", codec.lower()):
                    return True
            return False

        if _has_codec("Delta", modifiers):
            if not (_has_codec("ZSTD", modifiers) or _has_codec("LZ4", modifiers)):
                raise DangerousOperationError(
                    f"Changing column type from {old_type} to {new_col_type} is dangerous.\n"
                    "Clickhouse 21 doesn't support Delta codec without ZSTD or LZ4. "
                    "To attempt to run this migration set blocking=True"
                )


def _check_modifiers(
    modifier_str: str,
    modifier: Type[TypeModifier],
    old_type_str: str,
    new_col_type: ColumnType[MigrationModifiers],
) -> None:
    modifier_str = modifier_str.lower()
    new_modifiers = new_col_type.get_modifiers()
    if isinstance(new_modifiers, MigrationModifiers):
        if (
            modifier_str in old_type_str
            and not new_modifiers.has_modifier(modifier)
            or modifier_str not in old_type_str
            and new_modifiers.has_modifier(modifier)
        ):
            raise DangerousOperationError(
                f"Changing column type from {old_type_str} to {new_col_type} is dangerous "
                f"because only one has {modifier_str} type in it.\nChanging it will block or isn't supported. "
                "To attempt to run this migration set blocking=True"
            )
