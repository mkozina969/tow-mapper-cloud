"""Headerless, fixed-width ASCII PRN exports with Windows line endings."""
from decimal import Decimal, InvalidOperation
import pandas as pd


def _field(value, width, row, label):
    if pd.isna(value):
        raise ValueError(f"Row {row}: {label} is missing.")
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    value = str(value).strip()
    if not value or any(ord(c) < 32 or ord(c) > 126 for c in value):
        raise ValueError(f"Row {row}: {label} must contain printable ASCII text.")
    if len(value) > width:
        raise ValueError(f"Row {row}: {label} exceeds {width} characters.")
    return value.ljust(width)


def pallets_prn(frame, tow_col, quantity_col, pallet_col):
    if len({tow_col, quantity_col, pallet_col}) != 3:
        raise ValueError("Choose three different source columns.")
    if frame.empty:
        raise ValueError("No rows available for pallets export.")
    lines = []
    for position, (_, record) in enumerate(frame.iterrows(), start=1):
        try:
            quantity = Decimal(str(record[quantity_col]).strip())
            if not quantity.is_finite():
                raise InvalidOperation
        except (InvalidOperation, ValueError):
            raise ValueError(f"Row {position}: quantity must be a finite number.") from None
        qty_text = format(quantity, "f")
        if "." in qty_text:
            qty_text = qty_text.rstrip("0").rstrip(".")
        lines.append(
            _field(record[tow_col], 20, position, "TOW code")
            + _field(qty_text, 30, position, "Quantity")
            + _field(record[pallet_col], 50, position, "Pallet code")
        )
    return ("\r\n".join(lines) + "\r\n").encode("ascii")


def regular_prn(frame, *columns):
    """Export three selected columns in order, using widths 10 / 15 / 15."""
    if len(columns) != 3 or len(set(columns)) != 3:
        raise ValueError("Choose exactly three different source columns.")
    if frame.empty:
        raise ValueError("No rows available for regular export.")
    lines = []
    for position, (_, record) in enumerate(frame.iterrows(), start=1):
        fields = []
        for column, width in zip(columns, (10, 15, 15)):
            value = record[column]
            if isinstance(value, Decimal) and value.is_finite():
                value = format(value, "f")
                if "." in value:
                    value = value.rstrip("0").rstrip(".")
            fields.append(_field(value, width, position, str(column)))
        lines.append("".join(fields))
    return ("\r\n".join(lines) + "\r\n").encode("ascii")
