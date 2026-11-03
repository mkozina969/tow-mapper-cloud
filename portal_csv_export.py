"""Headerless ten-field Navision portal CSV, matching Portal_export.csv."""
import csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from io import StringIO
import pandas as pd


def _number(value, row, label):
    try:
        number = Decimal(str(value).strip().replace(",", "."))
        if not number.is_finite():
            raise InvalidOperation
    except (InvalidOperation, ValueError):
        raise ValueError(f"Row {row}: {label} must be a finite number.") from None
    return format(number, "f").replace(".", ",")


def _date(value, row):
    if isinstance(value, (datetime, date)):
        parsed = value
    else:
        parsed = None
        for pattern in ("%d.%m.%Y.", "%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(str(value).strip(), pattern)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(f"Row {row}: date must be a valid date.")
    return f"{parsed.day}.{parsed.month}.{parsed.year}."


def portal_csv(frame):
    if len(frame.columns) != 10:
        raise ValueError("Select exactly ten columns in the portal layout order.")
    if frame.empty:
        raise ValueError("No rows available for portal export.")
    output = StringIO(newline="")
    writer = csv.writer(output, delimiter=";", lineterminator="\r\n")
    for position, record in enumerate(frame.itertuples(index=False, name=None), start=1):
        values = []
        for value in record:
            if pd.isna(value):
                values.append("")
            elif isinstance(value, float) and value.is_integer():
                values.append(str(int(value)))
            else:
                values.append(str(value))
        if any("\r" in v or "\n" in v or "\x00" in v for v in values):
            raise ValueError(f"Row {position}: fields cannot contain line breaks or NUL characters.")
        if values[3] != "":
            raise ValueError(f"Row {position}: column 4 must be blank, as in the portal template.")
        for index in (0, 1, 2, 4, 8, 9):
            if not values[index].strip():
                raise ValueError(f"Row {position}: column {index + 1} is required.")
        values[5] = _number(record[5], position, "quantity")
        values[6] = _number(record[6], position, "unit price")
        values[7] = _date(record[7], position)
        writer.writerow(values)
    return output.getvalue().encode("utf-8")
