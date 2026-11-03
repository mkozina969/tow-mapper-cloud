"""Supplier-specific packaging conversion, independent of Streamlit and the database."""
from decimal import Decimal, InvalidOperation, localcontext


def convert_invoice(frame, rules, quantity_col, price_col):
    """Return a converted copy and audit rows. Never modify the source invoice.

    Rules map supplier IDs to positive integer dividers for one selected vendor.
    Reject the whole batch on invalid quantities/prices rather than exporting a
    partially converted invoice. Callers must always pass the original upload.
    """
    if quantity_col == price_col or "supplier_id" in (quantity_col, price_col):
        raise ValueError("Supplier, quantity and price columns must be different.")
    result = frame.copy(deep=True)
    for col in (quantity_col, price_col):
        result[col] = result[col].astype(object)
    audit = []
    for position, (_, row) in enumerate(frame.iterrows()):
        supplier = str(row["supplier_id"]).strip()
        if supplier not in rules:
            continue
        try:
            divider = Decimal(str(rules[supplier]))
            qty = Decimal(str(row[quantity_col]).strip())
            price = Decimal(str(row[price_col]).strip())
            if not all(x.is_finite() for x in (divider, qty, price)):
                raise ValueError("Values must be finite numbers.")
            if divider <= 0 or divider != divider.to_integral_value():
                raise ValueError("Divider must be a positive integer.")
            with localcontext() as context:
                context.prec = max(50, len(qty.as_tuple().digits) + len(price.as_tuple().digits) + len(divider.as_tuple().digits) + 10)
                if qty % divider != 0:
                    raise ValueError(f"Quantity {qty} is not divisible by {divider}.")
                converted_qty, converted_price = qty / divider, price * divider
                if qty * price != converted_qty * converted_price:
                    raise ValueError("Conversion changed the line value.")
                result.iat[position, result.columns.get_loc(quantity_col)] = converted_qty
                result.iat[position, result.columns.get_loc(price_col)] = converted_price
                audit.append({"row": position + 2, "supplier_id": supplier, "divider": int(divider), "original_quantity": str(qty), "original_price": str(price), "quantity": str(converted_qty), "price": str(converted_price), "line_value": str(qty * price)})
        except (InvalidOperation, ValueError, ArithmeticError) as exc:
            raise ValueError(f"Excel row {position + 2}, item {supplier}: {exc}") from exc
    return result, audit
