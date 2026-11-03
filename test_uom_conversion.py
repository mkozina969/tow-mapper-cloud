import unittest
from decimal import Decimal
import pandas as pd
from uom_conversion import convert_invoice


class ConversionTests(unittest.TestCase):
    def test_package_sizes_and_value(self):
        for divider in (2, 3, 4, 6, 8, 12, 16):
            source = pd.DataFrame({"supplier_id": ["A", "B"], "qty": [divider * 4, 5], "price": ["2.50", "1.23"]})
            result, audit = convert_invoice(source, {"A": divider}, "qty", "price")
            self.assertEqual(result.loc[0, "qty"], Decimal(4))
            self.assertEqual(result.loc[0, "price"], Decimal("2.50") * divider)
            self.assertEqual(result.loc[1, "qty"], 5)
            self.assertEqual(source.loc[0, "qty"], divider * 4)
            self.assertEqual(len(audit), 1)
            again, _ = convert_invoice(source, {"A": divider}, "qty", "price")
            pd.testing.assert_frame_equal(result, again)

    def test_invalid_inputs_block_batch(self):
        for qty, price, divider in [(4, 2, 3), ("bad", 2, 3), (3, "NaN", 3), (3, 2, 0)]:
            with self.assertRaises(ValueError):
                convert_invoice(pd.DataFrame({"supplier_id": ["A"], "qty": [qty], "price": [price]}), {"A": divider}, "qty", "price")

    def test_returns_and_zero(self):
        for qty in (-12, 0):
            result, _ = convert_invoice(pd.DataFrame({"supplier_id": ["A"], "qty": [qty], "price": ["0.123456"]}), {"A": 3}, "qty", "price")
            self.assertEqual(result.loc[0, "qty"], Decimal(qty) / 3)


if __name__ == "__main__":
    unittest.main()
