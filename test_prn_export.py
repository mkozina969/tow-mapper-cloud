import unittest
from decimal import Decimal
import pandas as pd
from prn_export import pallets_prn, regular_prn


class PalletsPrnTests(unittest.TestCase):
    def test_exact_sample_layout(self):
        df = pd.DataFrame({"tow": ["G08V6T", "A6755A"], "qty": [4, 23], "pallet": ["37417912001", "37417912001"]})
        expected = b"G08V6T              4                             37417912001\r\nA6755A              23                            37417912001\r\n"
        self.assertEqual(pallets_prn(df, "tow", "qty", "pallet"), expected)

    def test_converted_decimal_and_text_codes(self):
        df = pd.DataFrame({"tow": ["001234"], "qty": [Decimal("4.000")], "pallet": ["00123456789"]})
        line = pallets_prn(df, "tow", "qty", "pallet").decode("ascii")
        self.assertEqual(line[:20], "001234".ljust(20))
        self.assertEqual(line[20:50], "4".ljust(30))
        self.assertEqual(line[50:], "00123456789\r\n")

    def test_invalid_values_rejected(self):
        for tow, qty, pallet in [("A" * 21, 4, "123"), (None, 4, "123"), ("A", "bad", "123"), ("A", "NaN", "123"), ("A", 4, "1" * 12), ("A", 4, "A\nB")]:
            with self.subTest(tow=tow, qty=qty, pallet=pallet), self.assertRaises(ValueError):
                pallets_prn(pd.DataFrame({"tow": [tow], "qty": [qty], "pallet": [pallet]}), "tow", "qty", "pallet")


class RegularPrnTests(unittest.TestCase):
    def test_widths_order_no_header_and_decimals(self):
        df = pd.DataFrame({"code": ["001234"], "qty": [Decimal("4.000")], "price": [Decimal("12.50")]})
        data = regular_prn(df, "code", "qty", "price")
        self.assertEqual(data, b"001234    " + b"4              " + b"12.5           " + b"\r\n")
        self.assertEqual(len(data), 42)
        self.assertEqual(regular_prn(df, "price", "code", "qty")[:10], b"12.5      ")

    def test_overflow_missing_and_controls(self):
        for values in [("X" * 11, "Q", "P"), ("X", "Q" * 16, "P"), ("X", "Q", "P" * 16), ("X", None, "P"), ("X", "Q", "A\nB")]:
            with self.subTest(values=values), self.assertRaises(ValueError):
                regular_prn(pd.DataFrame([values], columns=["a", "b", "c"]), "a", "b", "c")


if __name__ == "__main__":
    unittest.main()
