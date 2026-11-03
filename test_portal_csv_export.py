import csv
from decimal import Decimal
from io import StringIO
import unittest
import pandas as pd
from portal_csv_export import portal_csv


class PortalCsvTests(unittest.TestCase):
    def frame(self):
        return pd.DataFrame([["00123", "Invoice", "Item", "", "9A84E3", Decimal("2"), Decimal("20.51"), "27.05.2026", "H00", "DOB0003078"]])

    def test_reference_layout_and_no_header(self):
        self.assertEqual(portal_csv(self.frame()), b"00123;Invoice;Item;;9A84E3;2;20,51;27.5.2026.;H00;DOB0003078\r\n")

    def test_quoting(self):
        frame = self.frame()
        frame.iloc[0, 0] = 'INV;"123'
        rows = list(csv.reader(StringIO(portal_csv(frame).decode()), delimiter=";"))
        self.assertEqual(len(rows[0]), 10)
        self.assertEqual(rows[0][0], 'INV;"123')

    def test_invalid_inputs(self):
        for column, value in [(3, "not blank"), (5, "bad"), (6, "NaN"), (7, "invalid"), (0, "A\nB")]:
            frame = self.frame()
            frame.iloc[0, column] = value
            with self.subTest(column=column), self.assertRaises(ValueError):
                portal_csv(frame)
        with self.assertRaises(ValueError):
            portal_csv(self.frame().iloc[:, :9])


if __name__ == "__main__":
    unittest.main()
