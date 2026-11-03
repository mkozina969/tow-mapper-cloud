from pathlib import Path
import pandas as pd
import streamlit as st
from sqlalchemy import text


def render_uom_admin(engine, vendor):
    with st.expander("UoM Conversion — manage packaging rules"):
        if st.button("Initialize UoM table"):
            with engine.begin() as conn:
                conn.execute(text(Path(__file__).with_name("migrations").joinpath("001_item_uom_conversion.sql").read_text()))
            st.success("Conversion table ready.")
        with st.form("uom_rule"):
            rule_vendor = st.text_input("Conversion vendor_id", value=vendor).strip()
            supplier = st.text_input("Conversion supplier_id").strip()
            divider = st.number_input("PCS per SET / divider", min_value=1, max_value=2147483647, value=1, step=1)
            active = st.checkbox("Conversion active", value=True)
            if st.form_submit_button("Save rule"):
                if not rule_vendor or not supplier:
                    st.error("Vendor and supplier item are required. GLOBAL conversion rules are not supported.")
                else:
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("""INSERT INTO item_uom_conversion (vendor_id, supplier_id, divider, active)
                                VALUES (:v, :s, :d, :a)
                                ON CONFLICT (vendor_id, supplier_id) DO UPDATE
                                SET divider = EXCLUDED.divider, active = EXCLUDED.active, updated_at = NOW()"""),
                                {"v": rule_vendor, "s": supplier, "d": int(divider), "a": active})
                        st.session_state["mapped_ready"] = False
                        st.success("Saved. Run mapping again using the original invoice.")
                    except Exception as exc:
                        st.error(f"Save failed: {exc}")
        st.caption("To edit or disable a rule, enter the same vendor and supplier item and save the new values.")
        try:
            with engine.connect() as conn:
                rules = pd.read_sql(text("SELECT vendor_id, supplier_id, divider, active FROM item_uom_conversion WHERE vendor_id = :v ORDER BY supplier_id"), conn, params={"v": vendor})
            st.dataframe(rules, use_container_width=True)
        except Exception:
            st.info("Initialize the conversion table before mapping invoices.")
