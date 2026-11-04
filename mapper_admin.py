"""PIN gate and existing crosswalk management workflow."""

import os

import pandas as pd
import streamlit as st
from sqlalchemy import text

from bulk_import import render_bulk_import


def require_admin():
    expected_pin = str(os.getenv("ADMIN_PIN") or st.secrets.get("ADMIN_PIN", "")).strip()
    if expected_pin and st.session_state.get("admin_unlocked", False):
        st.caption("Admin unlocked for this session.")
        return

    with st.form("admin_unlock"):
        pin = st.text_input("Admin PIN", type="password")
        submitted = st.form_submit_button("Unlock", type="primary")
    if submitted and expected_pin and pin.strip() == expected_pin:
        st.session_state["admin_unlocked"] = True
        st.rerun()
    if submitted:
        st.error("The admin PIN was not accepted.")
    st.info("Enter the admin PIN to access this section.")
    st.stop()


def render_crosswalk_admin(engine, read_sql):
    st.markdown("### Add a single mapping")

    with st.form("admin_add_single"):
        c1, c2, c3 = st.columns(3)
        with c1:
            vendor_in = st.text_input("vendor_id (leave blank for GLOBAL)", value=st.session_state.get("vendor_prefill", ""))
        with c2:
            supp_in = st.text_input("supplier_id", value=st.session_state.get("supplier_prefill", ""))
        with c3:
            tow_in = st.text_input("tow_code", value=st.session_state.get("tow_prefill", ""))

        st.caption("Action…")
        action = st.radio(
            label="Action…",
            options=["Queue (downloadable CSV)", "Directly to DB (upsert)"],
            horizontal=True,
            key="admin_add_action"
        )

        submitted = st.form_submit_button("Add")
        if submitted:
            v = (vendor_in or "").strip()
            s = (supp_in or "").strip()
            t = (tow_in or "").strip()

            if not s:
                st.error("supplier_id is required.")
            else:
                if action.startswith("Queue"):
                    queue_cols = ["vendor_id", "supplier_id", "tow_code"]
                    if "updates_df" not in st.session_state:
                        st.session_state["updates_df"] = pd.DataFrame(columns=queue_cols)
                    st.session_state["updates_df"] = pd.concat(
                        [st.session_state["updates_df"], pd.DataFrame([{"vendor_id": v, "supplier_id": s, "tow_code": t}])],
                        ignore_index=True
                    )
                    st.success("Queued. (updates.csv u dnu sekcije)")
                else:
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO crosswalk (vendor_id, supplier_id, tow_code)
                                VALUES (:v, :s, :t)
                                ON CONFLICT (vendor_id, supplier_id)
                                DO UPDATE SET tow_code = EXCLUDED.tow_code
                            """), {"v": v, "s": s, "t": t})
                        st.success("Upsert OK.")
                    except Exception as e:
                        st.error(f"DB error: {e}")

    render_bulk_import(engine, "mapping")

    st.markdown("### Apply queued CSV to DB")

    if "updates_df" in st.session_state and not st.session_state["updates_df"].empty:
        dfq = st.session_state["updates_df"].copy()
        st.dataframe(dfq, use_container_width=True, height=200)

        qcsv = dfq.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download updates.csv",
            data=qcsv,
            file_name="updates.csv",
            mime="text/csv",
            key="dl_updates_csv"
        )

        c_apply1, c_apply2 = st.columns([1, 1])
        with c_apply1:
            if st.button("Apply updates.csv to DB", key="btn_apply_updates"):
                try:
                    with engine.begin() as conn:
                        for _, r in dfq.iterrows():
                            conn.execute(text("""
                                INSERT INTO crosswalk (vendor_id, supplier_id, tow_code)
                                VALUES (:v, :s, :t)
                                ON CONFLICT (vendor_id, supplier_id)
                                DO UPDATE SET tow_code = EXCLUDED.tow_code
                            """), {
                                "v": str(r.get("vendor_id", "")),
                                "s": str(r.get("supplier_id", "")),
                                "t": str(r.get("tow_code", "")),
                            })
                    st.success("updates.csv applied to DB.")
                except Exception as e:
                    st.error(f"Apply failed: {e}")
        with c_apply2:
            if st.button("Clear queued items", key="btn_clear_updates"):
                st.session_state["updates_df"] = pd.DataFrame(columns=["vendor_id", "supplier_id", "tow_code"])
                st.info("Queue cleared.")
    else:
        st.caption("No updates.csv yet.")

    st.markdown("### Live search / inspect")

    c_f1, c_f2, c_f3 = st.columns([2.2, 2.2, 1.2])
    with c_f1:
        vendor_filter_live = st.text_input("vendor_id filter (blank = ALL)", value=st.session_state.get("ls_vendor", ""))
    with c_f2:
        supp_filter_live = st.text_input("supplier_id search (exact or contains)", value=st.session_state.get("ls_supplier", ""))
    with c_f3:
        exact = st.checkbox("Exact supplier match", value=st.session_state.get("ls_exact", False))

    st.session_state["ls_vendor"] = vendor_filter_live
    st.session_state["ls_supplier"] = supp_filter_live
    st.session_state["ls_exact"] = exact

    clauses, params = [], {}
    if vendor_filter_live != "":
        clauses.append("c.vendor_id = :v1")
        params["v1"] = vendor_filter_live
    if supp_filter_live != "":
        if exact:
            clauses.append("c.supplier_id = :s1")
            params["s1"] = supp_filter_live
        else:
            clauses.append("c.supplier_id ILIKE :s1")
            params["s1"] = f"%{supp_filter_live}%"

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    q = f"""
        SELECT c.vendor_id, v.vendor_name, c.supplier_id, c.tow_code
        FROM crosswalk c
        LEFT JOIN vendors v ON v.vendor_id = c.vendor_id
        {where}
        ORDER BY c.vendor_id, c.supplier_id
        LIMIT 500
    """

    try:
        df_res = read_sql(q, params)
        st.caption(f"500 results shown (max 500) — shown: {len(df_res):,}")
        st.dataframe(df_res, use_container_width=True, height=260)

        st.markdown("##### Pick row as prefill")
        c_idx, c_btn = st.columns([1, 2])
        with c_idx:
            idx = st.number_input(" ", min_value=0, max_value=max(len(df_res)-1, 0), value=0, step=1, label_visibility="collapsed")
        with c_btn:
            if st.button("Prefill Admin form from row", key="btn_prefill"):
                if not df_res.empty:
                    row = df_res.iloc[int(idx)]
                    st.session_state["vendor_prefill"] = str(row.get("vendor_id", "") or "")
                    st.session_state["supplier_prefill"] = str(row.get("supplier_id", "") or "")
                    st.session_state["tow_prefill"] = str(row.get("tow_code", "") or "")
                    st.success("Prefilled. Review the values in Add a single mapping before saving.")
    except Exception as e:
        st.error(f"Search failed: {e}")
