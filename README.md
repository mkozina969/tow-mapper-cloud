# Supplier → TOW Mapper (Cloud DB)

Map supplier codes to internal TOW codes using a **persistent cloud database** (PostgreSQL).  
Inserts survive Streamlit Cloud restarts.

## 1) Run locally
```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows
# source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
# Set DATABASE_URL (env) or create .streamlit/secrets.toml (see below)
streamlit run streamlit_app.py
