# Supplier → TOW Mapper (Cloud DB)

Map supplier invoice product codes to internal **TOW** codes using a **persistent PostgreSQL (Neon)** database.  
All inserts/updates survive Streamlit restarts.

---

## 🔧 Setup (local)

```cmd
cd C:\Users\mkozi\OneDrive\Desktop\tow-mapper-cloud
py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
