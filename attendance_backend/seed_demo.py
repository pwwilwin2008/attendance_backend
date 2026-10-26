"""Creates two demo organizations (2 admins, 2 employees) in the local database. Safe to re-run.
Usage:  python seed_demo.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from fastapi.testclient import TestClient
import sqlalchemy as sa
import asyncio
import main

DEMO = [
    {"org": "Alpha Tech", "category": "Company", "email": "alpha.admin@demo.com", "password": "Alpha@12345",
     "emp": {"code": "ALP001", "name": "Anil Kumar", "role": "Field Executive", "pin": "111111", "salary": 30000}},
    {"org": "Beta College", "category": "College", "email": "beta.admin@demo.com", "password": "Beta@12345",
     "emp": {"code": "BET001", "name": "Bina Thomas", "role": "Lecturer", "pin": "222222", "salary": 45000}},
]

async def force_pin(staff_id, pin):
    async with main.AsyncSessionLocal() as db:
        st = (await db.execute(sa.select(main.StaffPersonnel).where(main.StaffPersonnel.staff_id == staff_id))).scalar_one()
        st.pin_hash = main.hash_secret(pin)
        await db.commit()

with TestClient(main.app) as c:
    for d in DEMO:
        r = c.post("/api/v1/tenant/auth", json={"tenantName": d["org"], "adminEmail": d["email"], "password": d["password"],
                                                 "category": d["category"], "isLogin": False})
        if r.status_code != 200:  # already exists -> log in instead
            r = c.post("/api/v1/tenant/auth", json={"adminEmail": d["email"], "password": d["password"], "isLogin": True})
        assert r.status_code == 200, r.text
        tok = r.json()["token"]
        e = d["emp"]
        r = c.post("/api/v1/staff", headers={"Authorization": f"Bearer {tok}"},
                   json={"staffId": e["code"], "name": e["name"], "role": e["role"], "salary": e["salary"],
                         "shiftStart": "09:00 AM", "shiftEnd": "05:00 PM"})
        assert r.status_code in (200, 400), r.text   # 400 = already enrolled
        asyncio.run(force_pin(f"{d['org'].lower()}__{e['code']}", e["pin"]))
print("Demo accounts ready.")
