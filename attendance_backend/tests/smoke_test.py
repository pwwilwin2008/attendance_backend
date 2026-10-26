import os, sys, io, base64
import tempfile
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///" + tempfile.mkdtemp() + "/test.db"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from fastapi.testclient import TestClient
from PIL import Image
import numpy as np
import main

def img(seed):
    rng = np.random.default_rng(seed)
    a = (rng.random((64, 64, 3)) * 255).astype("uint8")
    b = io.BytesIO(); Image.fromarray(a).save(b, "PNG"); return base64.b64encode(b.getvalue()).decode()

ok = True
def check(label, cond, extra=""):
    global ok; ok &= bool(cond); print(("PASS " if cond else "FAIL ") + label, extra)

with TestClient(main.app) as c:
    H = lambda t: {"Authorization": f"Bearer {t}"}
    # --- admin auth
    r = c.get("/api/v1/staff"); check("no token -> 401", r.status_code == 401)
    r = c.post("/api/v1/tenant/auth", json={"tenantName": "Acme College", "adminEmail": "boss@acme.com", "password": "short", "isLogin": False}); check("weak password rejected", r.status_code == 400)
    r = c.post("/api/v1/tenant/auth", json={"tenantName": "Acme College", "adminEmail": "boss@acme.com", "password": "Str0ngPass!", "isLogin": False, "category": "College"}); check("register", r.status_code == 200); adm = r.json()["token"]
    r = c.post("/api/v1/tenant/auth", json={"adminEmail": "boss@acme.com", "password": "wrong", "isLogin": True}); check("wrong password -> 401", r.status_code == 401)
    r = c.post("/api/v1/tenant/auth", json={"adminEmail": "boss@acme.com", "password": "Str0ngPass!", "isLogin": True}); check("login", r.status_code == 200 and r.json()["tenantName"] == "Acme College")
    # second tenant
    r = c.post("/api/v1/tenant/auth", json={"tenantName": "Other Co", "adminEmail": "x@other.com", "password": "An0therPass!", "isLogin": False}); adm2 = r.json()["token"]
    # --- enroll
    ref = img(1)
    r = c.post("/api/v1/staff", headers=H(adm), json={"staffId": "s1", "name": "Sam", "salary": 26000, "photoData": ref}); check("enroll", r.status_code == 200); pin = r.json()["pin"]; check("pin is 6 digits", len(pin) == 6)
    r = c.get("/api/v1/staff", headers=H(adm)); check("list has hasPin", r.json()[0]["hasPin"] is True)
    r = c.get("/api/v1/staff", headers=H(adm2)); check("other tenant sees none", r.json() == [])
    r = c.delete("/api/v1/staff/S1", headers=H(adm2)); check("other tenant cannot delete", r.status_code == 404)
    # --- employee sign-in
    r = c.post("/api/v1/employee/verify-code", json={"tenantName": "acme college", "staffId": "s1", "pin": "000000"}); check("wrong PIN -> 401", r.status_code == 401)
    r = c.post("/api/v1/employee/verify-code", json={"tenantName": "acme college", "staffId": "s1", "pin": pin}); check("right PIN", r.status_code == 200); pend = r.json()["pendingToken"]
    check("no photo leaked", "photoData" not in r.text)
    r = c.post("/api/v1/attendance/punch", headers=H(pend), json={"type": "PUNCH_IN"}); check("pending token cannot punch", r.status_code == 403)
    r = c.post("/api/v1/auth/verify-face", headers=H(pend), json={"livePhoto": img(99)}); check("different face rejected", r.status_code == 401, r.json().get("similarity"))
    r = c.post("/api/v1/auth/verify-face", headers=H(pend), json={"livePhoto": ref}); check("same face accepted", r.status_code == 200); emp = r.json()["token"]
    r = c.get("/api/v1/staff", headers=H(emp)); check("employee token cannot use admin API", r.status_code == 403)
    # --- employee actions
    r = c.post("/api/v1/attendance/punch", headers=H(emp), json={"type": "PUNCH_OUT", "report": "x"}); check("punch out first -> 409", r.status_code == 409)
    r = c.post("/api/v1/attendance/punch", headers=H(emp), json={"type": "PUNCH_IN"}); check("punch in", r.status_code == 200)
    r = c.post("/api/v1/telemetry/ping", headers=H(emp), json={"latitude": 10.0, "longitude": 76.0}); check("telemetry", r.status_code == 200)
    r = c.post("/api/v1/attendance/punch", headers=H(emp), json={"type": "PUNCH_OUT", "report": "done"}); check("punch out", r.status_code == 200)
    r = c.get("/api/v1/me/attendance", headers=H(emp)); check("my history", len(r.json()) == 2)
    r = c.post("/api/v1/leaves/apply", headers=H(emp), json={"date": "2026-10-20", "reason": "family"}); check("apply leave", r.status_code == 200); lid = r.json()["leaveId"]
    r = c.post("/api/v1/leaves/apply", headers=H(emp), json={"date": "bad", "reason": "x"}); check("bad leave date -> 400", r.status_code == 400)
    r = c.get("/api/v1/me/leaves", headers=H(emp)); check("my leaves", r.json()[0]["status"] == "PENDING")
    # --- admin actions
    r = c.get("/api/v1/admin/audit-feed", headers=H(adm)); check("feed", len(r.json()["attendance"]) == 2 and len(r.json()["leaveRequests"]) == 1)
    r = c.get("/api/v1/admin/audit-feed", headers=H(adm2)); check("feed isolation", r.json()["attendance"] == [])
    r = c.post("/api/v1/admin/leave/action", headers=H(adm2), json={"leaveId": lid, "status": "APPROVED"}); check("other admin cannot approve", r.status_code == 404)
    r = c.post("/api/v1/admin/leave/action", headers=H(adm), json={"leaveId": lid, "status": "APPROVED"}); check("approve", r.status_code == 200)
    r = c.get("/api/v1/me/leaves", headers=H(emp)); check("employee sees APPROVED", r.json()[0]["status"] == "APPROVED")
    r = c.get("/api/v1/admin/payroll?month=2026-10", headers=H(adm)); j = r.json(); print("   payroll:", j["workingDays"], j["staff"][0])
    check("payroll", r.status_code == 200 and j["staff"][0]["approvedLeaveDays"] == 1 and j["staff"][0]["presentDays"] in (0, 1))
    r = c.post("/api/v1/admin/tenant/saturday-policy", headers=H(adm), json={"saturdayPolicy": "ALL_OFF"}); check("policy set", r.status_code == 200)
    r = c.get("/api/v1/admin/payroll?month=2026-10", headers=H(adm)); print("   ALL_OFF working days:", r.json()["workingDays"])
    r = c.post("/api/v1/staff/S1/reset-pin", headers=H(adm)); check("reset pin", r.status_code == 200 and r.json()["pin"] != pin)
    r = c.post("/api/v1/employee/verify-code", json={"tenantName": "acme college", "staffId": "s1", "pin": pin}); check("old PIN stops working", r.status_code == 401)
    for _ in range(5): c.post("/api/v1/employee/verify-code", json={"tenantName": "acme college", "staffId": "s1", "pin": "111111"})
    r = c.post("/api/v1/employee/verify-code", json={"tenantName": "acme college", "staffId": "s1", "pin": "111111"}); check("lockout after repeated failures -> 429", r.status_code == 429)
    r = c.get("/api/v1/tenant/get-media?tenantName=acme college"); check("public branding", r.status_code == 200)
print("ALL PASS" if ok else "SOME FAILED")
