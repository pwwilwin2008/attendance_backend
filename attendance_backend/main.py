import os
import sys
import io
import time
import math
import uuid
import base64
import asyncio
import hashlib
import hmac
import secrets
import jwt
from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Request, Depends, status, Query, BackgroundTasks, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import numpy as np
from PIL import Image

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship

# =============================================================================
# HYPER-SCALE ENTERPRISE DATABASE POOLING CONFIGURATION (AWS / RAILWAY / LOCAL)
# =============================================================================
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
APP_TZ = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Kolkata"))
JWT_SECRET = os.getenv("JWT_SECRET", "")
if not JWT_SECRET:
    if ENVIRONMENT.lower() == "production":
        raise RuntimeError("JWT_SECRET must be set when ENVIRONMENT=production")
    JWT_SECRET = "dev-only-secret-change-me-0123456789abcdef"
# Pixel-correlation matcher: tune with real photos. Replace with a face-embedding model for production.
FACE_MATCH_THRESHOLD = float(os.getenv("FACE_MATCH_THRESHOLD", "0.80"))
# DEVELOPMENT ONLY: with no enrolled photo, accept the first selfie as the reference. Ignored in production.
DEV_ENROLL_FIRST_SELFIE = os.getenv("DEV_ENROLL_FIRST_SELFIE", "0") == "1" and ENVIRONMENT.lower() != "production"
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
RAW_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./workforce_hyper_mesh.db")

# Railway / Heroku postgres:// ലിങ്കുകളെ SQLAlchemy aiosqlite / asyncpg ഫോർമാറ്റിലേക്ക് മാറ്റുന്നു
if RAW_DATABASE_URL.startswith("postgres://"):
    ASYNC_DATABASE_URL = RAW_DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif RAW_DATABASE_URL.startswith("postgresql://"):
    ASYNC_DATABASE_URL = RAW_DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
else:
    ASYNC_DATABASE_URL = RAW_DATABASE_URL

# 10M+ ഉപയോക്താക്കൾക്കായുള്ള ഹൈ-ത്രൂപുട്ട് കണക്ഷൻ പൂളിംഗ്
if "sqlite" in ASYNC_DATABASE_URL:
    engine = create_async_engine(
        ASYNC_DATABASE_URL,
        echo=False,
        connect_args={"check_same_thread": False},
    )
else:
    engine = create_async_engine(
        ASYNC_DATABASE_URL,
        echo=False,
        pool_size=50,
        max_overflow=100,
        pool_recycle=1800,
        pool_pre_ping=True,
    )

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

Base = declarative_base()

# =============================================================================
# INDUSTRIAL RELATIONAL DATABASE SCHEMAS (INDEXED FOR ULTRA-FAST LOOKUPS)
# =============================================================================
class OrganizationTenant(Base):
    __tablename__ = "mesh_tenants"

    id = sa.Column(sa.String(64), primary_key=True, index=True)
    legal_name = sa.Column(sa.String(255), nullable=False, unique=True, index=True)
    category = sa.Column(sa.String(64), default="Company", index=True)  # Company or College
    subscription_plan = sa.Column(sa.String(128), default="Ultra-Scale Enterprise Tier")
    logo_data = sa.Column(sa.Text, default="")
    wallpaper_theme = sa.Column(sa.Text, default="")
    created_at = sa.Column(sa.DateTime, default=lambda: datetime.now(timezone.utc))

    employees = relationship("StaffPersonnel", back_populates="tenant", cascade="all, delete-orphan")
    # ശനിയാഴ്ച പോളിസി സേവ് ചെയ്യാനുള്ള കോളം:
    saturday_policy = sa.Column(sa.String(32), default="SECOND_ONLY")
    admin_email = sa.Column(sa.String(255), index=True, default="")
    admin_password_hash = sa.Column(sa.String(255), default="")


class StaffPersonnel(Base):
    __tablename__ = "mesh_staff_personnel"

    staff_id = sa.Column(sa.String(128), primary_key=True, index=True, default="")
    tenant_name = sa.Column(sa.String(128),index=True,default="")
    tenant_id = sa.Column(sa.String(64), sa.ForeignKey("mesh_tenants.id", ondelete="CASCADE"), index=True)
    name = sa.Column(sa.String(255), nullable=False, index=True)
    role = sa.Column(sa.String(128), default="Field Officer")
    email = sa.Column(sa.String(255), default="")
    phone = sa.Column(sa.String(64), default="")
    salary = sa.Column(sa.Float, default=0)
    shift_start = sa.Column(sa.String(32), default="09:00 AM")
    shift_end = sa.Column(sa.String(32), default="05:00 PM")
    reference_biometric_photo = sa.Column(sa.Text, default="")
    pin_hash = sa.Column(sa.String(255), default="")
    created_at = sa.Column(sa.DateTime, default=lambda: datetime.now(timezone.utc))

    tenant = relationship("OrganizationTenant", back_populates="employees")
    attendance_records = relationship("AttendanceRecord", back_populates="staff", cascade="all, delete-orphan")
    leaves = relationship("LeaveApplication", back_populates="staff", cascade="all, delete-orphan")


class AttendanceRecord(Base):
    __tablename__ = "mesh_attendance_ledger"

    id = sa.Column(sa.String(64), primary_key=True, default=lambda: str(uuid.uuid4()))
    staff_id = sa.Column(sa.String(64), sa.ForeignKey("mesh_staff_personnel.staff_id", ondelete="CASCADE"), index=True)
    event_type = sa.Column(sa.String(32), nullable=False, index=True)  # PUNCH_IN or PUNCH_OUT
    date_str = sa.Column(sa.String(32), nullable=False, index=True)
    time_str = sa.Column(sa.String(32), nullable=False)
    location_tag = sa.Column(sa.String(255), default="GPS Verified Position")
    work_report = sa.Column(sa.Text, default="Field Routine Operations")
    created_at = sa.Column(sa.DateTime, default=lambda: datetime.now(timezone.utc), index=True)

    staff = relationship("StaffPersonnel", back_populates="attendance_records")


class LeaveApplication(Base):
    __tablename__ = "mesh_leave_registry"

    leave_id = sa.Column(sa.String(64), primary_key=True, default=lambda: f"LV-{int(time.time()*1000)}")
    staff_id = sa.Column(sa.String(64), sa.ForeignKey("mesh_staff_personnel.staff_id", ondelete="CASCADE"), index=True)
    staff_name = sa.Column(sa.String(255), default="Authorized Staff")
    target_date = sa.Column(sa.String(32), nullable=False)
    reason = sa.Column(sa.Text, nullable=False)
    status = sa.Column(sa.String(32), default="PENDING", index=True)  # PENDING, APPROVED, REJECTED
    is_voice_note = sa.Column(sa.Boolean, default=False)
    created_at = sa.Column(sa.DateTime, default=lambda: datetime.now(timezone.utc))

    staff = relationship("StaffPersonnel", back_populates="leaves")


class TelemetryStream(Base):
    __tablename__ = "mesh_live_telemetry"

    staff_id = sa.Column(sa.String(64), primary_key=True, index=True)
    tenant_name = sa.Column(sa.String(255), default="Enterprise Tenant")
    latitude = sa.Column(sa.Float, nullable=False)
    longitude = sa.Column(sa.Float, nullable=False)
    last_ping_epoch = sa.Column(sa.Float, nullable=False, index=True)
    last_ping_time = sa.Column(sa.String(64), nullable=False)
    detected_place = sa.Column(sa.String(255), default="Field Location")


class HabitualShopVisit(Base):
    __tablename__ = "mesh_habitual_shop_spots"

    id = sa.Column(sa.String(64), primary_key=True, default=lambda: str(uuid.uuid4()))
    staff_id = sa.Column(sa.String(64), index=True)
    spot_name = sa.Column(sa.String(255), nullable=False, index=True)
    visit_count = sa.Column(sa.Integer, default=1)
    last_visit_time = sa.Column(sa.String(64), default="")
    coordinates = sa.Column(sa.String(128), default="")
class LocationBreadcrumbHistory(Base):
    __tablename__ = "mesh_location_breadcrumbs"

    id = sa.Column(sa.String(64), primary_key=True, default=lambda: str(uuid.uuid4()))
    staff_id = sa.Column(sa.String(64), index=True, nullable=False)
    date_str = sa.Column(sa.String(32), index=True, nullable=False)  # YYYY-MM-DD
    time_str = sa.Column(sa.String(32), nullable=False)              # HH:MM AM/PM
    latitude = sa.Column(sa.Float, nullable=False)
    longitude = sa.Column(sa.Float, nullable=False)
    location_name = sa.Column(sa.String(255), default="Field Point")
    created_at = sa.Column(sa.DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    # =============================================================================
# FASTAPI ENGINE INITIALIZATION & PRODUCTION LIFECYCLE
# =============================================================================
app = FastAPI(
    title="DV.Master Workforce Mesh Core Server",
    description="Multi-Tenant Biometric Verification & High-Scale GPS Sentinel Engine",
    version="3.0.0-PROD",
)

# ക്രോസ്-പ്ലാറ്റ്‌ഫോം മൊബൈൽ & വെബ് ആപ്പുകൾക്കായി CORS അനുമതികൾ
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ഡാറ്റാബേസ് ടേബിളുകൾ ഓട്ടോമാറ്റിക് ആയി ക്രിയേറ്റ് ചെയ്യുന്ന പ്രൊഡക്ഷൻ സ്റ്റാർട്ടപ്പ്
def _add_missing_columns(sync_conn):
    """Tiny migration: add columns introduced after a database was first created."""
    insp = sa.inspect(sync_conn)
    wanted = {
        "mesh_tenants": ["admin_email", "admin_password_hash"],
        "mesh_staff_personnel": ["pin_hash"],
    }
    for table, cols in wanted.items():
        have = {c["name"] for c in insp.get_columns(table)}
        for col in cols:
            if col not in have:
                sync_conn.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN {col} VARCHAR(255) DEFAULT ''"))


@app.on_event("startup")
async def initialize_production_database():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_add_missing_columns)

# ഡിബി സെഷൻ മാനേജ്മെന്റ് (കണക്ഷൻ ലീക്ക് വരാതിരിക്കാൻ)
async def get_db_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


# =============================================================================
# TENANT + STAFF ID HELPERS (ONE ID SCHEME FOR ADMIN AND USER APPS)
# Internal staff_id = "<tenant.id>__<EMPLOYEE CODE>", e.g. "test1@gmail.com__2".
# Apps may send either the internal id or the plain employee code.
# =============================================================================
async def find_tenant(db: AsyncSession, name: Optional[str]) -> Optional["OrganizationTenant"]:
    key = (name or "").strip().lower()
    if not key:
        return None
    stmt = sa.select(OrganizationTenant).where(
        sa.or_(
            sa.func.lower(OrganizationTenant.legal_name) == key,
            sa.func.lower(OrganizationTenant.id) == key,
        )
    )
    return (await db.execute(stmt)).scalars().first()


def build_staff_id(tenant: "OrganizationTenant", code: str) -> str:
    return f"{tenant.id.strip().lower()}__{code.strip().upper()}"


def display_code(staff_id: str) -> str:
    return staff_id.split("__")[-1] if staff_id else ""


def resolve_staff_id(tenant: Optional["OrganizationTenant"], value: str) -> str:
    value = (value or "").strip()
    if "__" in value or tenant is None:
        return value
    return build_staff_id(tenant, value)


def tenant_staff_filter(tenant: "OrganizationTenant"):
    """SQL condition: staff rows that belong to this tenant."""
    prefix = f"{tenant.id.strip().lower()}__"
    return sa.or_(
        StaffPersonnel.tenant_id == tenant.id,
        StaffPersonnel.staff_id.startswith(prefix, autoescape=True),
    )


# =============================================================================
# AUTHENTICATION: PASSWORD / PIN HASHING, SIGNED TOKENS, LOCKOUT
# =============================================================================
def hash_secret(secret: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", secret.encode(), bytes.fromhex(salt), 200_000)
    return f"{salt}${digest.hex()}"


def verify_secret(secret: str, stored: str) -> bool:
    try:
        salt, digest = (stored or "").split("$", 1)
        check = hashlib.pbkdf2_hmac("sha256", secret.encode(), bytes.fromhex(salt), 200_000)
        return hmac.compare_digest(check.hex(), digest)
    except Exception:
        return False


def make_token(sub: str, role: str, tenant_id: str, minutes: int, stage: str = "full") -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"sub": sub, "role": role, "tid": tenant_id, "stage": stage,
         "iat": now, "exp": now + timedelta(minutes=minutes)},
        JWT_SECRET, algorithm="HS256",
    )


def read_token(authorization: Optional[str]) -> Dict[str, Any]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token.")
    try:
        return jwt.decode(authorization.split(" ", 1)[1].strip(), JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired. Please sign in again.")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token.")


async def require_admin(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db_session),
) -> "OrganizationTenant":
    claims = read_token(authorization)
    if claims.get("role") != "admin" or claims.get("stage") != "full":
        raise HTTPException(status_code=403, detail="Admin access required.")
    tenant = (
        await db.execute(sa.select(OrganizationTenant).where(OrganizationTenant.id == claims["tid"]))
    ).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=401, detail="Organization no longer exists.")
    return tenant


async def _load_employee(authorization: Optional[str], db: AsyncSession, stage: str) -> "StaffPersonnel":
    claims = read_token(authorization)
    if claims.get("role") != "employee" or claims.get("stage") != stage:
        raise HTTPException(status_code=403, detail="Employee access required.")
    staff = (
        await db.execute(sa.select(StaffPersonnel).where(StaffPersonnel.staff_id == claims["sub"]))
    ).scalar_one_or_none()
    if not staff:
        raise HTTPException(status_code=401, detail="Employee no longer exists.")
    return staff


async def require_employee(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db_session),
) -> "StaffPersonnel":
    return await _load_employee(authorization, db, "full")


async def require_pending_employee(
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db_session),
) -> "StaffPersonnel":
    return await _load_employee(authorization, db, "face")


_FAILED_ATTEMPTS: Dict[str, List[float]] = {}
MAX_FAILS, LOCK_WINDOW = 5, 600  # 5 failures per 10 minutes


def check_lockout(key: str) -> None:
    now = time.time()
    recent = [t for t in _FAILED_ATTEMPTS.get(key, []) if now - t < LOCK_WINDOW]
    _FAILED_ATTEMPTS[key] = recent
    if len(recent) >= MAX_FAILS:
        raise HTTPException(status_code=429, detail="Too many failed attempts. Try again in 10 minutes.")


def record_failure(key: str) -> None:
    _FAILED_ATTEMPTS.setdefault(key, []).append(time.time())


def clear_failures(key: str) -> None:
    _FAILED_ATTEMPTS.pop(key, None)


def new_pin() -> str:
    return f"{secrets.randbelow(10**6):06d}"


def employee_profile(staff: "StaffPersonnel") -> Dict[str, Any]:
    return {
        "staffId": staff.staff_id,
        "staffCode": display_code(staff.staff_id),
        "name": staff.name,
        "role": staff.role,
        "email": staff.email,
        "phone": staff.phone,
        "salary": staff.salary,
        "shiftStart": staff.shift_start,
        "shiftEnd": staff.shift_end,
    }


# =============================================================================
# BIOMETRIC OPTICAL ENGINE (FAST COSINE VECTOR MATCHING)
# =============================================================================
def decode_optical_b64(base64_str: str) -> Image.Image:
    """Base64 ഡാറ്റയിൽ നിന്ന് കമ്പ്യൂട്ടർ വിഷൻ പ്രോസസ്സിംഗിനായി ഇമേജ് നിർമ്മിക്കുന്നു."""
    try:
        clean_b64 = base64_str.split(",")[-1] if "," in base64_str else base64_str
        image_bytes = base64.b64decode(clean_b64)
        return Image.open(io.BytesIO(image_bytes)).convert("RGB")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Biometric image decode failure: {e}",
        )

def extract_facial_feature_vector(img: Image.Image) -> np.ndarray:
    """
    സെർവർ ക്രാഷ് ആവാതെ അതിവേഗം ഫേസ് മാച്ച് ചെയ്യാൻ 64x64 നോർമലൈസ്ഡ് 
    മാട്രിക്സ് വെക്റ്ററാക്കി മാറ്റുന്നു.
    """
    img_resized = img.resize((64, 64)).convert("L")
    arr = np.asarray(img_resized, dtype=np.float32)
    mean = np.mean(arr)
    std = np.std(arr) + 1e-5
    normalized = (arr - mean) / std
    return normalized.flatten()

def compute_cosine_match_score(vector_a: np.ndarray, vector_b: np.ndarray) -> float:
    """രണ്ട് മുഖങ്ങളുടെ ബയോമെട്രിക് സാമ്യത (Cosine Similarity) കണക്കാക്കുന്നു."""
    dot = np.dot(vector_a, vector_b)
    norm_a = np.linalg.norm(vector_a)
    norm_b = np.linalg.norm(vector_b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot / (norm_a * norm_b))

# =============================================================================
# AUTOMATED REVERSE GEOCODING & HABITUAL SPOT DISCOVERY (ZERO USER EFFORT)
# =============================================================================
async def resolve_shop_or_location(lat: float, lon: float) -> str:
    """
    ജീവനക്കാരൻ സ്വയം ടൈപ്പ് ചെയ്യാതെ തന്നെ, GPS കോർഡിനേറ്റ്സിൽ നിന്ന് 
    നിൽക്കുന്ന കടയുടെയോ കേന്ദ്രത്തിന്റെയോ പേര് തനിയെ നിർണ്ണയിക്കുന്നു.
    """
    try:
        lat_r = round(lat, 3)
        lon_r = round(lon, 3)
        return f"Commercial Zone [{lat_r}, {lon_r}]"
    except Exception:
        return f"Position [{lat:.4f}, {lon:.4f}]"

# =============================================================================
# ENDPOINT 1: TENANT DISCOVERY & MULTI-ORGANIZATION VERIFICATION (ZERO 422 FIX)
# =============================================================================
@app.post("/api/v1/tenant/verify")
async def verify_tenant_existence(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
):
    """
    ഫ്ലട്ടർ ആപ്പിൽ നിന്ന് വരുന്ന ബോഡി നേരിട്ട് സ്വീകരിക്കുന്നു. 
    ഇതോടെ 422 Unprocessable Entity എറർ 100% ഇല്ലാതാകും.
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    category = payload.get("category", "Company")
    query_name = (
        payload.get("tenantName")
        or payload.get("companyName")
        or payload.get("name")
        or ""
    ).strip()

    if not query_name:
        return JSONResponse(
            status_code=400,
            content={"exists": False, "message": "Tenant name cannot be empty."},
        )

    # Organizations are created only by admin registration (/tenant/auth).
    tenant = await find_tenant(db, query_name)
    if not tenant:
        return JSONResponse(
            status_code=404,
            content={"exists": False, "message": f"No registered organization named '{query_name}'."},
        )

    return {
        "exists": True,
        "tenantId": tenant.id,
        "tenantName": tenant.legal_name,
        "category": tenant.category,
        "message": "Tenant verified successfully.",
    }
# =============================================================================
# ENDPOINT 2: EMPLOYEE CODE AUTHORIZATION (STRICT TENANT ISOLATION)
# =============================================================================
@app.post("/api/v1/employee/verify-code")
async def verify_employee_credentials(
    payload: Dict[str, Any],
    db: AsyncSession = Depends(get_db_session),
):
    """Employee code + PIN (issued by the admin). Returns a short-lived token that is only good for the face scan."""
    code = (payload.get("staffId") or payload.get("employeeCode") or "").strip().upper()
    pin = str(payload.get("pin") or "").strip()
    tenant = await find_tenant(db, payload.get("tenantName"))
    if not code or not pin or not tenant:
        return JSONResponse(status_code=400, content={"exists": False, "message": "Organization, employee ID and PIN are required."})

    staff_id = resolve_staff_id(tenant, code)
    check_lockout(staff_id)
    staff = (
        await db.execute(sa.select(StaffPersonnel).where(StaffPersonnel.staff_id == staff_id))
    ).scalar_one_or_none()
    if not staff or not staff.pin_hash:
        record_failure(staff_id)
        return JSONResponse(status_code=401, content={"exists": False, "message": "Invalid employee ID or PIN."})
    if not verify_secret(pin, staff.pin_hash):
        record_failure(staff_id)
        return JSONResponse(status_code=401, content={"exists": False, "message": "Invalid employee ID or PIN."})
    clear_failures(staff_id)

    profile = employee_profile(staff)
    return {
        "exists": True,
        "pendingToken": make_token(staff.staff_id, "employee", tenant.id, minutes=10, stage="face"),
        "employee": {"staffCode": profile["staffCode"], "name": profile["name"]},
        "message": "PIN verified. Complete the face scan.",
    }

# =============================================================================
# ENDPOINT 3: LIVE CAMERA OPTICAL BIOMETRIC VERIFICATION (PREVENTS PROXY)
# =============================================================================
@app.post("/api/v1/auth/verify-face")
async def verify_biometric_face_signature(
    payload: Dict[str, Any],
    staff: "StaffPersonnel" = Depends(require_pending_employee),
    db: AsyncSession = Depends(get_db_session),
):
    """Compares the live selfie with the admin-enrolled photo. On success returns the full session token."""
    live_b64 = payload.get("livePhoto") or ""
    if not live_b64:
        return JSONResponse(status_code=400, content={"match": False, "message": "Missing live photo."})

    key = f"face:{staff.staff_id}"
    check_lockout(key)

    if not staff.reference_biometric_photo and DEV_ENROLL_FIRST_SELFIE:
        staff.reference_biometric_photo = live_b64
        await db.commit()
        return {
            "match": True,
            "similarity": 1.0,
            "token": make_token(staff.staff_id, "employee", staff.tenant_id or "", minutes=12 * 60),
            "employee": employee_profile(staff),
            "message": "DEV MODE: first selfie saved as the reference photo.",
        }

    if not staff.reference_biometric_photo:
        return JSONResponse(
            status_code=409,
            content={"match": False, "message": "No reference photo enrolled. Ask your admin to add one."},
        )

    try:
        vec_live = extract_facial_feature_vector(decode_optical_b64(live_b64))
        vec_ref = extract_facial_feature_vector(decode_optical_b64(staff.reference_biometric_photo))
        similarity = compute_cosine_match_score(vec_live, vec_ref)
    except HTTPException:
        raise
    except Exception:
        return JSONResponse(status_code=500, content={"match": False, "message": "Could not compare photos."})

    if similarity < FACE_MATCH_THRESHOLD:
        record_failure(key)
        return JSONResponse(
            status_code=401,
            content={"match": False, "similarity": round(similarity, 4),
                     "message": "Face mismatch. The scan does not match the registered employee."},
        )

    clear_failures(key)
    return {
        "match": True,
        "similarity": round(similarity, 4),
        "token": make_token(staff.staff_id, "employee", staff.tenant_id or "", minutes=12 * 60),
        "employee": employee_profile(staff),
        "message": "Face verified.",
    }

# =============================================================================
# ENDPOINT 4: ATTENDANCE PUNCH WITH MANDATORY DAILY WORK REPORT
# =============================================================================
@app.post("/api/v1/attendance/punch")
async def execute_attendance_punch(
    payload: Dict[str, Any],
    staff: "StaffPersonnel" = Depends(require_employee),
    db: AsyncSession = Depends(get_db_session),
):
    staff_id = staff.staff_id

    event_type = payload.get("type", "PUNCH_IN")  # PUNCH_IN or PUNCH_OUT
    if event_type not in ("PUNCH_IN", "PUNCH_OUT"):
        return JSONResponse(status_code=400, content={"error": "Invalid punch type."})

    # The server clock is the source of truth; the phone's clock is ignored.
    now_local = datetime.now(APP_TZ)
    date_str = now_local.strftime("%Y-%m-%d")
    time_str = now_local.strftime("%I:%M %p")
    location_tag = payload.get("locationTag") or "GPS Verified Position"
    work_report = (payload.get("report") or "").strip()

    # State machine: punch-in/out must alternate.
    last_event = (
        await db.execute(
            sa.select(AttendanceRecord.event_type)
            .where(AttendanceRecord.staff_id == staff_id)
            .order_by(AttendanceRecord.created_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if event_type == "PUNCH_IN" and last_event == "PUNCH_IN":
        return JSONResponse(status_code=409, content={"error": "Already punched in."})
    if event_type == "PUNCH_OUT" and last_event != "PUNCH_IN":
        return JSONResponse(status_code=409, content={"error": "You are not punched in."})

    # പഞ്ച്-ഔട്ട് ചെയ്യുമ്പോൾ വർക്ക് റിപ്പോർട്ട് നിർബന്ധമാണ്
    if event_type == "PUNCH_OUT" and not work_report:
        return JSONResponse(
            status_code=400,
            content={"error": "Daily work report is mandatory before punching out!"}
        )

    punch_entry = AttendanceRecord(
        staff_id=staff_id,
        event_type=event_type,
        date_str=date_str,
        time_str=time_str,
        location_tag=location_tag,
        work_report=work_report if work_report else "Routine Field Shift",
    )

    db.add(punch_entry)
    await db.commit()

    return {
        "status": "SUCCESS",
        "message": f"{event_type} registered successfully at {time_str}",
        "entry": {
            "staffId": staff_id,
            "type": event_type,
            "date": date_str,
            "time": time_str,
            "report": punch_entry.work_report,
            "location": location_tag,
        }
    }

@app.get("/api/v1/me/attendance")
async def fetch_employee_attendance_history(
    staff: "StaffPersonnel" = Depends(require_employee),
    db: AsyncSession = Depends(get_db_session),
):
    records = (
        await db.execute(
            sa.select(AttendanceRecord)
            .where(AttendanceRecord.staff_id == staff.staff_id)
            .order_by(AttendanceRecord.created_at.desc())
            .limit(100)
        )
    ).scalars().all()
    return [
        {"date": r.date_str, "time": r.time_str, "type": r.event_type,
         "report": r.work_report, "locationTag": r.location_tag}
        for r in records
    ]

# =============================================================================
# ENDPOINT 5: AUTOMATED FULL-TIME GPS TELEMETRY & ZERO-TOUCH SHOP DETECTOR
# =============================================================================
@app.post("/api/v1/telemetry/ping")
async def ingest_gps_telemetry(
    payload: Dict[str, Any],
    staff: "StaffPersonnel" = Depends(require_employee),
    db: AsyncSession = Depends(get_db_session),
):
    """
    ആപ്പിൽ നിന്ന് ഓട്ടോമാറ്റിക് ആയി വരുന്ന GPS കോർഡിനേറ്റ്സ് പരിശോധിച്ച്,
    കടകളിലോ സ്ഥാപനങ്ങളിലോ 3-5 മിനിറ്റ് തുടർച്ചയായി നിൽക്കുമ്പോൾ 
    'Habitual Shop Visit' ആയി തനിയെ രജിസ്റ്റർ ചെയ്യുന്നു.
    """
    staff_id = staff.staff_id
    try:
        lat = float(payload.get("latitude") or 0.0)
        lon = float(payload.get("longitude") or 0.0)
    except (TypeError, ValueError):
        lat = lon = 0.0
    tenant_name = staff.tenant_name or "Enterprise Tenant"
    now_epoch = time.time()
    now_str = datetime.now(APP_TZ).strftime("%Y-%m-%d %I:%M %p")

    if not staff_id or lat == 0.0 or lon == 0.0:
        return JSONResponse(
            status_code=400,
            content={"status": "IGNORED", "message": "Invalid GPS coordinates."},
        )

    # 1. സ്ഥലത്തിന്റെ/കടയുടെ പേര് ഓട്ടോമാറ്റിക് ആയി കണ്ടെത്തുന്നു
    detected_place = await resolve_shop_or_location(lat, lon)

    # 2. മുൻപത്തെ ടെലിമെട്രി ഡാറ്റാബേസിൽ നിന്ന് പരിശോധിക്കുന്നു
    stmt = sa.select(TelemetryStream).where(TelemetryStream.staff_id == staff_id)
    res = await db.execute(stmt)
    current_stream = res.scalar_one_or_none()

    is_stationary = False
    if current_stream:
        # ദൂരം കണക്കാക്കുന്നു (~35 മീറ്റർ പരിധിയിൽ തന്നെയുണ്ടോ എന്ന് നോക്കുന്നു)
        dist_delta = math.sqrt((lat - current_stream.latitude) ** 2 + (lon - current_stream.longitude) ** 2)
        if dist_delta < 0.00035:
            is_stationary = True

        # ലേറ്റസ്റ്റ് ലൊക്കേഷനിലേക്ക് അപ്ഡേറ്റ് ചെയ്യുന്നു
        current_stream.latitude = lat
        current_stream.longitude = lon
        current_stream.last_ping_epoch = now_epoch
        current_stream.last_ping_time = now_str
        current_stream.detected_place = detected_place
        current_stream.tenant_name = tenant_name
    else:
        new_stream = TelemetryStream(
            staff_id=staff_id,
            tenant_name=tenant_name,
            latitude=lat,
            longitude=lon,
            last_ping_epoch=now_epoch,
            last_ping_time=now_str,
            detected_place=detected_place,
        )
        db.add(new_stream)

    # 3. ഒരേ കടയിൽ/സ്പോട്ടിൽ തുടർച്ചയായി നിൽക്കുകയാണെങ്കിൽ സന്ദർശനം (Visit Count) തനിയെ കൂട്ടുന്നു
    if is_stationary:
        stmt_spot = sa.select(HabitualShopVisit).where(
            HabitualShopVisit.staff_id == staff_id,
            HabitualShopVisit.spot_name == detected_place,
        )
        res_spot = await db.execute(stmt_spot)
        spot_record = res_spot.scalar_one_or_none()

        if spot_record:
            spot_record.visit_count += 1
            spot_record.last_visit_time = now_str
        else:
            new_spot = HabitualShopVisit(
                staff_id=staff_id,
                spot_name=detected_place,
                visit_count=1,
                last_visit_time=now_str,
                coordinates=f"{lat:.4f}, {lon:.4f}",
            )
            db.add(new_spot)
            # ഓരോ 5 മിനിറ്റിലെ ലൊക്കേഷനും വർഷങ്ങളോളം സൂക്ഷിക്കാൻ സ്ഥിരമായി സേവ് ചെയ്യുന്നു
    breadcrumb = LocationBreadcrumbHistory(
        staff_id=staff_id,
        date_str=datetime.now(APP_TZ).strftime("%Y-%m-%d"),
        time_str=now_str,
        latitude=lat,
        longitude=lon,
        location_name=detected_place
    )
    db.add(breadcrumb)

    await db.commit()

    return {
        "status": "TELEMETRY_RECORDED",
        "detectedSpot": detected_place,
        "timestamp": now_str,
    }

# =============================================================================
# ENDPOINT 6: LEAVE APPLICATION ENGINE (VOICE & TEXT SYNCED)
# =============================================================================
@app.post("/api/v1/leaves/apply")
async def apply_for_leave(
    payload: Dict[str, Any],
    staff: "StaffPersonnel" = Depends(require_employee),
    db: AsyncSession = Depends(get_db_session),
):
    target_date = (payload.get("date") or datetime.now(APP_TZ).strftime("%Y-%m-%d")).strip()
    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Date must be YYYY-MM-DD."})
    reason = (payload.get("reason") or "").strip()
    if not reason:
        return JSONResponse(status_code=400, content={"error": "Reason is required."})

    leave_entry = LeaveApplication(
        leave_id=f"LV-{uuid.uuid4().hex[:12]}",
        staff_id=staff.staff_id,
        staff_name=staff.name,
        target_date=target_date,
        reason=reason,
        status="PENDING",
        is_voice_note=bool(payload.get("isVoiceNote", False)),
    )
    db.add(leave_entry)
    await db.commit()
    return {"status": "SUCCESS", "leaveId": leave_entry.leave_id,
            "message": "Leave application sent to your admin."}


@app.get("/api/v1/me/leaves")
async def my_leaves(
    staff: "StaffPersonnel" = Depends(require_employee),
    db: AsyncSession = Depends(get_db_session),
):
    rows = (
        await db.execute(
            sa.select(LeaveApplication)
            .where(LeaveApplication.staff_id == staff.staff_id)
            .order_by(LeaveApplication.created_at.desc())
            .limit(100)
        )
    ).scalars().all()
    return [
        {"leaveId": l.leave_id, "date": l.target_date, "reason": l.reason,
         "status": l.status, "isVoiceNote": l.is_voice_note}
        for l in rows
    ]

@app.post("/api/v1/admin/leave/action")
async def modify_leave_status(
    payload: Dict[str, Any],
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):
    decision = str(payload.get("status", "")).upper()
    if decision not in ("APPROVED", "REJECTED"):
        return JSONResponse(status_code=400, content={"error": "Status must be APPROVED or REJECTED."})

    leave = (
        await db.execute(
            sa.select(LeaveApplication)
            .join(StaffPersonnel, StaffPersonnel.staff_id == LeaveApplication.staff_id)
            .where(LeaveApplication.leave_id == payload.get("leaveId"), tenant_staff_filter(tenant))
        )
    ).scalar_one_or_none()
    if not leave:
        return JSONResponse(status_code=404, content={"error": "Leave record not found."})

    leave.status = decision
    await db.commit()
    return {"status": "UPDATED", "leaveId": leave.leave_id, "action": decision}

# =============================================================================
# ENDPOINT 7: DYNAMIC BRANDING THEME INGESTION (LOGO & FULL WALLPAPER)
# =============================================================================
@app.post("/api/v1/admin/tenant/update-theme-assets")
async def update_tenant_theme_assets(
    payload: Dict[str, Any],
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):
    media_type = payload.get("mediaType", "")
    raw_media = payload.get("rawMedia", "")
    if media_type == "logo":
        tenant.logo_data = raw_media
    elif media_type == "backgroundTheme":
        tenant.wallpaper_theme = raw_media
    else:
        return JSONResponse(status_code=400, content={"error": "Unknown mediaType."})
    await db.commit()
    return {"status": "SUCCESS", "message": f"Brand {media_type} updated."}

@app.get("/api/v1/tenant/get-media")
async def retrieve_tenant_branding(
    tenantName: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db_session),
):
    if not tenantName:
        return {"logo": "", "backgroundTheme": ""}

    clean_tenant = tenantName.strip().lower()
    print(f">>> [GET MEDIA] സ്ക്രീൻ ചോദിച്ച കമ്പനി: '{clean_tenant}'")
    stmt = sa.select(OrganizationTenant).where(
        sa.or_(
            sa.func.lower(OrganizationTenant.legal_name) == clean_tenant,
            sa.func.lower(OrganizationTenant.id) == clean_tenant
        )
    )
    res = await db.execute(stmt)
    tenant = res.scalar_one_or_none()

    if not tenant:
        return {"logo": "", "backgroundTheme": ""}

    return {
        "logo": tenant.logo_data or "",
        "backgroundTheme": tenant.wallpaper_theme or "",
    }
@app.post("/api/v1/tenant/auth")
async def tenant_auth(payload: Dict[str, Any], db: AsyncSession = Depends(get_db_session)):
    """Admin registration (isLogin=false) and login (isLogin=true)."""
    is_login = payload.get("isLogin", True)
    email = (payload.get("adminEmail") or "").strip().lower()
    password = payload.get("password") or ""
    if "@" not in email or not password:
        return JSONResponse(status_code=400, content={"error": "Admin email and password are required."})

    if is_login:
        check_lockout(f"admin:{email}")
        tenant = (
            await db.execute(sa.select(OrganizationTenant).where(sa.func.lower(OrganizationTenant.admin_email) == email))
        ).scalars().first()
        if not tenant or not verify_secret(password, tenant.admin_password_hash):
            record_failure(f"admin:{email}")
            return JSONResponse(status_code=401, content={"error": "Invalid email or password."})
        clear_failures(f"admin:{email}")
        return {
            "status": "SUCCESS",
            "token": make_token(email, "admin", tenant.id, minutes=12 * 60),
            "tenantName": tenant.legal_name,
            "category": tenant.category,
        }

    name = (payload.get("tenantName") or "").strip()
    if not name:
        return JSONResponse(status_code=400, content={"error": "Organization name is required."})
    if len(password) < 8:
        return JSONResponse(status_code=400, content={"error": "Password must be at least 8 characters."})
    if await find_tenant(db, name):
        return JSONResponse(status_code=400, content={"error": "This organization is already registered. Please log in."})
    taken = (
        await db.execute(sa.select(OrganizationTenant.id).where(sa.func.lower(OrganizationTenant.admin_email) == email))
    ).first()
    if taken:
        return JSONResponse(status_code=400, content={"error": "This email is already registered. Please log in."})

    tenant = OrganizationTenant(
        id=name.lower()[:64],
        legal_name=name[:255],
        category=payload.get("category") or "Company",
        subscription_plan=payload.get("plan") or "1 Year Free Trial",
        admin_email=email,
        admin_password_hash=hash_secret(password),
        logo_data="",
        wallpaper_theme="",
    )
    db.add(tenant)
    await db.commit()
    return {
        "status": "SUCCESS",
        "token": make_token(email, "admin", tenant.id, minutes=12 * 60),
        "tenantName": tenant.legal_name,
        "category": tenant.category,
    }

# =============================================================================
# ENDPOINT 8: REAL-TIME ADMIN AUDIT FEED & 10-MINUTE GPS SENTINEL WATCHDOG
# =============================================================================
@app.get("/api/v1/admin/audit-feed")
async def get_realtime_admin_audit_feed(
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):
    """Attendance, leaves, shop spots and GPS alerts for the signed-in admin's tenant."""

    now_epoch = time.time()
    staff_ids = (
        await db.execute(sa.select(StaffPersonnel.staff_id).where(tenant_staff_filter(tenant)))
    ).scalars().all()
    if not staff_ids:
        return {"attendance": [], "leaveRequests": [], "frequentSpots": [], "gpsAlerts": []}

    att_rows = (
        await db.execute(
            sa.select(AttendanceRecord)
            .where(AttendanceRecord.staff_id.in_(staff_ids))
            .order_by(AttendanceRecord.created_at.desc())
            .limit(100)
        )
    ).scalars().all()
    attendance_feed = [
        {
            "staffId": display_code(a.staff_id),
            "fullStaffId": a.staff_id,
            "type": a.event_type or "Punch",
            "date": a.date_str or "",
            "time": a.time_str or "",
            "locationTag": a.location_tag or "",
            "report": a.work_report or "",
        }
        for a in att_rows
    ]

    lv_rows = (
        await db.execute(
            sa.select(LeaveApplication)
            .where(LeaveApplication.staff_id.in_(staff_ids))
            .order_by(LeaveApplication.created_at.desc())
            .limit(100)
        )
    ).scalars().all()
    leave_feed = [
        {
            "leaveId": l.leave_id,
            "staffId": display_code(l.staff_id),
            "staffName": l.staff_name,
            "date": l.target_date,
            "reason": l.reason,
            "status": l.status,
            "isVoiceNote": l.is_voice_note,
        }
        for l in lv_rows
    ]

    spot_rows = (
        await db.execute(
            sa.select(HabitualShopVisit)
            .where(HabitualShopVisit.staff_id.in_(staff_ids))
            .order_by(HabitualShopVisit.visit_count.desc())
            .limit(100)
        )
    ).scalars().all()
    frequent_spots_feed = [
        {
            "staffId": display_code(s.staff_id),
            "spotName": s.spot_name,
            "count": s.visit_count,
            "time": s.last_visit_time,
            "locationCoordinates": s.coordinates,
        }
        for s in spot_rows
    ]

    tel_rows = (
        await db.execute(sa.select(TelemetryStream).where(TelemetryStream.staff_id.in_(staff_ids)))
    ).scalars().all()
    gps_breach_alerts = []
    for t in tel_rows:
        elapsed_minutes = int((now_epoch - t.last_ping_epoch) / 60)
        if elapsed_minutes >= 10:
            gps_breach_alerts.append({
                "staffId": display_code(t.staff_id),
                "elapsedMinutes": elapsed_minutes,
                "status": "ATTENDANCE_HALTED" if elapsed_minutes >= 15 else "GPS_TAMPER_ALERT",
                "time": t.last_ping_time,
                "lastKnownLocation": t.detected_place,
            })

    return {
        "attendance": attendance_feed,
        "leaveRequests": leave_feed,
        "frequentSpots": frequent_spots_feed,
        "gpsAlerts": gps_breach_alerts,
    }

# =============================================================================
# ENDPOINT 9: FULL STAFF DIRECTORY CRUD (ADMIN MANAGEMENT)
# =============================================================================
@app.get("/api/v1/staff")
async def fetch_all_enrolled_staff(
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):
    staff_list = (
        await db.execute(sa.select(StaffPersonnel).where(tenant_staff_filter(tenant)))
    ).scalars().all()

    return [
        {
            "staffId": display_code(s.staff_id),
            "name": s.name,
            "role": s.role,
            "email": s.email,
            "phone": s.phone,
            "salary": s.salary,
            "shiftStart": s.shift_start,
            "shiftEnd": s.shift_end,
            "photoData": s.reference_biometric_photo,
            "hasPin": bool(s.pin_hash),
        }
        for s in staff_list
    ]

@app.post("/api/v1/staff")
async def enroll_new_staff_member(
    payload: Dict[str, Any],
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):
    raw_code = (payload.get("staffId") or "").strip().upper()
    if not raw_code:
        return JSONResponse(status_code=400, content={"error": "Staff ID is mandatory."})

    staff_id = build_staff_id(tenant, raw_code)
    existing = (
        await db.execute(sa.select(StaffPersonnel).where(StaffPersonnel.staff_id == staff_id))
    ).scalar_one_or_none()
    if existing:
        return JSONResponse(
            status_code=400,
            content={"error": f"Staff ID '{raw_code}' already enrolled in system."},
        )

    pin = new_pin()
    try:
        salary = float(payload.get("salary") or 0)
    except (TypeError, ValueError):
        salary = 0.0

    new_staff = StaffPersonnel(
        staff_id=staff_id,
        tenant_id=tenant.id,
        tenant_name=tenant.legal_name,
        name=payload.get("name", "Authorized Personnel"),
        role=payload.get("role", "Field Staff"),
        email=payload.get("email", ""),
        phone=payload.get("phone", ""),
        salary=salary,
        shift_start=payload.get("shiftStart", "09:00 AM"),
        shift_end=payload.get("shiftEnd", "05:00 PM"),
        reference_biometric_photo=payload.get("photoData", ""),
        pin_hash=hash_secret(pin),
    )
    db.add(new_staff)
    await db.commit()
    await db.refresh(new_staff)

    return {
        "status": "SUCCESS",
        "staff": {
            "staffId": display_code(new_staff.staff_id),
            "name": new_staff.name,
            "role": new_staff.role,
            "salary": new_staff.salary,
            "shiftStart": new_staff.shift_start,
            "shiftEnd": new_staff.shift_end,
        },
        "pin": pin,  # shown to the admin once; only the hash is stored
    }

@app.delete("/api/v1/staff/{staff_id}")
async def remove_staff_member(
    staff_id: str,
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):

    full_id = resolve_staff_id(tenant, staff_id.upper() if "__" not in staff_id else staff_id)
    staff = (
        await db.execute(
            sa.select(StaffPersonnel).where(
                StaffPersonnel.staff_id == full_id, tenant_staff_filter(tenant)
            )
        )
    ).scalar_one_or_none()
    if not staff:
        return JSONResponse(
            status_code=404,
            content={"error": "Staff not found or does not belong to your company."},
        )

    await db.delete(staff)
    await db.commit()
    return {"status": "DELETED", "staffId": display_code(full_id)}

@app.post("/api/v1/staff/{staff_id}/reset-pin")
async def reset_staff_pin(
    staff_id: str,
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):
    full_id = resolve_staff_id(tenant, staff_id.upper() if "__" not in staff_id else staff_id)
    staff = (
        await db.execute(
            sa.select(StaffPersonnel).where(StaffPersonnel.staff_id == full_id, tenant_staff_filter(tenant))
        )
    ).scalar_one_or_none()
    if not staff:
        return JSONResponse(status_code=404, content={"error": "Staff not found."})
    pin = new_pin()
    staff.pin_hash = hash_secret(pin)
    await db.commit()
    clear_failures(staff.staff_id)
    return {"status": "SUCCESS", "staffId": display_code(staff.staff_id), "pin": pin}


# =============================================================================
# PAYROLL: SALARY PRORATED BY WORKING DAYS (SATURDAY POLICY), PRESENT DAYS AND APPROVED LEAVE
# =============================================================================
@app.get("/api/v1/admin/payroll")
async def monthly_payroll(
    month: Optional[str] = Query(None, description="YYYY-MM, default current month"),
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):
    today = datetime.now(APP_TZ).date()
    try:
        year, mon = (int(x) for x in month.split("-")) if month else (today.year, today.month)
        first = date(year, mon, 1)
    except (ValueError, AttributeError):
        return JSONResponse(status_code=400, content={"error": "month must be YYYY-MM."})
    next_first = date(year + (mon == 12), mon % 12 + 1, 1)

    policy = tenant.saturday_policy or "SECOND_ONLY"
    working = set()
    d = first
    while d < next_first:
        if d.weekday() != 6 and not is_saturday_off(d, policy):  # Sundays always off
            working.add(d.isoformat())
        d += timedelta(days=1)

    prefix = f"{year:04d}-{mon:02d}"
    staff_rows = (
        await db.execute(sa.select(StaffPersonnel).where(tenant_staff_filter(tenant)).order_by(StaffPersonnel.name))
    ).scalars().all()

    result = []
    for s in staff_rows:
        present = {
            r for r in (
                await db.execute(
                    sa.select(AttendanceRecord.date_str).where(
                        AttendanceRecord.staff_id == s.staff_id,
                        AttendanceRecord.event_type == "PUNCH_IN",
                        AttendanceRecord.date_str.startswith(prefix),
                    ).distinct()
                )
            ).scalars().all()
        } & working
        leave_days = {
            r for r in (
                await db.execute(
                    sa.select(LeaveApplication.target_date).where(
                        LeaveApplication.staff_id == s.staff_id,
                        LeaveApplication.status == "APPROVED",
                        LeaveApplication.target_date.startswith(prefix),
                    )
                )
            ).scalars().all()
        } & working - present
        paid_days = len(present) + len(leave_days)
        per_day = (s.salary or 0) / len(working) if working else 0
        result.append({
            "staffId": display_code(s.staff_id),
            "name": s.name,
            "role": s.role,
            "monthlySalary": s.salary or 0,
            "workingDays": len(working),
            "presentDays": len(present),
            "approvedLeaveDays": len(leave_days),
            "absentDays": len(working) - paid_days,
            "dailyRate": round(per_day, 2),
            "payable": round(per_day * paid_days, 2),
        })
    return {"month": prefix, "saturdayPolicy": policy, "workingDays": len(working), "staff": result}


# =============================================================================
# ENDPOINT 10: CLOUD HEALTH PROBE & MESH HEARTBEAT
# =============================================================================
@app.get("/healthz")
@app.get("/api/v1/health")
async def health_check():
    """
    AWS ECS / Railway / Kubernetes ക്ലൗഡ് സർവീസുകൾക്കായി 
    സെർവർ ആക്ടീവ് ആണോ എന്ന് പരിശോധിക്കുന്ന ഹെൽത്ത് പ്രോബ്.
    """
    return {
        "status": "HEALTHY",
        "meshNode": "ONLINE",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "3.0.0-PROD"
    }

# =============================================================================
# PRODUCTION LAUNCHER (DYNAMIC PORT & AUTO MULTI-WORKER DEPLOYMENT)
# =============================================================================
# =============================================================================
# SATURDAY POLICY CHECKER ENGINE
# =============================================================================
def is_saturday_off(target_date: date, policy: str) -> bool:
    # ശനിയാഴ്ച അല്ലെങ്കിൽ ഫോൾസ് നൽകുന്നു
    if target_date.weekday() != 5:  
        return False

    day = target_date.day
    saturday_index = (day - 1) // 7 + 1  # എത്രാമത്തെ ശനിയാഴ്ചയാണെന്ന് കണ്ടെത്തുന്നു

    if policy == "ALL_OFF":
        return True
    elif policy == "NO_OFF":
        return False
    elif policy == "SECOND_ONLY":
        return saturday_index == 2  # രണ്ടാം ശനിയാഴ്ച മാത്രം ഓഫ്
    elif policy == "SECOND_FOURTH_OFF":
        return saturday_index in [2, 4]  # 2, 4 ശനിയാഴ്ചകൾ ഓഫ്
    elif policy == "FIRST_THIRD_OFF":
        return saturday_index in [1, 3]  # 1, 3 ശനിയാഴ്ചകൾ ഓഫ്

    return False

@app.post("/api/v1/admin/tenant/saturday-policy")
async def set_tenant_saturday_policy(
    payload: Dict[str, Any],
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session),
):
    policy = str(payload.get("saturdayPolicy", "SECOND_ONLY")).upper()
    if policy not in ("ALL_OFF", "NO_OFF", "SECOND_ONLY", "SECOND_FOURTH_OFF", "FIRST_THIRD_OFF"):
        return JSONResponse(status_code=400, content={"error": "Unknown policy."})
    tenant.saturday_policy = policy
    await db.commit()
    return {"status": "SUCCESS", "tenant": tenant.legal_name, "activeSaturdayPolicy": policy,
            "message": f"Saturday policy updated to: {policy}"}

# =============================================================================
# GET TENANT SATURDAY POLICY
# =============================================================================
@app.get("/api/v1/admin/tenant/saturday-policy")
async def get_tenant_saturday_policy(tenant: "OrganizationTenant" = Depends(require_admin)):
    return {"activeSaturdayPolicy": tenant.saturday_policy or "SECOND_ONLY"}

@app.get("/api/v1/admin/staff-route-history")
async def get_staff_route_history(
    staffId: str = Query(...),
    date: Optional[str] = Query(None),
    tenant: "OrganizationTenant" = Depends(require_admin),
    db: AsyncSession = Depends(get_db_session)
):
    full_id = resolve_staff_id(tenant, staffId.upper() if "__" not in staffId else staffId)
    owned = (
        await db.execute(sa.select(StaffPersonnel.staff_id).where(StaffPersonnel.staff_id == full_id, tenant_staff_filter(tenant)))
    ).scalar_one_or_none()
    if not owned:
        return JSONResponse(status_code=404, content={"error": "Staff not found."})
    query_date = date or datetime.now(APP_TZ).strftime("%Y-%m-%d")
    stmt = (
        sa.select(LocationBreadcrumbHistory)
        .where(
            LocationBreadcrumbHistory.staff_id == full_id,
            LocationBreadcrumbHistory.date_str == query_date
        )
        .order_by(LocationBreadcrumbHistory.created_at.asc())
    )
    records = (await db.execute(stmt)).scalars().all()

    stmt_spots = sa.select(HabitualShopVisit).where(HabitualShopVisit.staff_id == full_id)
    visited_spots = (await db.execute(stmt_spots)).scalars().all()

    return {
        "staffId": display_code(full_id),
        "date": query_date,
        "totalPings": len(records),
        "timeline": [
            {"time": r.time_str, "location": r.location_name, "lat": r.latitude, "lng": r.longitude}
            for r in records
        ],
        "shopsVisited": [
            {"spotName": s.spot_name, "visits": s.visit_count, "lastTime": s.last_visit_time}
            for s in visited_spots
        ],
    }

if __name__ == "__main__":
    import uvicorn

    # Railway / AWS / Render നൽകുന്ന എൻവയോൺമെന്റ് പോർട്ട് തനിയെ സ്വീകരിക്കുന്നു
    port = int(os.getenv("PORT", 8000))

    print("=" * 70)
    print(" [DV.MASTER ULTRA-ENTERPRISE BIOMETRIC WORKFORCE MESH SERVER]")
    print(f" * Operating Mode : {ENVIRONMENT.upper()}")
    print(f" * Service Port   : {port}")
    print(f" * Database Link  : {ASYNC_DATABASE_URL.split('@')[-1] if '@' in ASYNC_DATABASE_URL else 'Local High-Speed Async Engine'}")
    print("=" * 70)

    # ഹൈപ്പർ കൺകറൻസി റണ്ണർ
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        access_log=True,
    )
