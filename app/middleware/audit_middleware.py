"""
app/middleware/audit_middleware.py
══════════════════════════════════════════════════════════
Audit Trail Middleware
يسجّل كل طلب API تلقائياً في user_activity_log

يستخرج:
- من المستخدم (من JWT)
- ماذا فعل (من method + path)
- متى (timestamp)
- من أين (IP)
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
import re
import time
import uuid
from typing import Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

import structlog

logger = structlog.get_logger(__name__)

# ── تحديد نوع الحدث من URL + Method ─────────────────────
ACTION_MAP = [
    # Auth
    (r"POST",    r"/auth/login",                    "login",    "تسجيل دخول",        "auth",       "auth",      "المصادقة"),
    (r"POST",    r"/auth/logout",                   "logout",   "تسجيل خروج",        "auth",       "auth",      "المصادقة"),
    (r"POST",    r"/auth/refresh",                  "login",    "تجديد الجلسة",       "auth",       "auth",      "المصادقة"),

    # Journal Entries
    (r"POST",    r"/accounting/je$",                "create",   "إنشاء قيد",         "accounting", "je",        "المحاسبة"),
    (r"PUT",     r"/accounting/je/",                "update",   "تعديل قيد",         "accounting", "je",        "المحاسبة"),
    (r"POST",    r"/accounting/je/.+/submit",       "submit",   "إرسال قيد للمراجعة","accounting", "je",        "المحاسبة"),
    (r"POST",    r"/accounting/je/.+/approve",      "approve",  "اعتماد قيد",        "accounting", "je",        "المحاسبة"),
    (r"POST",    r"/accounting/je/.+/post",         "post",     "ترحيل قيد",         "accounting", "je",        "المحاسبة"),
    (r"POST",    r"/accounting/je/.+/reverse",      "reverse",  "عكس قيد",           "accounting", "je",        "المحاسبة"),
    (r"POST",    r"/accounting/je/.+/reject",       "reject",   "رفض قيد",           "accounting", "je",        "المحاسبة"),
    (r"GET",     r"/accounting/je/",                "view",     "عرض قيد",           "accounting", "je",        "المحاسبة"),

    # COA
    (r"POST",    r"/accounting/coa",                "create",   "إضافة حساب",        "accounting", "coa",       "المحاسبة"),
    (r"PUT",     r"/accounting/coa/",               "update",   "تعديل حساب",        "accounting", "coa",       "المحاسبة"),
    (r"DELETE",  r"/accounting/coa/",               "delete",   "حذف حساب",          "accounting", "coa",       "المحاسبة"),
    (r"POST",    r"/accounting/coa/import",         "import",   "استيراد دليل الحسابات","accounting","coa",     "المحاسبة"),

    # Recurring
    (r"POST",    r"/recurring$",                    "create",   "إنشاء قيد متكرر",   "accounting", "recurring", "المحاسبة"),
    (r"POST",    r"/recurring/.+/post-pending",     "post",     "ترحيل أقساط متكررة","accounting", "recurring", "المحاسبة"),

    # Fiscal
    (r"POST",    r"/fiscal",                        "create",   "إنشاء سنة/فترة مالية","settings", "fiscal",    "الإعداد"),
    (r"POST",    r"/fiscal-locks",                  "lock",     "قفل فترة مالية",    "settings",   "fiscal",    "الإعداد"),
    (r"DELETE",  r"/fiscal-locks/",                 "unlock",   "فتح فترة مالية",    "settings",   "fiscal",    "الإعداد"),

    # Opening Balances
    (r"POST",    r"/opening-balances/post",         "post",     "ترحيل الأرصدة الافتتاحية","accounting","ob",  "المحاسبة"),
    (r"POST",    r"/opening-balances/batch",        "update",   "تحديث الأرصدة الافتتاحية","accounting","ob",  "المحاسبة"),

    # Reports
    (r"GET",     r"/reports/income",                "view",     "عرض قائمة الدخل",   "reports",    "report",    "التقارير"),
    (r"GET",     r"/reports/balance",               "view",     "عرض الميزانية",     "reports",    "report",    "التقارير"),
    (r"GET",     r"/reports/cashflow",              "view",     "عرض التدفقات",      "reports",    "report",    "التقارير"),
    (r"GET",     r"/reports/vat",                   "view",     "عرض تقرير VAT",     "reports",    "report",    "التقارير"),
    (r"GET",     r"/reports/trial-balance",         "view",     "عرض ميزان المراجعة","reports",    "report",    "التقارير"),
    (r"GET",     r"/reports/financial-analysis",    "view",     "عرض التحليل المالي","reports",    "report",    "التقارير"),

    # Settings
    (r"PUT",     r"/settings/company",              "update",   "تعديل إعدادات الشركة","settings", "company",   "الإعداد"),
    (r"POST",    r"/settings/branches",             "create",   "إضافة فرع",         "settings",   "branch",    "الإعداد"),
    (r"POST",    r"/settings/cost-centers",         "create",   "إضافة مركز تكلفة",  "settings",   "cc",        "الإعداد"),
    (r"POST",    r"/settings/currencies",           "create",   "إضافة عملة",        "settings",   "currency",  "الإعداد"),
    (r"POST",    r"/settings/currencies/exchange-rates","create","إضافة سعر صرف",    "settings",   "currency",  "الإعداد"),

    # Users
    (r"POST",    r"/users",                         "create",   "إضافة مستخدم",      "users",      "user",      "المستخدمون"),
    (r"PUT",     r"/users/",                        "update",   "تعديل مستخدم",      "users",      "user",      "المستخدمون"),
    (r"DELETE",  r"/users/",                        "delete",   "حذف مستخدم",        "users",      "user",      "المستخدمون"),

    # Tax
    (r"POST",    r"/accounting/tax-types",          "create",   "إضافة نوع ضريبة",   "settings",   "tax",       "الإعداد"),
    (r"PUT",     r"/accounting/tax-types/",         "update",   "تعديل نوع ضريبة",   "settings",   "tax",       "الإعداد"),

    # Rebuild
    (r"POST",    r"/accounting/rebuild-balances",   "admin",    "إعادة بناء الأرصدة","accounting", "admin",     "المحاسبة"),
]

# المسارات التي لا نسجّلها (ضجيج)
SKIP_PATHS = {
    "/health", "/docs", "/openapi.json", "/redoc",
    "/favicon.ico",
}
SKIP_PATTERNS = [
    r"^/api/v1/accounting/je/activity",   # لا نسجّل عرض السجل نفسه
    r"^/api/v1/accounting/je/\w+/activity",
    r"^/api/v1/audit",
    r"^/api/v1/notifications",
    r"^/api/v1/accounting/dashboard",
]

# المسارات التي لا تحتاج تسجيل (GET العادية)
SKIP_GET_PATTERNS = [
    r"/settings/",
    r"/accounting/coa$",
    r"/accounting/je$",
    r"/accounting/trial-balance",
    r"/dimensions",
]


def _classify(method: str, path: str):
    """استخراج نوع الحدث من method + path"""
    for m, p, action_type, action_ar, module, resource_type, module_ar in ACTION_MAP:
        if method.upper() == m and re.search(p, path, re.IGNORECASE):
            return action_type, action_ar, module, resource_type, module_ar
    return None, None, None, None, None


def _should_skip(method: str, path: str) -> bool:
    """هل نتخطى هذا الطلب؟"""
    if any(path.startswith(p) for p in SKIP_PATHS):
        return True
    for pattern in SKIP_PATTERNS:
        if re.search(pattern, path):
            return True
    # GET الاعتيادية لا نسجّلها
    if method.upper() == "GET":
        for pattern in SKIP_GET_PATTERNS:
            if re.search(pattern, path):
                return True
        # نسجّل فقط GET للتقارير
        if not re.search(r"/reports/", path):
            return True
    return False


def _get_client_ip(request: Request) -> str:
    """استخراج IP الحقيقي"""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class AuditMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next) -> Response:
        path   = request.url.path
        method = request.method

        # تخطي المسارات غير المهمة
        if _should_skip(method, path):
            return await call_next(request)

        # تنفيذ الطلب
        start_time = time.time()
        response   = await call_next(request)
        duration   = round((time.time() - start_time) * 1000)  # ms

        # لا نسجّل إلا الطلبات الناجحة (2xx) والمهمة
        if response.status_code < 200 or response.status_code >= 400:
            return response

        try:
            # استخراج بيانات المستخدم من state (يُعبَّأ بواسطة get_current_user)
            user_email   = getattr(request.state, "user_email",   None)
            user_id      = getattr(request.state, "user_id",      None)
            tenant_id    = getattr(request.state, "tenant_id",    None)
            display_name = getattr(request.state, "display_name", None)

            if not tenant_id or not user_email:
                return response

            action_type, action_ar, module, resource_type, module_ar = _classify(method, path)
            if not action_type:
                return response

            # استخراج resource_id من URL (آخر UUID في المسار)
            resource_id = None
            uuid_match  = re.findall(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                path, re.IGNORECASE
            )
            if uuid_match:
                resource_id = uuid_match[-1]

            # حفظ في قاعدة البيانات
            from app.db.session import AsyncSessionLocal
            async with AsyncSessionLocal() as db:
                await db.execute(
                    __import__("sqlalchemy", fromlist=["text"]).text("""
                        INSERT INTO user_activity_log
                            (id, tenant_id, user_id, user_email, display_name,
                             action_type, action_ar, module, module_ar,
                             resource_type, resource_id,
                             ip_address, user_agent, status, extra_data)
                        VALUES
                            (gen_random_uuid(), :tid, :uid, :email, :dname,
                             :atype, :aar, :mod, :mod_ar,
                             :rtype, :rid,
                             :ip, :ua, 'success',
                             :extra)
                    """),
                    {
                        "tid":    str(tenant_id),
                        "uid":    str(user_id) if user_id else None,
                        "email":  user_email,
                        "dname":  display_name or (user_email.split("@")[0] if user_email else None),
                        "atype":  action_type,
                        "aar":    action_ar,
                        "mod":    module,
                        "mod_ar": module_ar,
                        "rtype":  resource_type,
                        "rid":    resource_id,
                        "ip":     _get_client_ip(request),
                        "ua":     request.headers.get("User-Agent", "")[:200],
                        "extra":  json.dumps({"path": path, "duration_ms": duration}),
                    }
                )
                await db.commit()

        except Exception as e:
            logger.warning("audit_middleware_error", error=str(e))

        return response
