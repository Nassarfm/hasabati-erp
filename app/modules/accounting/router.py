"""
app/modules/accounting/router.py
══════════════════════════════════════════════════════════
Accounting Module API — 16 endpoints

Chart of Accounts:
  GET    /accounting/coa              قائمة الحسابات
  POST   /accounting/coa              إنشاء حساب
  GET    /accounting/coa/{id}         تفاصيل حساب

Journal Entries:
  POST   /accounting/je               إنشاء قيد (draft)
  GET    /accounting/je               قائمة القيود
  GET    /accounting/je/{id}          تفاصيل قيد
  POST   /accounting/je/{id}/post     ترحيل قيد
  POST   /accounting/je/{id}/reverse  عكس قيد

Fiscal Periods:
  GET    /accounting/fiscal-periods   قائمة الفترات
  GET    /accounting/fiscal-locks     قائمة الأقفال
  POST   /accounting/fiscal-locks     قفل فترة
  DELETE /accounting/fiscal-locks/{id} فك قفل

Reports:
  GET    /accounting/ledger/{code}    كشف حساب
  GET    /accounting/trial-balance    ميزان المراجعة
  GET    /accounting/dashboard        ملخص محاسبي
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, File, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import created, ok, paginated
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.accounting.schemas import (
    COAAccountCreate, JournalEntryCreate,
    LockPeriodRequest, PostJERequest, ReverseJERequest,
)
from app.modules.accounting.service import AccountingService

router = APIRouter(prefix="/accounting", tags=["المحاسبة"])


def _svc(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
) -> AccountingService:
    return AccountingService(db, user)


# ══════════════════════════════════════════════════════════
# Chart of Accounts
# ══════════════════════════════════════════════════════════
@router.get("/coa", summary="قائمة الحسابات")
async def list_coa(svc: AccountingService = Depends(_svc)):
    accounts = await svc.list_accounts()
    return ok(
        data=[a.to_dict() for a in accounts],
        message=f"{len(accounts)} حساب",
    )


@router.post("/coa", status_code=201, summary="إنشاء حساب جديد")
async def create_account(
    data: COAAccountCreate,
    svc: AccountingService = Depends(_svc),
):
    acc = await svc.create_account(data)
    return created(data=acc.to_dict(), message=f"تم إنشاء الحساب {data.code}")


@router.get("/coa/{account_id}", summary="تفاصيل حساب")
async def get_account(
    account_id: uuid.UUID,
    svc: AccountingService = Depends(_svc),
):
    from app.core.exceptions import NotFoundError
    # Direct repo access for simple reads
    acc = await svc._coa_repo.get_or_raise(account_id)
    return ok(data=acc.to_dict())


# ══════════════════════════════════════════════════════════
# Journal Entries
# ══════════════════════════════════════════════════════════
@router.post("/je", status_code=201, summary="إنشاء قيد محاسبي (draft)")
async def create_je(
    data: JournalEntryCreate,
    svc: AccountingService = Depends(_svc),
):
    """
    Creates a DRAFT journal entry.
    Must be explicitly posted via POST /je/{id}/post.
    Validates double-entry balance before saving.
    """
    je = await svc.create_draft_je(data)
    return created(
        data={
            "id": str(je.id),
            "serial": je.serial,
            "status": je.status,
            "total_debit": float(je.total_debit),
            "total_credit": float(je.total_credit),
        },
        message=f"تم إنشاء القيد {je.serial} — في انتظار الترحيل",
    )


@router.get("/je", summary="قائمة القيود المحاسبية")
async def list_je(
    status:      Optional[str]  = Query(None),
    je_type:     Optional[str]  = Query(None),
    date_from:   Optional[date] = Query(None),
    date_to:     Optional[date] = Query(None),
    fiscal_year: Optional[int]  = Query(None),
    page:        int            = Query(1, ge=1),
    page_size:   int            = Query(20, ge=1, le=100),
    svc: AccountingService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc.list_je(
        status=status,
        je_type=je_type,
        date_from=date_from,
        date_to=date_to,
        fiscal_year=fiscal_year,
        offset=offset,
        limit=page_size,
    )
    return paginated(
        items=[je.to_dict() for je in items],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/je/{je_id}", summary="تفاصيل قيد محاسبي")
async def get_je(
    je_id: uuid.UUID,
    svc: AccountingService = Depends(_svc),
):
    je = await svc.get_je(je_id)
    data = je.to_dict()
    data["lines"] = [l.to_dict() for l in je.lines]
    return ok(data=data)


@router.post("/je/{je_id}/post", summary="ترحيل قيد محاسبي")
async def post_je(
    je_id: uuid.UUID,
    body: PostJERequest,
    svc: AccountingService = Depends(_svc),
):
    """
    Posts a draft JE through PostingEngine.
    Validates:
      ✅ Double-entry balance
      ✅ Fiscal period not locked
      ✅ All accounts are postable
      ✅ Tenant isolation
    """
    je = await svc.post_je(je_id, force=body.force)
    return ok(
        data={
            "id": str(je.id),
            "serial": je.serial,
            "status": je.status,
            "posted_at": str(je.posted_at),
            "total_debit": float(je.total_debit),
            "total_credit": float(je.total_credit),
        },
        message=f"✅ تم ترحيل القيد {je.serial}",
    )


@router.post("/je/{je_id}/reverse", summary="عكس قيد محاسبي")
async def reverse_je(
    je_id: uuid.UUID,
    data: ReverseJERequest,
    svc: AccountingService = Depends(_svc),
):
    """
    Reverses a posted JE.
    Creates a new REV- entry with flipped DR/CR.
    Marks original as REVERSED.
    """
    result = await svc.reverse_je(je_id, data)
    return ok(
        data=result,
        message=f"✅ تم عكس القيد — قيد العكس: {result.get('je_serial')}",
    )


# ══════════════════════════════════════════════════════════
# Fiscal Periods & Locks
# ══════════════════════════════════════════════════════════
@router.get("/fiscal-locks", summary="أقفال الفترات المالية")
async def list_locks(svc: AccountingService = Depends(_svc)):
    locks = await svc.list_locks()
    return ok(data=locks)


@router.post("/fiscal-locks", status_code=201, summary="قفل فترة مالية")
async def lock_period(
    data: LockPeriodRequest,
    svc: AccountingService = Depends(_svc),
):
    """
    Lock a fiscal period.
    soft = warning (admin can bypass)
    hard = absolute block (no bypass)
    """
    result = await svc.lock_period(data)
    lock_label = "صارم" if data.lock_type == "hard" else "ناعم"
    return created(
        data=result,
        message=f"تم قفل الفترة {data.fiscal_year}"
                + (f"/{data.fiscal_month:02d}" if data.fiscal_month else "")
                + f" — قفل {lock_label}",
    )


@router.delete("/fiscal-locks/{lock_id}", summary="فك قفل فترة مالية")
async def unlock_period(
    lock_id: uuid.UUID,
    svc: AccountingService = Depends(_svc),
):
    result = await svc.unlock_period(lock_id)
    return ok(data=result, message="تم فك القفل")


# ══════════════════════════════════════════════════════════
# Reports
# ══════════════════════════════════════════════════════════
@router.get("/trial-balance", summary="ميزان المراجعة")
async def trial_balance(
    fiscal_year:  int           = Query(..., ge=2000, le=2100),
    fiscal_month: Optional[int] = Query(None, ge=1, le=12),
    svc: AccountingService = Depends(_svc),
):
    """
    Trial balance from account_balances table.
    Returns debit/credit totals per account for the period.
    is_balanced=true means DR == CR ✅
    """
    result = await svc.get_trial_balance(fiscal_year, fiscal_month)
    return ok(data=result)


@router.get("/ledger/{account_code}", summary="كشف حساب")
async def account_ledger(
    account_code: str,
    date_from:    Optional[date] = Query(None),
    date_to:      Optional[date] = Query(None),
    svc: AccountingService = Depends(_svc),
):
    """Account ledger — all JE lines for a specific account."""
    from sqlalchemy import select
    from app.modules.accounting.models import JournalEntryLine, JournalEntry

    q = (
        select(JournalEntryLine, JournalEntry)
        .join(JournalEntry, JournalEntryLine.journal_entry_id == JournalEntry.id)
        .where(JournalEntryLine.tenant_id == svc.user.tenant_id)
        .where(JournalEntryLine.account_code == account_code)
        .where(JournalEntry.status == "posted")
    )
    if date_from:
        q = q.where(JournalEntry.entry_date >= date_from)
    if date_to:
        q = q.where(JournalEntry.entry_date <= date_to)

    q = q.order_by(JournalEntry.entry_date, JournalEntry.serial)
    result = await svc.db.execute(q)
    rows = result.all()

    running = Decimal("0")
    lines = []
    from decimal import Decimal
    for line, je in rows:
        running += line.debit - line.credit
        lines.append({
            "je_serial": je.serial,
            "entry_date": str(je.entry_date),
            "description": line.description,
            "debit": float(line.debit),
            "credit": float(line.credit),
            "running_balance": float(running),
            "source_doc_number": je.source_doc_number,
        })

    return ok(data={
        "account_code": account_code,
        "lines": lines,
        "total_debit": float(sum(l["debit"] for l in lines)),
        "total_credit": float(sum(l["credit"] for l in lines)),
        "closing_balance": float(running),
    })


@router.get("/dashboard", summary="ملخص محاسبي")
async def accounting_dashboard(
    fiscal_year: int = Query(default=2024),
    svc: AccountingService = Depends(_svc),
):
    """KPIs for the accounting dashboard."""
    from sqlalchemy import func, select
    from app.modules.accounting.models import JournalEntry

    # Count JEs by status
    result = await svc.db.execute(
        select(JournalEntry.status, func.count())
        .where(JournalEntry.tenant_id == svc.user.tenant_id)
        .where(JournalEntry.fiscal_year == fiscal_year)
        .group_by(JournalEntry.status)
    )
    counts = {row[0]: row[1] for row in result.all()}

    return ok(data={
        "fiscal_year": fiscal_year,
        "je_draft": counts.get("draft", 0),
        "je_posted": counts.get("posted", 0),
        "je_reversed": counts.get("reversed", 0),
        "active_locks": len(await svc._lock_repo.list_active_locks()),
    })


# ══════════════════════════════════════════════════════════
# COA Import — Excel / CSV
# ══════════════════════════════════════════════════════════
@router.post("/coa/import", status_code=201, summary="استيراد دليل الحسابات من Excel أو CSV",
             response_model=None)
async def import_coa(
    file: UploadFile = File(..., description="ملف Excel (.xlsx) أو CSV"),
    dry_run: bool = Query(default=False, description="تحقق بدون حفظ"),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    استيراد دليل الحسابات من ملف Excel (.xlsx) أو CSV.

    **الأعمدة المطلوبة في الملف:**
    - `code` / `كود الحساب`
    - `name_ar` / `اسم الحساب بالعربي`
    - `account_type` / `نوع الحساب` — asset | liability | equity | revenue | expense
    - `account_nature` / `طبيعة الحساب` — debit | credit
    - `level` / `المستوى` — 1 إلى 4
    - `postable` / `قابل للترحيل` — نعم | لا

    **اختيارية:**
    - `name_en`, `parent_code`, `opening_balance`, `notes`

    **dry_run=true** → يتحقق من الملف ويعيد النتيجة بدون حفظ.
    """
    from app.modules.accounting.coa_import import COAImportService

    content = await file.read()
    if not content:
        from app.core.exceptions import ValidationError
        raise ValidationError("الملف فارغ")

    svc = COAImportService(db, user)
    result = await svc.import_file(
        content=content,
        filename=file.filename or "upload.xlsx",
        dry_run=dry_run,
    )

    if result.has_errors:
        return ok(
            data=result.to_dict(),
            message=f"❌ يوجد {result.error_count} خطأ — لم يتم الحفظ. راجع error_details.",
        )

    action = "معاينة" if dry_run else "استيراد"
    return created(
        data=result.to_dict(),
        message=(
            f"✅ {action} ناجح — "
            f"تم إدراج {result.success_count} حساب | "
            f"تخطي {len(result.skipped)} (مكرر)"
        ),
    )


@router.get("/coa/template", summary="تحميل نموذج Excel لاستيراد الحسابات",
            response_class=StreamingResponse)
async def download_coa_template():
    """
    يُنزّل ملف Excel جاهز يمكن تعبئته واستيراده.
    يحتوي على: أعمدة صحيحة + تحقق من البيانات + تعليمات + مثال.
    """
    import io

    try:
        import openpyxl
        from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
        from openpyxl.worksheet.datavalidation import DataValidation
        from openpyxl.utils import get_column_letter
    except ImportError:
        from app.core.exceptions import ValidationError
        raise ValidationError("openpyxl غير مثبت على الخادم")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "دليل الحسابات"
    ws.sheet_view.rightToLeft = True

    # Styles
    H_FILL   = PatternFill("solid", fgColor="1F3864")
    R_FILL   = PatternFill("solid", fgColor="FCE4D6")
    O_FILL   = PatternFill("solid", fgColor="E2EFDA")
    EX_FILL  = PatternFill("solid", fgColor="FFF2CC")
    H_FONT   = Font(bold=True, color="FFFFFF", size=11, name="Arial")
    B_FONT   = Font(size=10, name="Arial")
    THIN     = Side(style="thin", color="BFBFBF")
    BRD      = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
    CTR      = Alignment(horizontal="center", vertical="center", wrap_text=True)

    COLS = [
        ("code",            "كود الحساب *",              12, True),
        ("name_ar",         "اسم الحساب بالعربي *",      30, True),
        ("name_en",         "اسم الحساب بالإنجليزي",     20, False),
        ("account_type",    "نوع الحساب *",              18, True),
        ("account_nature",  "طبيعة الحساب *",            16, True),
        ("level",           "المستوى (1-4) *",           12, True),
        ("parent_code",     "كود الحساب الأب",           14, False),
        ("postable",        "قابل للترحيل (نعم/لا) *",  18, True),
        ("opening_balance", "الرصيد الافتتاحي",          16, False),
        ("notes",           "ملاحظات",                   20, False),
    ]

    for i, (_, label, width, req) in enumerate(COLS, 1):
        c = ws.cell(row=1, column=i, value=label)
        c.font = H_FONT; c.fill = H_FILL; c.alignment = CTR; c.border = BRD
        ws.column_dimensions[get_column_letter(i)].width = width
    ws.row_dimensions[1].height = 35
    ws.freeze_panes = "A2"

    # Dropdowns
    dv_type = DataValidation(type="list",
        formula1='"asset,liability,equity,revenue,expense"', showDropDown=False)
    dv_nature = DataValidation(type="list",
        formula1='"debit,credit"', showDropDown=False)
    dv_post = DataValidation(type="list",
        formula1='"نعم,لا"', showDropDown=False)
    for dv in [dv_type, dv_nature, dv_post]:
        ws.add_data_validation(dv)
    dv_type.sqref = "D2:D500"; dv_nature.sqref = "E2:E500"; dv_post.sqref = "H2:H500"

    # Example rows
    EXAMPLES = [
        ("11",   "الأصول المتداولة",      "Current Assets",    "asset",     "debit",  2, "1",   "لا",  0,    "مجموعة"),
        ("1001", "الصندوق الرئيسي",       "Main Cash",          "asset",     "debit",  4, "11",  "نعم", 5000, ""),
        ("2101", "ذمم الموردين",          "Suppliers AP",       "liability", "credit", 4, "21",  "نعم", 0,    ""),
        ("4001", "مبيعات بضاعة",          "Merchandise Sales",  "revenue",   "credit", 4, "41",  "نعم", 0,    ""),
        ("5001", "تكلفة البضاعة المباعة", "COGS",              "expense",   "debit",  4, "51",  "نعم", 0,    ""),
        ("6001", "رواتب الموظفين",        "Employee Salaries",  "expense",   "debit",  4, "6200","نعم", 0,    ""),
    ]
    for ri, row in enumerate(EXAMPLES, 2):
        for ci, val in enumerate(row, 1):
            c = ws.cell(row=ri, column=ci, value=val)
            c.fill = EX_FILL; c.font = B_FONT; c.border = BRD
            c.alignment = Alignment(horizontal="right" if ci <= 2 else "center")

    # Empty rows
    for ri in range(len(EXAMPLES) + 2, 102):
        for ci, (_, _, _, req) in enumerate(COLS, 1):
            c = ws.cell(row=ri, column=ci)
            c.fill = R_FILL if req else O_FILL; c.border = BRD; c.font = B_FONT

    # Instructions sheet
    ws2 = wb.create_sheet("تعليمات")
    ws2.sheet_view.rightToLeft = True
    INST = [
        ("📋 تعليمات استيراد دليل الحسابات", True),
        ("", False),
        ("الصفوف الصفراء في الورقة الأولى هي أمثلة — احذفها واستبدلها ببياناتك", False),
        ("", False),
        ("الأعمدة الإلزامية (خلفية برتقالية):", True),
        ("  كود الحساب: رقم فريد — مثل 1001 أو 4001", False),
        ("  اسم الحساب بالعربي: الاسم الكامل للحساب", False),
        ("  نوع الحساب: asset / liability / equity / revenue / expense", False),
        ("  طبيعة الحساب: debit (مدين) أو credit (دائن)", False),
        ("  المستوى: من 1 إلى 4 (المستوى 4 فقط قابل للترحيل)", False),
        ("  قابل للترحيل: نعم أو لا", False),
        ("", False),
        ("المستويات الهرمية:", True),
        ("  1 = قسم رئيسي (أصول، التزامات...) — لا ترحيل", False),
        ("  2 = مجموعة (أصول متداولة، مصروفات إدارية...) — لا ترحيل", False),
        ("  3 = حساب رئيسي (النقدية، المبيعات...) — لا ترحيل", False),
        ("  4 = حساب تحليلي — يقبل القيود المحاسبية ✓", False),
        ("", False),
        ("الطبيعة الموصى بها لكل نوع:", True),
        ("  asset → debit | liability → credit | equity → credit", False),
        ("  revenue → credit | expense → debit", False),
    ]
    for ri, (txt, bold) in enumerate(INST, 1):
        c = ws2.cell(row=ri, column=1, value=txt)
        c.font = Font(bold=bold, size=11, name="Arial",
                      color="FFFFFF" if bold and txt else "000000")
        if bold and txt:
            c.fill = PatternFill("solid", fgColor="1F3864")
    ws2.column_dimensions["A"].width = 65

    # Stream response
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="coa_template.xlsx"'},
    )


@router.post("/coa/seed", status_code=201, summary="تحميل دليل الحسابات الجاهز الموصى به",
             response_model=None)
async def seed_default_coa(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    يُدرج دليل الحسابات الجاهز والموصى به للشركات السعودية.

    يحتوي على **80+ حساب** موزعة على 4 مستويات:
    - أصول (متداولة + ثابتة + غير ملموسة)
    - التزامات (متداولة + طويلة الأجل)
    - حقوق الملكية
    - إيرادات
    - تكلفة المبيعات
    - مصروفات تشغيلية + مالية + إهلاك

    متوافق مع: IFRS · IAS 1 · ZATCA

    ⚠️ إذا كان الحساب موجوداً بنفس الكود — يتم تخطيه (لا استبدال).
    """
    from app.modules.accounting.coa_import import COAImportService
    from scripts.seed_coa import COA

    user.require("can_manage_coa")

    # Convert seed data → CSV in memory → import
    import csv, io
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "code", "name_ar", "name_en", "account_type", "account_nature",
        "level", "parent_code", "postable", "opening_balance",
    ])
    writer.writeheader()
    for acc in COA:
        writer.writerow({
            "code":            acc.code,
            "name_ar":         acc.name_ar,
            "name_en":         acc.name_en,
            "account_type":    acc.account_type,
            "account_nature":  acc.account_nature,
            "level":           acc.level,
            "parent_code":     acc.parent_code or "",
            "postable":        "نعم" if acc.postable else "لا",
            "opening_balance": acc.opening_balance,
        })

    svc = COAImportService(db, user)
    result = await svc.import_file(
        content=buf.getvalue().encode("utf-8"),
        filename="seed.csv",
        dry_run=False,
    )

    return created(
        data=result.to_dict(),
        message=(
            f"✅ تم تحميل دليل الحسابات الجاهز — "
            f"{result.success_count} حساب جديد | "
            f"{len(result.skipped)} موجود مسبقاً"
        ),
    )
