"""
app/modules/inventory/router.py
══════════════════════════════════════════════════════════
Inventory & Warehouse Module — Complete API
إدارة المخزون والمستودعات

Engines: Movement + Valuation (FIFO/AVG) + Ledger + Balances
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/inventory", tags=["المخزون والمستودعات"])
TID = "00000000-0000-0000-0000-000000000001"


# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════
async def _next_serial(db: AsyncSession, tid: str, tx_type: str, tx_date: date) -> str:
    year = tx_date.year
    r = await db.execute(text("""
        UPDATE je_sequences SET last_sequence = last_sequence + 1
        WHERE tenant_id=:tid AND je_type_code=:code AND fiscal_year=:year
        RETURNING last_sequence
    """), {"tid": tid, "code": tx_type, "year": year})
    row = r.fetchone()
    if not row:
        await db.execute(text("""
            INSERT INTO je_sequences (id,tenant_id,je_type_code,fiscal_year,last_sequence)
            VALUES (gen_random_uuid(),:tid,:code,:year,1)
            ON CONFLICT (tenant_id,je_type_code,fiscal_year)
            DO UPDATE SET last_sequence=je_sequences.last_sequence+1
        """), {"tid": tid, "code": tx_type, "year": year})
        r2 = await db.execute(text("""
            SELECT last_sequence FROM je_sequences
            WHERE tenant_id=:tid AND je_type_code=:code AND fiscal_year=:year
        """), {"tid": tid, "code": tx_type, "year": year})
        seq = r2.fetchone()[0]
    else:
        seq = row[0]
    sr = await db.execute(text("""
        SELECT prefix, padding, separator FROM series_settings
        WHERE tenant_id=:tid AND je_type_code=:code LIMIT 1
    """), {"tid": tid, "code": tx_type})
    srow = sr.fetchone()
    prefix  = srow[0] if srow else tx_type
    padding = srow[1] if srow else 7
    sep     = srow[2] if srow else "-"
    return f"{prefix}{sep}{year}{sep}{seq:0{padding}d}"


async def _get_balance(db, tid, item_id, warehouse_id) -> dict:
    r = await db.execute(text("""
        SELECT qty_on_hand, qty_reserved, avg_cost, total_value
        FROM inv_balances
        WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
    """), {"tid": tid, "iid": item_id, "wid": warehouse_id})
    row = r.fetchone()
    if row:
        return {"qty_on_hand": Decimal(str(row[0])), "qty_reserved": Decimal(str(row[1])),
                "avg_cost": Decimal(str(row[2])), "total_value": Decimal(str(row[3]))}
    return {"qty_on_hand": Decimal(0), "qty_reserved": Decimal(0),
            "avg_cost": Decimal(0), "total_value": Decimal(0)}


async def _upsert_balance(db, tid, item_id, warehouse_id,
                           qty_delta: Decimal, cost_delta: Decimal, tx_date: date):
    bal = await _get_balance(db, tid, item_id, warehouse_id)
    new_qty  = bal["qty_on_hand"] + qty_delta
    new_val  = bal["total_value"] + cost_delta
    new_cost = new_val / new_qty if new_qty > 0 else Decimal(0)

    await db.execute(text("""
        INSERT INTO inv_balances
          (id,tenant_id,item_id,warehouse_id,qty_on_hand,qty_reserved,avg_cost,total_value,last_movement)
        VALUES
          (gen_random_uuid(),:tid,:iid,:wid,:qty,0,:cost,:val,:dt)
        ON CONFLICT (tenant_id,item_id,warehouse_id)
        DO UPDATE SET
          qty_on_hand  = :qty,
          avg_cost     = :cost,
          total_value  = :val,
          last_movement= :dt,
          updated_at   = NOW()
    """), {"tid": tid, "iid": item_id, "wid": warehouse_id,
           "qty": new_qty, "cost": new_cost, "val": new_val, "dt": tx_date})


async def _add_ledger(db, tid, item_id, warehouse_id, tx_type,
                       tx_date, qty_in, qty_out, unit_cost, total_cost,
                       reference_id, serial_num=None, lot_num=None):
    # حساب الرصيد التراكمي
    r = await db.execute(text("""
        SELECT COALESCE(SUM(qty_in-qty_out),0)
        FROM inv_ledger
        WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
    """), {"tid": tid, "iid": item_id, "wid": warehouse_id})
    prev_qty = Decimal(str(r.scalar() or 0))
    bal_qty  = prev_qty + qty_in - qty_out

    r2 = await db.execute(text("""
        SELECT COALESCE(SUM(total_cost*(CASE WHEN qty_in>0 THEN 1 ELSE -1 END)),0)
        FROM inv_ledger
        WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
    """), {"tid": tid, "iid": item_id, "wid": warehouse_id})
    prev_cost  = Decimal(str(r2.scalar() or 0))
    sign       = 1 if qty_in > 0 else -1
    bal_cost   = prev_cost + (total_cost * sign)

    await db.execute(text("""
        INSERT INTO inv_ledger
          (id,tenant_id,item_id,warehouse_id,tx_type,tx_date,
           qty_in,qty_out,unit_cost,total_cost,balance_qty,balance_cost,
           reference_id,reference_type,lot_number)
        VALUES
          (gen_random_uuid(),:tid,:iid,:wid,:tx_type,:tx_date,
           :qi,:qo,:uc,:tc,:bq,:bc,
           :ref_id,'INV_TX',:lot)
    """), {"tid": tid, "iid": item_id, "wid": warehouse_id,
           "tx_type": tx_type, "tx_date": tx_date,
           "qi": qty_in, "qo": qty_out, "uc": unit_cost, "tc": total_cost,
           "bq": bal_qty, "bc": bal_cost,
           "ref_id": str(reference_id), "lot": lot_num})


async def _fifo_issue(db, tid, item_id, warehouse_id, qty_needed: Decimal) -> Decimal:
    """تطبيق FIFO — يعيد إجمالي التكلفة"""
    r = await db.execute(text("""
        SELECT id, remaining_qty, unit_cost
        FROM inv_fifo_layers
        WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid AND remaining_qty > 0
        ORDER BY receipt_date, created_at
    """), {"tid": tid, "iid": item_id, "wid": warehouse_id})
    layers = r.fetchall()
    total_cost = Decimal(0)
    remaining  = qty_needed
    for layer in layers:
        if remaining <= 0:
            break
        consume    = min(Decimal(str(layer[1])), remaining)
        total_cost += consume * Decimal(str(layer[2]))
        remaining  -= consume
        await db.execute(text("""
            UPDATE inv_fifo_layers SET remaining_qty = remaining_qty - :consume
            WHERE id = :lid
        """), {"consume": consume, "lid": str(layer[0])})
    return total_cost


async def _avg_issue_cost(db, tid, item_id, warehouse_id, qty: Decimal) -> Decimal:
    """الحصول على تكلفة الصرف بطريقة المتوسط المرجح"""
    bal = await _get_balance(db, tid, item_id, warehouse_id)
    return qty * bal["avg_cost"]


async def _post_je(db, tid: str, user_email: str, tx_type: str, tx_date: date,
                   description: str, debit_acc: str, credit_acc: str,
                   amount: Decimal, reference: str = None):
    """ترحيل قيد محاسبي مرتبط بالمخزون"""
    try:
        from app.services.posting.engine import PostingEngine, PostingRequest, PostingLine
        t_id = uuid.UUID(tid)
        engine = PostingEngine(db, t_id)
        lines = [
            PostingLine(account_code=debit_acc,  description=description, debit=amount,  credit=Decimal(0)),
            PostingLine(account_code=credit_acc, description=description, debit=Decimal(0), credit=amount),
        ]
        result = await engine.post(PostingRequest(
            tenant_id=t_id, je_type=tx_type, description=description,
            entry_date=tx_date, lines=lines, created_by_email=user_email,
            reference=reference, source_module="inventory",
        ))
        return {"je_id": str(result.je_id), "je_serial": result.je_serial}
    except Exception:
        return {"je_id": None, "je_serial": None}


# ══════════════════════════════════════════════════════════
# DASHBOARD
# ══════════════════════════════════════════════════════════
@router.get("/dashboard")
async def dashboard(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    try:
        r = await db.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM inv_items WHERE tenant_id=:tid AND is_active=true) AS total_items,
          (SELECT COUNT(*) FROM inv_warehouses WHERE tenant_id=:tid AND is_active=true) AS total_warehouses,
          (SELECT COUNT(*) FROM inv_categories WHERE tenant_id=:tid) AS total_categories,
          (SELECT COALESCE(SUM(total_value),0) FROM inv_balances WHERE tenant_id=:tid) AS total_value,
          (SELECT COALESCE(SUM(qty_on_hand),0) FROM inv_balances WHERE tenant_id=:tid) AS total_qty
        """), {"tid": tid})
        krow = r.mappings().fetchone()

        # أصناف تحت الحد الأدنى
        r2 = await db.execute(text("""
        SELECT i.item_code, i.item_name, i.min_qty, i.reorder_point,
               COALESCE(SUM(b.qty_on_hand),0) AS qty_on_hand,
               u.uom_name
        FROM inv_items i
        LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
        LEFT JOIN inv_uom u ON u.id=i.uom_id
        WHERE i.tenant_id=:tid AND i.is_active=true AND i.min_qty > 0
        GROUP BY i.id, i.item_code, i.item_name, i.min_qty, i.reorder_point, u.uom_name
        HAVING COALESCE(SUM(b.qty_on_hand),0) <= i.min_qty
        LIMIT 10
        """), {"tid": tid})
        low_stock = [dict(row._mapping) for row in r2.fetchall()]

        # قيمة المخزون حسب التصنيف
        r3 = await db.execute(text("""
        SELECT c.category_name, COALESCE(SUM(b.total_value),0) AS total_value
        FROM inv_categories c
        JOIN inv_items i ON i.category_id=c.id AND i.tenant_id=:tid
        LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
        WHERE c.tenant_id=:tid
        GROUP BY c.id, c.category_name
        ORDER BY total_value DESC LIMIT 8
        """), {"tid": tid})
        by_category = [dict(row._mapping) for row in r3.fetchall()]

        # آخر الحركات
        r4 = await db.execute(text("""
        SELECT t.serial, t.tx_type, t.tx_date, t.description, t.total_cost, t.status
        FROM inv_transactions t
        WHERE t.tenant_id=:tid
        ORDER BY t.created_at DESC LIMIT 8
        """), {"tid": tid})
        recent_tx = [dict(row._mapping) for row in r4.fetchall()]

        # معدل دوران المخزون (آخر 30 يوم)
        r5 = await db.execute(text("""
        SELECT COALESCE(SUM(total_cost),0) AS cogs_30d
        FROM inv_ledger
        WHERE tenant_id=:tid AND qty_out>0
          AND tx_date >= CURRENT_DATE - 30
        """), {"tid": tid})
        cogs_30d = float(r5.scalar() or 0)
        total_val = float(krow["total_value"] or 1)
        turnover_rate = round((cogs_30d / total_val * 12) if total_val > 0 else 0, 2)

    except Exception as e:
        import traceback; print(f"[inv/dashboard] {traceback.format_exc()}")
        return ok(data={"kpis":{"total_items":0,"total_warehouses":0,"total_categories":0,"total_value":0,"total_qty":0,"low_stock_count":0,"turnover_rate":0},"low_stock":[],"by_category":[],"recent_tx":[]}, message=str(e))
    return ok(data={
        "kpis": {
            "total_items":      krow["total_items"],
            "total_warehouses": krow["total_warehouses"],
            "total_categories": krow["total_categories"],
            "total_value":      float(krow["total_value"] or 0),
            "total_qty":        float(krow["total_qty"] or 0),
            "low_stock_count":  len(low_stock),
            "turnover_rate":    turnover_rate,
        },
        "low_stock":    low_stock,
        "by_category":  by_category,
        "recent_tx":    recent_tx,
    })


# ══════════════════════════════════════════════════════════
# UOM
# ══════════════════════════════════════════════════════════
@router.get("/uom")
async def list_uom(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM inv_uom WHERE tenant_id=:tid ORDER BY uom_name"), {"tid": tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])

@router.post("/uom", status_code=201)
async def create_uom(data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    uid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO inv_uom (id,tenant_id,uom_code,uom_name,uom_name_en,is_base)
        VALUES (:id,:tid,:code,:name,:name_en,:base)
    """), {"id":uid,"tid":tid,"code":data["uom_code"],"name":data["uom_name"],
           "name_en":data.get("uom_name_en"),"base":data.get("is_base",False)})
    await db.commit()
    return created(data={"id":uid}, message="تم إنشاء وحدة القياس ✅")


# ══════════════════════════════════════════════════════════
# CATEGORIES
# ══════════════════════════════════════════════════════════
@router.get("/categories")
async def list_categories(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT c.*, p.category_name AS parent_name
        FROM inv_categories c
        LEFT JOIN inv_categories p ON p.id=c.parent_id
        WHERE c.tenant_id=:tid ORDER BY c.category_name
    """), {"tid": tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])

@router.post("/categories", status_code=201)
async def create_category(data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    cid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO inv_categories (id,tenant_id,category_code,category_name,parent_id,item_type,gl_account_code,cogs_account_code)
        VALUES (:id,:tid,:code,:name,:parent,:type,:gl,:cogs)
    """), {"id":cid,"tid":tid,"code":data["category_code"],"name":data["category_name"],
           "parent":data.get("parent_id"),"type":data.get("item_type","stock"),
           "gl":data.get("gl_account_code"),"cogs":data.get("cogs_account_code")})
    await db.commit()
    return created(data={"id":cid}, message="تم إنشاء التصنيف ✅")


# ══════════════════════════════════════════════════════════
# ITEMS (Item Master)
# ══════════════════════════════════════════════════════════
@router.get("/items")
async def list_items(
    category_id:Optional[uuid.UUID]=Query(None),
    item_type:  Optional[str]=Query(None),
    search:     Optional[str]=Query(None),
    limit: int=Query(50), offset: int=Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["i.tenant_id=:tid AND i.is_active=true"]
        params: dict = {"tid":tid,"limit":limit,"offset":offset}
        if category_id: conds.append("i.category_id=:cat"); params["cat"]=str(category_id)
        if item_type:   conds.append("i.item_type=:itype"); params["itype"]=item_type
        if search:      conds.append("(i.item_code ILIKE :s OR i.item_name ILIKE :s OR i.barcode ILIKE :s)"); params["s"]=f"%{search}%"
        where = " AND ".join(conds)
        cnt = await db.execute(text(f"SELECT COUNT(*) FROM inv_items i WHERE {where}"), params)
        r = await db.execute(text(f"""
            SELECT i.*,
                   c.category_name, u.uom_name,
                   COALESCE(SUM(b.qty_on_hand),0) AS total_qty,
                   COALESCE(SUM(b.total_value),0) AS total_value
            FROM inv_items i
            LEFT JOIN inv_categories c ON c.id=i.category_id
            LEFT JOIN inv_uom u ON u.id=i.uom_id
            LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
            WHERE {where}
            GROUP BY i.id, c.category_name, u.uom_name
            ORDER BY i.item_name LIMIT :limit OFFSET :offset
        """), params)
        return ok(data={"total":cnt.scalar(),"items":[dict(row._mapping) for row in r.fetchall()]})
    except Exception as e:
        import traceback; print(f"[inv/items] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في جلب الأصناف: {str(e)}")


@router.post("/items", status_code=201)
async def create_item(data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    iid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO inv_items
          (id,tenant_id,item_code,item_name,item_name_en,item_type,
           category_id,uom_id,barcode,cost_method,tracking_type,
           sale_price,purchase_price,avg_cost,
           min_qty,max_qty,reorder_point,reorder_qty,
           gl_account_code,cogs_account_code,description,
           allow_negative,created_by)
        VALUES
          (:id,:tid,:code,:name,:name_en,:itype,
           :cat,:uom,:barcode,:cost_method,:tracking,
           :sale,:purchase,:avg_cost,
           :min_qty,:max_qty,:reorder,:reorder_qty,
           :gl,:cogs,:desc,
           :neg,:by)
    """), {
        "id":iid,"tid":tid,"code":data["item_code"],"name":data["item_name"],
        "name_en":data.get("item_name_en"),"itype":data.get("item_type","stock"),
        "cat":data.get("category_id"),"uom":data.get("uom_id"),
        "barcode":data.get("barcode"),"cost_method":data.get("cost_method","avg"),
        "tracking":data.get("tracking_type","none"),
        "sale":data.get("sale_price",0),"purchase":data.get("purchase_price",0),
        "avg_cost":data.get("avg_cost",0),
        "min_qty":data.get("min_qty",0),"max_qty":data.get("max_qty",0),
        "reorder":data.get("reorder_point",0),"reorder_qty":data.get("reorder_qty",0),
        "gl":data.get("gl_account_code"),"cogs":data.get("cogs_account_code"),
        "desc":data.get("description"),"neg":data.get("allow_negative",False),
        "by":user.email,
    })
    await db.commit()
    return created(data={"id":iid}, message=f"تم إنشاء الصنف {data['item_code']} ✅")


@router.put("/items/{item_id}")
async def update_item(item_id: uuid.UUID, data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    data.pop("id",None); data.pop("tenant_id",None); data["updated_at"]=datetime.utcnow()
    set_clause = ", ".join([f"{k}=:{k}" for k in data.keys()])
    data.update({"id":str(item_id),"tid":tid})
    await db.execute(text(f"UPDATE inv_items SET {set_clause} WHERE id=:id AND tenant_id=:tid"), data)
    await db.commit()
    return ok(data={}, message="تم التعديل ✅")


@router.get("/items/{item_id}")
async def get_item(item_id: uuid.UUID, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT i.*, c.category_name, u.uom_name
        FROM inv_items i
        LEFT JOIN inv_categories c ON c.id=i.category_id
        LEFT JOIN inv_uom u ON u.id=i.uom_id
        WHERE i.id=:id AND i.tenant_id=:tid
    """), {"id":str(item_id),"tid":tid})
    row = r.mappings().fetchone()
    if not row: raise HTTPException(404, "الصنف غير موجود")

    # الأرصدة حسب المستودع
    r2 = await db.execute(text("""
        SELECT b.*, w.warehouse_name
        FROM inv_balances b
        JOIN inv_warehouses w ON w.id=b.warehouse_id
        WHERE b.item_id=:iid AND b.tenant_id=:tid
    """), {"iid":str(item_id),"tid":tid})
    balances = [dict(row._mapping) for row in r2.fetchall()]

    # آخر الحركات
    r3 = await db.execute(text("""
        SELECT l.*, w.warehouse_name
        FROM inv_ledger l
        JOIN inv_warehouses w ON w.id=l.warehouse_id
        WHERE l.item_id=:iid AND l.tenant_id=:tid
        ORDER BY l.tx_date DESC, l.created_at DESC LIMIT 20
    """), {"iid":str(item_id),"tid":tid})
    movements = [dict(row._mapping) for row in r3.fetchall()]

    return ok(data={"item":dict(row._mapping),"balances":balances,"movements":movements})


# ══════════════════════════════════════════════════════════
# WAREHOUSES
# ══════════════════════════════════════════════════════════
@router.get("/warehouses")
async def list_warehouses(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT w.*,
               COUNT(DISTINCT b.id) AS bin_count,
               COALESCE(SUM(ib.total_value),0) AS total_value,
               COALESCE(SUM(ib.qty_on_hand),0) AS total_qty
        FROM inv_warehouses w
        LEFT JOIN inv_bins b ON b.warehouse_id=w.id
        LEFT JOIN inv_balances ib ON ib.warehouse_id=w.id AND ib.tenant_id=:tid
        WHERE w.tenant_id=:tid
        GROUP BY w.id ORDER BY w.warehouse_name
    """), {"tid": tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])

@router.post("/warehouses", status_code=201)
async def create_warehouse(data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    wid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO inv_warehouses (id,tenant_id,warehouse_code,warehouse_name,warehouse_type,branch_code,gl_account_code,transit_account,address,notes,created_by)
        VALUES (:id,:tid,:code,:name,:type,:branch,:gl,:transit,:addr,:notes,:by)
    """), {"id":wid,"tid":tid,"code":data["warehouse_code"],"name":data["warehouse_name"],
           "type":data.get("warehouse_type","main"),"branch":data.get("branch_code"),
           "gl":data.get("gl_account_code"),"transit":data.get("transit_account"),
           "addr":data.get("address"),"notes":data.get("notes"),"by":user.email})
    await db.commit()
    return created(data={"id":wid}, message="تم إنشاء المستودع ✅")

@router.put("/warehouses/{wh_id}")
async def update_warehouse(wh_id: uuid.UUID, data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    data.pop("id",None); data.pop("tenant_id",None)
    set_clause = ", ".join([f"{k}=:{k}" for k in data.keys()])
    data.update({"id":str(wh_id),"tid":tid})
    await db.execute(text(f"UPDATE inv_warehouses SET {set_clause} WHERE id=:id AND tenant_id=:tid"), data)
    await db.commit()
    return ok(data={}, message="تم التعديل ✅")

@router.get("/warehouses/{wh_id}/bins")
async def list_bins(wh_id: uuid.UUID, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM inv_bins WHERE warehouse_id=:wid AND tenant_id=:tid ORDER BY bin_code"),
                          {"wid":str(wh_id),"tid":tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])

@router.post("/warehouses/{wh_id}/bins", status_code=201)
async def create_bin(wh_id: uuid.UUID, data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    bid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO inv_bins (id,tenant_id,warehouse_id,bin_code,bin_name,aisle,rack,shelf)
        VALUES (:id,:tid,:wid,:code,:name,:aisle,:rack,:shelf)
    """), {"id":bid,"tid":tid,"wid":str(wh_id),"code":data["bin_code"],"name":data.get("bin_name"),
           "aisle":data.get("aisle"),"rack":data.get("rack"),"shelf":data.get("shelf")})
    await db.commit()
    return created(data={"id":bid}, message="تم إنشاء الموقع ✅")


# ══════════════════════════════════════════════════════════
# INVENTORY TRANSACTIONS (GRN / GIN / GDN / GIT / IJ)
# ══════════════════════════════════════════════════════════
@router.get("/transactions")
async def list_transactions(
    tx_type:   Optional[str]=Query(None),
    status:    Optional[str]=Query(None),
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    limit: int=Query(50), offset: int=Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["t.tenant_id=:tid"]
    params: dict = {"tid":tid,"limit":limit,"offset":offset}
    if tx_type:  conds.append("t.tx_type=:tx_type"); params["tx_type"]=tx_type
    if status:   conds.append("t.status=:status");   params["status"]=status
    if date_from:conds.append("t.tx_date>=:df");     params["df"]=str(date_from)
    if date_to:  conds.append("t.tx_date<=:dt");     params["dt"]=str(date_to)
    where = " AND ".join(conds)
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM inv_transactions t WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT t.*,
               fw.warehouse_name AS from_warehouse_name,
               tw.warehouse_name AS to_warehouse_name
        FROM inv_transactions t
        LEFT JOIN inv_warehouses fw ON fw.id=t.from_warehouse_id
        LEFT JOIN inv_warehouses tw ON tw.id=t.to_warehouse_id
        WHERE {where}
        ORDER BY t.tx_date DESC, t.created_at DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total":cnt.scalar(),"items":[dict(row._mapping) for row in r.fetchall()]})


@router.post("/transactions", status_code=201)
async def create_transaction(data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    tx_date = date.fromisoformat(str(data["tx_date"]))
    tx_type = data["tx_type"]
    serial  = await _next_serial(db, tid, tx_type, tx_date)
    tx_id   = str(uuid.uuid4())
    lines   = data.get("lines", [])
    total_qty  = sum(float(l.get("qty",0)) for l in lines)
    total_cost = sum(float(l.get("qty",0)) * float(l.get("unit_cost",0)) for l in lines)

    await db.execute(text("""
        INSERT INTO inv_transactions
          (id,tenant_id,serial,tx_type,tx_date,from_warehouse_id,to_warehouse_id,
           reference,party_name,description,total_qty,total_cost,notes,status,created_by)
        VALUES
          (:id,:tid,:serial,:tx_type,:tx_date,:from_wh,:to_wh,
           :ref,:party,:desc,:tqty,:tcost,:notes,'draft',:by)
    """), {
        "id":tx_id,"tid":tid,"serial":serial,"tx_type":tx_type,"tx_date":tx_date,
        "from_wh":data.get("from_warehouse_id"),"to_wh":data.get("to_warehouse_id"),
        "ref":data.get("reference"),"party":data.get("party_name"),
        "desc":data.get("description",""),"tqty":total_qty,"tcost":total_cost,
        "notes":data.get("notes"),"by":user.email,
    })

    for i, line in enumerate(lines):
        await db.execute(text("""
            INSERT INTO inv_transaction_lines
              (id,tenant_id,transaction_id,line_order,item_id,uom_id,qty,unit_cost,total_cost,lot_number,serial_number,expiry_date,notes)
            VALUES
              (gen_random_uuid(),:tid,:tx_id,:order,:item,:uom,:qty,:uc,:tc,:lot,:sn,:exp,:notes)
        """), {
            "tid":tid,"tx_id":tx_id,"order":i+1,
            "item":line["item_id"],"uom":line.get("uom_id"),
            "qty":Decimal(str(line["qty"])),"uc":Decimal(str(line.get("unit_cost",0))),
            "tc":Decimal(str(line["qty"]))*Decimal(str(line.get("unit_cost",0))),
            "lot":line.get("lot_number"),"sn":line.get("serial_number"),
            "exp":line.get("expiry_date"),"notes":line.get("notes"),
        })

    await db.commit()
    return created(data={"id":tx_id,"serial":serial}, message=f"تم إنشاء {serial} ✅")


@router.get("/transactions/{tx_id}")
async def get_transaction(tx_id: uuid.UUID, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT t.*, fw.warehouse_name AS from_wh, tw.warehouse_name AS to_wh
        FROM inv_transactions t
        LEFT JOIN inv_warehouses fw ON fw.id=t.from_warehouse_id
        LEFT JOIN inv_warehouses tw ON tw.id=t.to_warehouse_id
        WHERE t.id=:id AND t.tenant_id=:tid
    """), {"id":str(tx_id),"tid":tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "المستند غير موجود")

    r2 = await db.execute(text("""
        SELECT l.*, i.item_name, i.item_code, u.uom_name
        FROM inv_transaction_lines l
        JOIN inv_items i ON i.id=l.item_id
        LEFT JOIN inv_uom u ON u.id=l.uom_id
        WHERE l.transaction_id=:tx_id
        ORDER BY l.line_order
    """), {"tx_id":str(tx_id)})
    lines = [dict(row._mapping) for row in r2.fetchall()]
    return ok(data={"transaction":dict(tx._mapping),"lines":lines})


@router.post("/transactions/{tx_id}/post")
async def post_transaction(tx_id: uuid.UUID, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)

    # جلب المستند
    r = await db.execute(text("""
        SELECT t.* FROM inv_transactions t WHERE t.id=:id AND t.tenant_id=:tid
    """), {"id":str(tx_id),"tid":tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "المستند غير موجود")
    if tx["status"] != "draft": raise HTTPException(400, "المستند مُرحَّل مسبقاً")

    # جلب الأسطر
    r2 = await db.execute(text("""
        SELECT l.*, i.cost_method, i.gl_account_code, i.cogs_account_code
        FROM inv_transaction_lines l
        JOIN inv_items i ON i.id=l.item_id
        WHERE l.transaction_id=:tx_id
    """), {"tx_id":str(tx_id)})
    lines = r2.mappings().all()

    # جلب إعدادات الحسابات
    r3 = await db.execute(text("""
        SELECT debit_account, credit_account FROM inv_account_settings
        WHERE tenant_id=:tid AND tx_type=:tx_type
    """), {"tid":tid,"tx_type":tx["tx_type"]})
    acc = r3.fetchone()
    debit_acc  = acc[0] if acc else "130101"
    credit_acc = acc[1] if acc else "210101"

    total_cost = Decimal(0)

    for line in lines:
        qty       = Decimal(str(line["qty"]))
        unit_cost = Decimal(str(line["unit_cost"] or 0))
        item_id   = str(line["item_id"])
        wh_id     = str(tx["from_warehouse_id"] or tx["to_warehouse_id"])
        tx_type   = tx["tx_type"]

        if tx_type == "GRN":
            # استلام — زيادة المخزون
            line_cost = qty * unit_cost
            # تحديث FIFO layer
            await db.execute(text("""
                INSERT INTO inv_fifo_layers
                  (id,tenant_id,item_id,warehouse_id,receipt_date,original_qty,remaining_qty,unit_cost,reference_id)
                VALUES
                  (gen_random_uuid(),:tid,:iid,:wid,:dt,:qty,:qty,:uc,:ref)
            """), {"tid":tid,"iid":item_id,"wid":str(tx["to_warehouse_id"]),"dt":tx["tx_date"],
                   "qty":qty,"uc":unit_cost,"ref":str(tx_id)})
            await _upsert_balance(db,tid,item_id,str(tx["to_warehouse_id"]),qty,line_cost,tx["tx_date"])
            await _add_ledger(db,tid,item_id,str(tx["to_warehouse_id"]),tx_type,
                              tx["tx_date"],qty,Decimal(0),unit_cost,line_cost,tx_id)
            total_cost += line_cost

            # تحديث متوسط التكلفة في بطاقة الصنف
            await db.execute(text("""
                UPDATE inv_items SET avg_cost = (
                    SELECT avg_cost FROM inv_balances
                    WHERE item_id=:iid AND tenant_id=:tid
                    ORDER BY updated_at DESC LIMIT 1
                ), updated_at=NOW()
                WHERE id=:iid AND tenant_id=:tid
            """), {"iid":item_id,"tid":tid})

        elif tx_type in ("GIN","GDN"):
            # صرف / تسليم — نقص المخزون
            wh_from = str(tx["from_warehouse_id"])
            if line["cost_method"] == "fifo":
                line_cost = await _fifo_issue(db,tid,item_id,wh_from,qty)
            else:
                line_cost = await _avg_issue_cost(db,tid,item_id,wh_from,qty)
            await _upsert_balance(db,tid,item_id,wh_from,-qty,-line_cost,tx["tx_date"])
            await _add_ledger(db,tid,item_id,wh_from,tx_type,
                              tx["tx_date"],Decimal(0),qty,line_cost/qty if qty else Decimal(0),line_cost,tx_id)
            total_cost += line_cost

        elif tx_type == "GIT":
            # تحويل داخلي
            wh_from = str(tx["from_warehouse_id"])
            wh_to   = str(tx["to_warehouse_id"])
            if line["cost_method"] == "fifo":
                line_cost = await _fifo_issue(db,tid,item_id,wh_from,qty)
            else:
                line_cost = await _avg_issue_cost(db,tid,item_id,wh_from,qty)
            unit_c = line_cost/qty if qty else Decimal(0)
            await _upsert_balance(db,tid,item_id,wh_from,-qty,-line_cost,tx["tx_date"])
            await _upsert_balance(db,tid,item_id,wh_to,  qty, line_cost,tx["tx_date"])
            await _add_ledger(db,tid,item_id,wh_from,tx_type,tx["tx_date"],Decimal(0),qty,unit_c,line_cost,tx_id)
            await _add_ledger(db,tid,item_id,wh_to,  tx_type,tx["tx_date"],qty,Decimal(0),unit_c,line_cost,tx_id)
            total_cost += line_cost

        elif tx_type == "IJ":
            # تسوية مخزون
            wh_id_ij = str(tx["from_warehouse_id"] or tx["to_warehouse_id"])
            bal      = await _get_balance(db,tid,item_id,wh_id_ij)
            sys_qty  = bal["qty_on_hand"]
            diff     = qty - sys_qty  # موجب=زيادة, سالب=نقص
            uc       = bal["avg_cost"]
            diff_cost = diff * uc
            await _upsert_balance(db,tid,item_id,wh_id_ij,diff,diff_cost,tx["tx_date"])
            qi = diff if diff>0 else Decimal(0)
            qo = -diff if diff<0 else Decimal(0)
            await _add_ledger(db,tid,item_id,wh_id_ij,tx_type,tx["tx_date"],qi,qo,uc,abs(diff_cost),tx_id)
            total_cost += abs(diff_cost)

    # قيد محاسبي
    je = await _post_je(db,tid,user.email,tx["tx_type"],tx["tx_date"],
                        tx["description"] or tx["serial"],
                        debit_acc,credit_acc,total_cost,tx["serial"])

    await db.execute(text("""
        UPDATE inv_transactions
        SET status='posted', je_id=:je_id, je_serial=:je_serial,
            posted_by=:by, posted_at=NOW(), total_cost=:tc
        WHERE id=:id AND tenant_id=:tid
    """), {"je_id":je["je_id"],"je_serial":je["je_serial"],"by":user.email,
           "tc":total_cost,"id":str(tx_id),"tid":tid})
    await db.commit()
    return ok(data={"je_serial":je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


@router.delete("/transactions/{tx_id}")
async def cancel_transaction(tx_id: uuid.UUID, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE inv_transactions SET status='cancelled'
        WHERE id=:id AND tenant_id=:tid AND status='draft'
    """), {"id":str(tx_id),"tid":tid})
    await db.commit()
    return ok(data={}, message="تم إلغاء المستند")


# ══════════════════════════════════════════════════════════
# PHYSICAL COUNT (جرد فعلي)
# ══════════════════════════════════════════════════════════
@router.get("/count-sessions")
async def list_count_sessions(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT cs.*, w.warehouse_name
        FROM inv_count_sessions cs
        LEFT JOIN inv_warehouses w ON w.id=cs.warehouse_id
        WHERE cs.tenant_id=:tid ORDER BY cs.count_date DESC
    """), {"tid":tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/count-sessions", status_code=201)
async def create_count_session(data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    count_date = date.fromisoformat(str(data.get("count_date", date.today())))
    serial  = await _next_serial(db, tid, "PIC", count_date)
    sess_id = str(uuid.uuid4())
    wh_id   = str(data["warehouse_id"])

    await db.execute(text("""
        INSERT INTO inv_count_sessions (id,tenant_id,serial,count_date,warehouse_id,count_type,status,notes,created_by)
        VALUES (:id,:tid,:serial,:dt,:wid,:type,'open',:notes,:by)
    """), {"id":sess_id,"tid":tid,"serial":serial,"dt":count_date,
           "wid":wh_id,"type":data.get("count_type","full"),"notes":data.get("notes"),"by":user.email})

    # إنشاء أسطر الجرد بشكل تلقائي من الأرصدة الحالية
    r = await db.execute(text("""
        SELECT b.item_id, b.qty_on_hand, b.avg_cost, i.item_name, i.item_code
        FROM inv_balances b
        JOIN inv_items i ON i.id=b.item_id
        WHERE b.warehouse_id=:wid AND b.tenant_id=:tid AND i.is_active=true
        ORDER BY i.item_name
    """), {"wid":wh_id,"tid":tid})
    items = r.fetchall()

    for item in items:
        await db.execute(text("""
            INSERT INTO inv_count_lines (id,tenant_id,session_id,item_id,system_qty,actual_qty,unit_cost)
            VALUES (gen_random_uuid(),:tid,:sess_id,:iid,:sys_qty,NULL,:uc)
        """), {"tid":tid,"sess_id":sess_id,"iid":str(item[0]),"sys_qty":item[1],"uc":item[3]})

    await db.commit()
    return created(data={"id":sess_id,"serial":serial,"item_count":len(items)},
                   message=f"تم إنشاء جلسة الجرد {serial} — {len(items)} صنف")


@router.get("/count-sessions/{sess_id}/lines")
async def get_count_lines(sess_id: uuid.UUID, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT cl.*, i.item_code, i.item_name, u.uom_name
        FROM inv_count_lines cl
        JOIN inv_items i ON i.id=cl.item_id
        LEFT JOIN inv_uom u ON u.id=i.uom_id
        WHERE cl.session_id=:sid AND cl.tenant_id=:tid
        ORDER BY i.item_name
    """), {"sid":str(sess_id),"tid":tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.put("/count-sessions/{sess_id}/lines/{line_id}")
async def update_count_line(sess_id: uuid.UUID, line_id: uuid.UUID, data: dict,
                             db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    actual_qty = Decimal(str(data["actual_qty"]))
    r = await db.execute(text("SELECT unit_cost, system_qty FROM inv_count_lines WHERE id=:id AND tenant_id=:tid"),
                          {"id":str(line_id),"tid":tid})
    row = r.fetchone()
    if not row: raise HTTPException(404)
    uc = Decimal(str(row[0] or 0))
    sys_qty = Decimal(str(row[1] or 0))
    variance_val = (actual_qty - sys_qty) * uc

    await db.execute(text("""
        UPDATE inv_count_lines
        SET actual_qty=:aq, variance_value=:vv
        WHERE id=:id AND tenant_id=:tid
    """), {"aq":actual_qty,"vv":variance_val,"id":str(line_id),"tid":tid})
    await db.commit()
    return ok(data={"variance":float(actual_qty-sys_qty),"variance_value":float(variance_val)})


@router.post("/count-sessions/{sess_id}/post")
async def post_count_session(sess_id: uuid.UUID, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM inv_count_sessions WHERE id=:id AND tenant_id=:tid"),
                          {"id":str(sess_id),"tid":tid})
    sess = r.mappings().fetchone()
    if not sess: raise HTTPException(404)
    if sess["status"] == "posted": raise HTTPException(400,"مُرحَّلة مسبقاً")

    r2 = await db.execute(text("""
        SELECT cl.*, i.cost_method
        FROM inv_count_lines cl
        JOIN inv_items i ON i.id=cl.item_id
        WHERE cl.session_id=:sid AND cl.tenant_id=:tid
          AND cl.actual_qty IS NOT NULL AND cl.actual_qty != cl.system_qty
    """), {"sid":str(sess_id),"tid":tid})
    diff_lines = r2.mappings().all()

    total_adj = Decimal(0)
    for line in diff_lines:
        diff = Decimal(str(line["actual_qty"])) - Decimal(str(line["system_qty"]))
        uc   = Decimal(str(line["unit_cost"] or 0))
        cost = diff * uc
        await _upsert_balance(db,tid,str(line["item_id"]),str(sess["warehouse_id"]),diff,cost,sess["count_date"])
        qi = diff if diff>0 else Decimal(0)
        qo = -diff if diff<0 else Decimal(0)
        await _add_ledger(db,tid,str(line["item_id"]),str(sess["warehouse_id"]),"PIC",
                          sess["count_date"],qi,qo,uc,abs(cost),sess_id)
        total_adj += abs(cost)

    await db.execute(text("""
        UPDATE inv_count_sessions SET status='posted', posted_by=:by, posted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"by":user.email,"id":str(sess_id),"tid":tid})
    await db.commit()
    return ok(data={"adjusted_lines":len(diff_lines),"total_adjustment":float(total_adj)},
              message=f"✅ تم ترحيل الجرد — {len(diff_lines)} صنف بفارق")


# ══════════════════════════════════════════════════════════
# STOCK INQUIRY
# ══════════════════════════════════════════════════════════
@router.get("/stock-inquiry")
async def stock_inquiry(
    warehouse_id: Optional[uuid.UUID]=Query(None),
    category_id:  Optional[uuid.UUID]=Query(None),
    search:       Optional[str]=Query(None),
    low_stock:    bool=Query(False),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["i.tenant_id=:tid AND i.is_active=true"]
    params: dict = {"tid":tid}
    if warehouse_id: conds.append("b.warehouse_id=:wid"); params["wid"]=str(warehouse_id)
    if category_id:  conds.append("i.category_id=:cat"); params["cat"]=str(category_id)
    if search:       conds.append("(i.item_code ILIKE :s OR i.item_name ILIKE :s)"); params["s"]=f"%{search}%"
    if low_stock:    conds.append("COALESCE(b.qty_on_hand,0) <= i.min_qty AND i.min_qty > 0")
    where = " AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT i.item_code, i.item_name, i.item_type, i.min_qty, i.reorder_point,
               c.category_name, u.uom_name, w.warehouse_name,
               COALESCE(b.qty_on_hand,0) AS qty_on_hand,
               COALESCE(b.qty_reserved,0) AS qty_reserved,
               COALESCE(b.qty_on_hand,0)-COALESCE(b.qty_reserved,0) AS qty_available,
               COALESCE(b.avg_cost,0) AS avg_cost,
               COALESCE(b.total_value,0) AS total_value,
               b.last_movement,
               CASE WHEN COALESCE(b.qty_on_hand,0) <= i.min_qty AND i.min_qty>0 THEN true ELSE false END AS is_low
        FROM inv_items i
        LEFT JOIN inv_categories c ON c.id=i.category_id
        LEFT JOIN inv_uom u ON u.id=i.uom_id
        LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
        LEFT JOIN inv_warehouses w ON w.id=b.warehouse_id
        WHERE {where}
        ORDER BY i.item_name, w.warehouse_name
    """), params)
    rows = [dict(row._mapping) for row in r.fetchall()]
    return ok(data={"items":rows,"total_value":sum(float(r.get("total_value",0)) for r in rows)})


# ══════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════
@router.get("/reports/stock-balance")
async def stock_balance_report(
    warehouse_id: Optional[uuid.UUID]=Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["b.tenant_id=:tid"]
    params: dict = {"tid":tid}
    if warehouse_id: conds.append("b.warehouse_id=:wid"); params["wid"]=str(warehouse_id)
    where = " AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT i.item_code, i.item_name, c.category_name, u.uom_name,
               w.warehouse_name, b.qty_on_hand, b.qty_reserved,
               b.qty_on_hand-b.qty_reserved AS qty_available,
               b.avg_cost, b.total_value, b.last_movement
        FROM inv_balances b
        JOIN inv_items i ON i.id=b.item_id
        LEFT JOIN inv_categories c ON c.id=i.category_id
        LEFT JOIN inv_uom u ON u.id=i.uom_id
        JOIN inv_warehouses w ON w.id=b.warehouse_id
        WHERE {where} AND b.qty_on_hand != 0
        ORDER BY c.category_name, i.item_name
    """), params)
    rows = [dict(row._mapping) for row in r.fetchall()]
    return ok(data={"items":rows,"total_value":sum(float(r["total_value"] or 0) for r in rows),"as_of":date.today().isoformat()})


@router.get("/reports/stock-movement")
async def stock_movement_report(
    item_id:      Optional[uuid.UUID]=Query(None),
    warehouse_id: Optional[uuid.UUID]=Query(None),
    date_from:    Optional[date]=Query(None),
    date_to:      Optional[date]=Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["l.tenant_id=:tid"]
    params: dict = {"tid":tid}
    if item_id:      conds.append("l.item_id=:iid");      params["iid"]=str(item_id)
    if warehouse_id: conds.append("l.warehouse_id=:wid"); params["wid"]=str(warehouse_id)
    if date_from:    conds.append("l.tx_date>=:df");       params["df"]=str(date_from)
    if date_to:      conds.append("l.tx_date<=:dt");       params["dt"]=str(date_to)
    where = " AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT l.tx_date, l.tx_type, l.qty_in, l.qty_out, l.unit_cost, l.total_cost,
               l.balance_qty, l.balance_cost, l.lot_number,
               i.item_code, i.item_name, w.warehouse_name
        FROM inv_ledger l
        JOIN inv_items i ON i.id=l.item_id
        JOIN inv_warehouses w ON w.id=l.warehouse_id
        WHERE {where}
        ORDER BY l.tx_date DESC, l.created_at DESC
        LIMIT 500
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.get("/reports/valuation")
async def valuation_report(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT c.category_name,
               COUNT(DISTINCT i.id) AS item_count,
               COALESCE(SUM(b.qty_on_hand),0) AS total_qty,
               COALESCE(SUM(b.total_value),0) AS total_value,
               COALESCE(AVG(b.avg_cost),0) AS avg_cost
        FROM inv_categories c
        JOIN inv_items i ON i.category_id=c.id AND i.tenant_id=:tid
        LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
        WHERE c.tenant_id=:tid
        GROUP BY c.id, c.category_name
        ORDER BY total_value DESC
    """), {"tid":tid})
    rows = [dict(row._mapping) for row in r.fetchall()]
    total = sum(float(r["total_value"] or 0) for r in rows)
    return ok(data={"categories":rows,"grand_total":total,"as_of":date.today().isoformat()})


@router.get("/reports/aging")
async def aging_report(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT i.item_code, i.item_name, c.category_name, u.uom_name,
               b.qty_on_hand, b.avg_cost, b.total_value, b.last_movement,
               CURRENT_DATE - b.last_movement AS days_since_movement,
               CASE
                 WHEN b.last_movement IS NULL THEN 'غير محدد'
                 WHEN CURRENT_DATE - b.last_movement <= 30  THEN '0-30 يوم'
                 WHEN CURRENT_DATE - b.last_movement <= 90  THEN '31-90 يوم'
                 WHEN CURRENT_DATE - b.last_movement <= 180 THEN '91-180 يوم'
                 ELSE 'أكثر من 180 يوم (راكد)'
               END AS aging_bucket
        FROM inv_balances b
        JOIN inv_items i ON i.id=b.item_id
        LEFT JOIN inv_categories c ON c.id=i.category_id
        LEFT JOIN inv_uom u ON u.id=i.uom_id
        WHERE b.tenant_id=:tid AND b.qty_on_hand > 0
        ORDER BY days_since_movement DESC NULLS LAST
    """), {"tid":tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])



# ══════════════════════════════════════════════════════════
# ADDITIONAL REPORTS
# ══════════════════════════════════════════════════════════

@router.get("/reports/cogs")
async def cogs_report(
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["l.tenant_id=:tid","l.qty_out>0"]
        params: dict = {"tid": tid}
        if date_from: conds.append("l.tx_date>=:df"); params["df"] = str(date_from)
        if date_to:   conds.append("l.tx_date<=:dt"); params["dt"] = str(date_to)
        r = await db.execute(text(f"""
            SELECT i.item_code, i.item_name,
                   SUM(l.qty_out) AS qty_sold, AVG(l.unit_cost) AS avg_cost,
                   SUM(l.total_cost) AS total_cogs
            FROM inv_ledger l
            JOIN inv_items i ON i.id=l.item_id
            WHERE {" AND ".join(conds)}
            GROUP BY i.id, i.item_code, i.item_name
            ORDER BY total_cogs DESC
        """), params)
        rows = [{**dict(r._mapping), "qty_sold":float(r._mapping["qty_sold"] or 0),
                 "avg_cost":float(r._mapping["avg_cost"] or 0),
                 "total_cogs":float(r._mapping["total_cogs"] or 0)}
                for r in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        return ok(data=[], message=str(e))


@router.get("/reports/turnover")
async def turnover_report(
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["l.tenant_id=:tid","l.qty_out>0"]
        params: dict = {"tid": tid}
        if date_from: conds.append("l.tx_date>=:df"); params["df"] = str(date_from)
        if date_to:   conds.append("l.tx_date<=:dt"); params["dt"] = str(date_to)
        r = await db.execute(text(f"""
            SELECT i.item_code, i.item_name,
                   SUM(l.total_cost) AS cogs,
                   AVG(COALESCE(b.total_value,0)) AS avg_inventory,
                   CASE WHEN AVG(COALESCE(b.total_value,0))>0
                        THEN SUM(l.total_cost)/AVG(COALESCE(b.total_value,0)) ELSE 0 END AS turnover_rate
            FROM inv_ledger l
            JOIN inv_items i ON i.id=l.item_id
            LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
            WHERE {" AND ".join(conds)}
            GROUP BY i.id, i.item_code, i.item_name
            ORDER BY turnover_rate DESC
        """), params)
        items = [{**dict(r._mapping),
                  "cogs":float(r._mapping["cogs"] or 0),
                  "avg_inventory":float(r._mapping["avg_inventory"] or 0),
                  "turnover_rate":float(r._mapping["turnover_rate"] or 0)}
                 for r in r.fetchall()]
        total_cogs = sum(i["cogs"] for i in items)
        total_inv  = sum(i["avg_inventory"] for i in items)
        return ok(data={"items":items,"summary":{
            "overall_turnover":round(total_cogs/total_inv,2) if total_inv>0 else 0,
            "total_cogs":round(total_cogs,2)}})
    except Exception as e:
        return ok(data={"items":[],"summary":{}}, message=str(e))


@router.get("/reports/variance")
async def variance_report(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT s.serial, s.count_date::text, s.warehouse_name,
                   s.lines_with_variance, s.total_variance_value
            FROM inv_count_sessions s
            WHERE s.tenant_id=:tid AND s.status='posted'
            ORDER BY s.count_date DESC
        """), {"tid": tid})
        sessions = [dict(row._mapping) for row in r.fetchall()]
        for sess in sessions:
            r2 = await db.execute(text("""
                SELECT l.item_code, l.item_name, l.system_qty, l.actual_qty,
                       l.variance, l.variance_value
                FROM inv_count_lines l
                JOIN inv_count_sessions s ON s.id=l.session_id
                WHERE s.serial=:serial AND s.tenant_id=:tid
                  AND COALESCE(l.variance,0)!=0
            """), {"serial":sess["serial"], "tid":tid})
            sess["lines"] = [dict(row._mapping) for row in r2.fetchall()]
        return ok(data=sessions)
    except Exception as e:
        return ok(data=[], message=str(e))


@router.get("/reports/negative-stock")
async def negative_stock_report(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT i.item_code, i.item_name, u.uom_name,
                   w.warehouse_name, b.qty_on_hand, b.avg_cost, b.total_value
            FROM inv_balances b
            JOIN inv_items i ON i.id=b.item_id
            LEFT JOIN inv_uom u ON u.id=i.uom_id
            JOIN inv_warehouses w ON w.id=b.warehouse_id
            WHERE b.tenant_id=:tid AND b.qty_on_hand < 0
            ORDER BY b.qty_on_hand ASC
        """), {"tid":tid})
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


@router.get("/reports/expiry")
async def expiry_report(
    days_ahead: int = Query(default=90),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT i.item_code, i.item_name, u.uom_name,
                   w.warehouse_name, tl.lot_number, tl.expiry_date::text,
                   tl.qty AS qty_on_hand,
                   (tl.expiry_date - CURRENT_DATE) AS days_to_expiry
            FROM inv_transaction_lines tl
            JOIN inv_transactions t ON t.id=tl.tx_id AND t.tenant_id=:tid AND t.status='posted'
            JOIN inv_items i ON i.id=tl.item_id
            LEFT JOIN inv_uom u ON u.id=i.uom_id
            JOIN inv_warehouses w ON w.id=t.to_warehouse_id
            WHERE tl.expiry_date IS NOT NULL
              AND tl.expiry_date <= CURRENT_DATE + (:days * INTERVAL '1 day')
            ORDER BY tl.expiry_date ASC
        """), {"tid":tid,"days":days_ahead})
        rows = [{**dict(r._mapping),"days_to_expiry":int(r._mapping["days_to_expiry"] or 0)}
                for r in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        return ok(data=[], message=str(e))


# ══════════════════════════════════════════════════════════
# ACCOUNT SETTINGS
# ══════════════════════════════════════════════════════════
@router.get("/settings/accounts")
async def get_account_settings(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM inv_account_settings WHERE tenant_id=:tid ORDER BY tx_type"), {"tid":tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])

@router.put("/settings/accounts/{tx_type}")
async def update_account_settings(tx_type: str, data: dict, db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    tid = str(user.tenant_id)
    await db.execute(text("""
        INSERT INTO inv_account_settings (id,tenant_id,tx_type,debit_account,credit_account,description)
        VALUES (gen_random_uuid(),:tid,:tx_type,:dr,:cr,:desc)
        ON CONFLICT (tenant_id,tx_type)
        DO UPDATE SET debit_account=:dr, credit_account=:cr, description=:desc
    """), {"tid":tid,"tx_type":tx_type,"dr":data.get("debit_account"),
           "cr":data.get("credit_account"),"desc":data.get("description")})
    await db.commit()
    return ok(data={}, message="تم حفظ الإعدادات ✅")
