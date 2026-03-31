"""
app/api/v1/ai_narrative.py
══════════════════════════════════════════════════════════
AI Narrative Router
المعمارية: Frontend → Railway Backend → Anthropic API
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import json
import os
from typing import Optional

import anthropic
import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/ai", tags=["الذكاء الاصطناعي"])


# ── Pydantic Models ───────────────────────────────────────────────────────────

class JELine(BaseModel):
    account_code: str
    account_name: str
    account_type: Optional[str] = ""
    description:  Optional[str] = ""
    debit:        float = 0.0
    credit:       float = 0.0


class NarrativeRequest(BaseModel):
    entry_date:   str
    je_type:      Optional[str] = "JV"
    reference:    Optional[str] = None
    description:  Optional[str] = ""
    lines:        list[JELine]
    extra_notes:  Optional[str] = None


class NarrativeResponse(BaseModel):
    narrative:  str
    summary:    str
    risks:      list[str]
    audit_note: str


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/narrative", response_model=NarrativeResponse)
async def generate_narrative(req: NarrativeRequest):
    """
    يولّد سرداً محاسبياً احترافياً للقيد باستخدام Claude.
    يُستدعى من Frontend فقط عبر:
        POST {API_BASE}/api/v1/ai/narrative
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="خدمة الذكاء الاصطناعي غير مهيأة — أضف ANTHROPIC_API_KEY في Railway"
        )

    # ── بناء ملخص الأسطر ─────────────────────────────────────────────────────
    total_debit  = 0.0
    total_credit = 0.0
    lines_text   = ""

    for i, line in enumerate(req.lines, 1):
        dr = line.debit  or 0.0
        cr = line.credit or 0.0
        total_debit  += dr
        total_credit += cr
        side       = f"مدين {dr:,.2f}" if dr > 0 else f"دائن {cr:,.2f}"
        type_label = _type_label(line.account_type)
        lines_text += f"  {i}. [{line.account_code}] {line.account_name} ({type_label}) — {side}"
        if line.description:
            lines_text += f" | بيان: {line.description}"
        lines_text += "\n"

    balanced = abs(total_debit - total_credit) < 0.01

    # ── Prompts ───────────────────────────────────────────────────────────────
    system_prompt = (
        "أنت محاسب قانوني معتمد (CPA) وخبير في المعايير الدولية لإعداد التقارير المالية IFRS "
        "والمعايير المحاسبية السعودية (SOCPA).\n"
        "مهمتك: تحليل قيد يومية وإعداد سرد محاسبي احترافي واضح.\n"
        "أسلوب الإجابة:\n"
        "- دقيق ومهني بدون حشو\n"
        "- مُركّز على المبررات المحاسبية\n"
        "- يُشير إلى المخاطر إن وُجدت\n"
        "- يراعي السياق السعودي (VAT، زكاة، SOCPA إن انطبق)\n"
        "الإجابة حصراً بصيغة JSON صالح بدون أي نص خارجه."
    )

    user_prompt = (
        f"حلّل القيد التالي وأعد استجابة JSON:\n\n"
        f"معلومات القيد:\n"
        f"- التاريخ: {req.entry_date}\n"
        f"- النوع: {req.je_type}\n"
        f"- البيان: {req.description or 'غير محدد'}\n"
        f"- المرجع: {req.reference or '—'}\n"
        f"- إجمالي المدين: {total_debit:,.2f} ريال\n"
        f"- إجمالي الدائن: {total_credit:,.2f} ريال\n"
        f"- الحالة: {'✅ متوازن' if balanced else '⚠️ غير متوازن'}\n\n"
        f"أسطر القيد:\n{lines_text}"
    )

    if req.extra_notes:
        user_prompt += f"\nملاحظات المحاسب: {req.extra_notes}"

    user_prompt += (
        '\n\nأعد JSON بهذا الشكل الحرفي بدون أي نص إضافي:\n'
        '{\n'
        '  "narrative": "وصف تفصيلي مهني للقيد ومبرره المحاسبي والأثر على القوائم المالية (3-5 جمل)",\n'
        '  "summary": "ملخص في جملة واحدة (أقل من 15 كلمة)",\n'
        '  "risks": ["خطر أو ملاحظة 1", "خطر أو ملاحظة 2"],\n'
        '  "audit_note": "ملاحظة للمراجع الخارجي (جملة واحدة)"\n'
        '}'
    )

    # ── استدعاء Anthropic ─────────────────────────────────────────────────────
    try:
        client  = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model      = "claude-sonnet-4-20250514",
            max_tokens = 1024,
            system     = system_prompt,
            messages   = [{"role": "user", "content": user_prompt}],
        )
        raw = message.content[0].text.strip()

        # إزالة markdown fences إن وُجدت
        if raw.startswith("```"):
            parts = raw.split("```")
            raw   = parts[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        data = json.loads(raw)

        logger.info("ai_narrative_generated", je_type=req.je_type, date=req.entry_date)

        return NarrativeResponse(
            narrative  = data.get("narrative",   "لم يتم توليد وصف"),
            summary    = data.get("summary",     ""),
            risks      = data.get("risks",       []),
            audit_note = data.get("audit_note",  ""),
        )

    except anthropic.AuthenticationError:
        logger.error("anthropic_auth_error")
        raise HTTPException(status_code=401, detail="مفتاح ANTHROPIC_API_KEY غير صحيح")

    except anthropic.RateLimitError:
        logger.warning("anthropic_rate_limit")
        raise HTTPException(status_code=429, detail="تم تجاوز حد الطلبات — حاول بعد قليل")

    except json.JSONDecodeError as e:
        logger.error("ai_json_parse_error", error=str(e))
        raise HTTPException(status_code=500, detail="خطأ في تحليل استجابة الذكاء الاصطناعي")

    except Exception as e:
        logger.error("ai_narrative_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"خطأ في توليد السرد: {str(e)}")


# ── Helper ────────────────────────────────────────────────────────────────────

def _type_label(account_type: str | None) -> str:
    return {
        "asset":     "أصول",
        "liability": "خصوم",
        "equity":    "حقوق ملكية",
        "revenue":   "إيرادات",
        "expense":   "مصروفات",
    }.get(account_type or "", account_type or "غير محدد")
