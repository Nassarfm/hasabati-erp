"""
app/middleware/request_state.py
يحفظ بيانات المستخدم في request.state لكي يقرأها AuditMiddleware
"""
from fastapi import Request
from app.core.tenant import CurrentUser


def set_user_state(request: Request, user: CurrentUser) -> None:
    """استدعاء هذه الدالة من get_current_user لحفظ بيانات المستخدم"""
    request.state.user_email   = user.email
    request.state.user_id      = str(user.user_id) if user.user_id else None
    request.state.tenant_id    = str(user.tenant_id)
    request.state.display_name = getattr(user, "display_name", None) or user.email.split("@")[0]
