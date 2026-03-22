"""
app/modules/dimensions/schemas.py
"""
from __future__ import annotations

import uuid
from typing import List, Optional
from pydantic import BaseModel, Field


class DimensionValueCreate(BaseModel):
    code: str     = Field(..., min_length=1, max_length=50)
    name_ar: str  = Field(..., min_length=1, max_length=255)
    name_en: Optional[str] = Field(None, max_length=255)


class DimensionValueUpdate(BaseModel):
    name_ar: Optional[str] = Field(None, min_length=1, max_length=255)
    name_en: Optional[str] = Field(None, max_length=255)
    is_active: Optional[bool] = None


class DimensionValueResponse(BaseModel):
    id: uuid.UUID
    code: str
    name_ar: str
    name_en: Optional[str]
    is_active: bool
    model_config = {"from_attributes": True}


class DimensionCreate(BaseModel):
    code: str           = Field(..., min_length=1, max_length=50)
    name_ar: str        = Field(..., min_length=1, max_length=255)
    name_en: Optional[str] = Field(None, max_length=255)
    classification: Optional[str] = Field(
        None, pattern="^(where|who|why|expense_only)?$"
    )
    is_required: bool = False
    sort_order: int   = 0


class DimensionUpdate(BaseModel):
    name_ar: Optional[str]        = Field(None, min_length=1, max_length=255)
    name_en: Optional[str]        = Field(None, max_length=255)
    classification: Optional[str] = None
    is_required: Optional[bool]   = None
    is_active: Optional[bool]     = None
    sort_order: Optional[int]     = None


class DimensionResponse(BaseModel):
    id: uuid.UUID
    code: str
    name_ar: str
    name_en: Optional[str]
    classification: Optional[str]
    is_required: bool
    is_system: bool
    is_active: bool
    sort_order: int
    values: List[DimensionValueResponse] = []
    model_config = {"from_attributes": True}


class DimensionListItem(BaseModel):
    id: uuid.UUID
    code: str
    name_ar: str
    name_en: Optional[str]
    classification: Optional[str]
    is_required: bool
    is_system: bool
    is_active: bool
    sort_order: int
    values_count: int = 0
    model_config = {"from_attributes": True}
