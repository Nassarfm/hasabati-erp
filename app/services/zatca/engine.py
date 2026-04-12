"""
app/services/zatca/engine.py
══════════════════════════════════════════════════════════
ZATCA Compliance Engine
هيئة الزكاة والضريبة والجمارك — محرك الفاتورة الإلكترونية

Phase 1: QR Code (TLV Base64) + Hash
Phase 2: XML UBL 2.1 + Digital Signature + API
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import base64
import hashlib
import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional


# ══════════════════════════════════════════════════════════
# QR CODE ENGINE — ZATCA Phase 1
# TLV (Tag-Length-Value) Base64 Encoding
# ══════════════════════════════════════════════════════════
def _tlv_encode(tag: int, value: str) -> bytes:
    """تشفير حقل واحد بنظام TLV"""
    val_bytes = value.encode('utf-8')
    return bytes([tag]) + bytes([len(val_bytes)]) + val_bytes


def generate_qr_code_phase1(
    seller_name: str,
    vat_number:  str,
    invoice_datetime: str,  # ISO 8601: 2026-04-12T14:30:00Z
    total_amount: str,
    vat_amount:   str,
) -> str:
    """
    توليد QR Code للمرحلة الأولى
    يحتوي 5 حقول TLV مشفرة بـ Base64
    """
    tlv = (
        _tlv_encode(1, seller_name)     +  # Tag 1: اسم البائع
        _tlv_encode(2, vat_number)      +  # Tag 2: الرقم الضريبي
        _tlv_encode(3, invoice_datetime)+  # Tag 3: وقت وتاريخ الفاتورة
        _tlv_encode(4, total_amount)    +  # Tag 4: إجمالي الفاتورة
        _tlv_encode(5, vat_amount)         # Tag 5: إجمالي الضريبة
    )
    return base64.b64encode(tlv).decode('utf-8')


def generate_qr_code_phase2(
    seller_name: str,
    vat_number:  str,
    invoice_datetime: str,
    total_amount: str,
    vat_amount:   str,
    invoice_hash: str,
    digital_signature: str = "",
    public_key: str = "",
) -> str:
    """
    توليد QR Code للمرحلة الثانية
    يحتوي 8 حقول TLV (إضافة Hash والتوقيع والمفتاح العام)
    """
    tlv = (
        _tlv_encode(1, seller_name)         +
        _tlv_encode(2, vat_number)          +
        _tlv_encode(3, invoice_datetime)    +
        _tlv_encode(4, total_amount)        +
        _tlv_encode(5, vat_amount)          +
        _tlv_encode(6, invoice_hash)        +  # Tag 6: Hash الفاتورة
        _tlv_encode(7, digital_signature)   +  # Tag 7: التوقيع الرقمي
        _tlv_encode(8, public_key)             # Tag 8: المفتاح العام
    )
    return base64.b64encode(tlv).decode('utf-8')


# ══════════════════════════════════════════════════════════
# HASH ENGINE
# ══════════════════════════════════════════════════════════
def calculate_invoice_hash(invoice_data: dict) -> str:
    """
    حساب Hash الفاتورة (SHA-256 Base64)
    يستخدم البيانات الأساسية للفاتورة
    """
    # ترتيب ثابت للحقول لضمان Hash ثابت
    canonical = json.dumps({
        "serial":       invoice_data.get("serial", ""),
        "uuid":         str(invoice_data.get("uuid_zatca", "")),
        "date":         str(invoice_data.get("invoice_date", "")),
        "customer_vat": invoice_data.get("customer_vat", ""),
        "total":        str(invoice_data.get("total_amount", "0")),
        "vat":          str(invoice_data.get("vat_amount", "0")),
    }, ensure_ascii=False, sort_keys=True)

    hash_bytes = hashlib.sha256(canonical.encode('utf-8')).digest()
    return base64.b64encode(hash_bytes).decode('utf-8')


FIRST_INVOICE_HASH = "NWZlY2Y5YTMxZTYyOTk2MDI2MDhmNmFjNjAxMWVlMzE="


def get_previous_hash(invoices_chain: list) -> str:
    """جلب Hash آخر فاتورة للسلسلة"""
    if not invoices_chain:
        return FIRST_INVOICE_HASH
    return invoices_chain[-1].get("invoice_hash", FIRST_INVOICE_HASH)


# ══════════════════════════════════════════════════════════
# XML UBL 2.1 GENERATOR — ZATCA Phase 1 & 2
# ══════════════════════════════════════════════════════════
def generate_invoice_xml(
    invoice:  dict,
    lines:    list,
    seller:   dict,
    previous_hash: str = FIRST_INVOICE_HASH,
    invoice_hash:  str = "",
) -> str:
    """
    توليد ملف XML متوافق مع ZATCA UBL 2.1
    """
    inv_uuid    = str(invoice.get("uuid_zatca", uuid.uuid4()))
    inv_date    = str(invoice.get("invoice_date", ""))
    inv_time    = str(invoice.get("invoice_time", "00:00:00"))
    inv_serial  = invoice.get("serial", "")
    inv_type    = invoice.get("invoice_type", "tax")
    currency    = invoice.get("currency_code", "SAR")

    # تحديد نوع الفاتورة حسب ZATCA
    type_code   = "388"  # Standard Invoice
    sub_type    = "0100000"  # Standard
    if inv_type == "simplified":
        type_code = "388"
        sub_type  = "0200000"  # Simplified
    elif inv_type == "credit_note":
        type_code = "381"
        sub_type  = "0100000"
    elif inv_type == "debit_note":
        type_code = "383"
        sub_type  = "0100000"

    subtotal    = f"{float(invoice.get('subtotal', 0)):.2f}"
    vat_amount  = f"{float(invoice.get('vat_amount', 0)):.2f}"
    total       = f"{float(invoice.get('total_amount', 0)):.2f}"
    discount    = f"{float(invoice.get('discount_amount', 0)):.2f}"

    # بناء أسطر الفاتورة
    lines_xml = ""
    for i, line in enumerate(lines, 1):
        qty       = float(line.get("quantity", 1))
        price     = float(line.get("unit_price", 0))
        net       = float(line.get("net_amount", 0))
        vat_r     = float(line.get("vat_rate", 15))
        vat_a     = float(line.get("vat_amount", 0))
        total_l   = float(line.get("total_amount", 0))
        cat       = line.get("vat_category", "S")
        item_name = line.get("item_name", "")

        lines_xml += f"""
    <cac:InvoiceLine>
        <cbc:ID>{i}</cbc:ID>
        <cbc:InvoicedQuantity unitCode="PCE">{qty:.3f}</cbc:InvoicedQuantity>
        <cbc:LineExtensionAmount currencyID="{currency}">{net:.2f}</cbc:LineExtensionAmount>
        <cac:TaxTotal>
            <cbc:TaxAmount currencyID="{currency}">{vat_a:.2f}</cbc:TaxAmount>
            <cbc:RoundingAmount currencyID="{currency}">{total_l:.2f}</cbc:RoundingAmount>
        </cac:TaxTotal>
        <cac:Item>
            <cbc:Name>{item_name}</cbc:Name>
            <cac:ClassifiedTaxCategory>
                <cbc:ID>{cat}</cbc:ID>
                <cbc:Percent>{vat_r:.2f}</cbc:Percent>
                <cac:TaxScheme><cbc:ID>VAT</cbc:ID></cac:TaxScheme>
            </cac:ClassifiedTaxCategory>
        </cac:Item>
        <cac:Price>
            <cbc:PriceAmount currencyID="{currency}">{price:.2f}</cbc:PriceAmount>
        </cac:Price>
    </cac:InvoiceLine>"""

    # QR Code
    qr = invoice.get("qr_code", "")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2"
         xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
         xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"
         xmlns:ext="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2">

    <cbc:ProfileID>reporting:1.0</cbc:ProfileID>
    <cbc:ID>{inv_serial}</cbc:ID>
    <cbc:UUID>{inv_uuid}</cbc:UUID>
    <cbc:IssueDate>{inv_date}</cbc:IssueDate>
    <cbc:IssueTime>{inv_time}</cbc:IssueTime>
    <cbc:InvoiceTypeCode name="{sub_type}">{type_code}</cbc:InvoiceTypeCode>
    <cbc:DocumentCurrencyCode>{currency}</cbc:DocumentCurrencyCode>
    <cbc:TaxCurrencyCode>SAR</cbc:TaxCurrencyCode>

    <!-- Additional Documents: Hash Chain -->
    <cac:AdditionalDocumentReference>
        <cbc:ID>ICV</cbc:ID>
        <cbc:UUID>{inv_serial}</cbc:UUID>
    </cac:AdditionalDocumentReference>
    <cac:AdditionalDocumentReference>
        <cbc:ID>PIH</cbc:ID>
        <cac:Attachment>
            <cbc:EmbeddedDocumentBinaryObject mimeCode="text/plain">{previous_hash}</cbc:EmbeddedDocumentBinaryObject>
        </cac:Attachment>
    </cac:AdditionalDocumentReference>
    <cac:AdditionalDocumentReference>
        <cbc:ID>QR</cbc:ID>
        <cac:Attachment>
            <cbc:EmbeddedDocumentBinaryObject mimeCode="text/plain">{qr}</cbc:EmbeddedDocumentBinaryObject>
        </cac:Attachment>
    </cac:AdditionalDocumentReference>

    <!-- Seller (AccountingSupplierParty) -->
    <cac:AccountingSupplierParty>
        <cac:Party>
            <cac:PartyIdentification>
                <cbc:ID schemeID="CRN">{seller.get("cr_number","")}</cbc:ID>
            </cac:PartyIdentification>
            <cac:PostalAddress>
                <cbc:StreetName>{seller.get("street","")}</cbc:StreetName>
                <cbc:BuildingNumber>{seller.get("building_number","")}</cbc:BuildingNumber>
                <cbc:CityName>{seller.get("city","")}</cbc:CityName>
                <cbc:PostalZone>{seller.get("postal_code","")}</cbc:PostalZone>
                <cbc:CountrySubentity>{seller.get("district","")}</cbc:CountrySubentity>
                <cac:Country><cbc:IdentificationCode>SA</cbc:IdentificationCode></cac:Country>
            </cac:PostalAddress>
            <cac:PartyTaxScheme>
                <cbc:CompanyID>{seller.get("vat_number","")}</cbc:CompanyID>
                <cac:TaxScheme><cbc:ID>VAT</cbc:ID></cac:TaxScheme>
            </cac:PartyTaxScheme>
            <cac:PartyLegalEntity>
                <cbc:RegistrationName>{seller.get("seller_name","")}</cbc:RegistrationName>
            </cac:PartyLegalEntity>
        </cac:Party>
    </cac:AccountingSupplierParty>

    <!-- Buyer (AccountingCustomerParty) -->
    <cac:AccountingCustomerParty>
        <cac:Party>
            <cac:PostalAddress>
                <cac:Country><cbc:IdentificationCode>SA</cbc:IdentificationCode></cac:Country>
            </cac:PostalAddress>
            <cac:PartyTaxScheme>
                <cbc:CompanyID>{invoice.get("customer_vat","")}</cbc:CompanyID>
                <cac:TaxScheme><cbc:ID>VAT</cbc:ID></cac:TaxScheme>
            </cac:PartyTaxScheme>
            <cac:PartyLegalEntity>
                <cbc:RegistrationName>{invoice.get("customer_name","")}</cbc:RegistrationName>
            </cac:PartyLegalEntity>
        </cac:Party>
    </cac:AccountingCustomerParty>

    <!-- Tax Totals -->
    <cac:TaxTotal>
        <cbc:TaxAmount currencyID="{currency}">{vat_amount}</cbc:TaxAmount>
        <cac:TaxSubtotal>
            <cbc:TaxableAmount currencyID="{currency}">{subtotal}</cbc:TaxableAmount>
            <cbc:TaxAmount currencyID="{currency}">{vat_amount}</cbc:TaxAmount>
            <cac:TaxCategory>
                <cbc:ID>S</cbc:ID>
                <cbc:Percent>15.00</cbc:Percent>
                <cac:TaxScheme><cbc:ID>VAT</cbc:ID></cac:TaxScheme>
            </cac:TaxCategory>
        </cac:TaxSubtotal>
    </cac:TaxTotal>

    <!-- Legal Monetary Total -->
    <cac:LegalMonetaryTotal>
        <cbc:LineExtensionAmount currencyID="{currency}">{subtotal}</cbc:LineExtensionAmount>
        <cbc:TaxExclusiveAmount currencyID="{currency}">{subtotal}</cbc:TaxExclusiveAmount>
        <cbc:TaxInclusiveAmount currencyID="{currency}">{total}</cbc:TaxInclusiveAmount>
        <cbc:AllowanceTotalAmount currencyID="{currency}">{discount}</cbc:AllowanceTotalAmount>
        <cbc:PayableAmount currencyID="{currency}">{total}</cbc:PayableAmount>
    </cac:LegalMonetaryTotal>

    <!-- Invoice Lines -->
    {lines_xml}

</Invoice>"""

    return xml


# ══════════════════════════════════════════════════════════
# VALIDATION ENGINE
# ══════════════════════════════════════════════════════════
def validate_invoice_zatca(invoice: dict, lines: list, seller: dict) -> dict:
    """
    التحقق من متطلبات ZATCA قبل الإصدار
    يعيد قائمة الأخطاء والتحذيرات
    """
    errors   = []
    warnings = []

    # التحقق من بيانات البائع
    if not seller.get("vat_number"):
        errors.append("الرقم الضريبي للمنشأة مطلوب")
    elif len(str(seller.get("vat_number",""))) != 15:
        errors.append("الرقم الضريبي يجب أن يكون 15 خانة")
    if not seller.get("seller_name"):
        errors.append("اسم المنشأة مطلوب")
    if not seller.get("city"):
        warnings.append("مدينة المنشأة غير محددة")
    if not seller.get("street"):
        warnings.append("عنوان المنشأة غير محدد")

    # التحقق من بيانات الفاتورة
    if not invoice.get("invoice_date"):
        errors.append("تاريخ الفاتورة مطلوب")
    if not invoice.get("customer_name"):
        errors.append("اسم العميل مطلوب")

    # B2B تتطلب رقم ضريبي للعميل
    inv_type = invoice.get("invoice_type","tax")
    if inv_type == "tax":
        if not invoice.get("customer_vat"):
            warnings.append("فاتورة ضريبية B2B: الرقم الضريبي للعميل موصى به")

    # التحقق من الأسطر
    if not lines:
        errors.append("يجب أن تحتوي الفاتورة على سطر واحد على الأقل")
    for i, line in enumerate(lines, 1):
        if not line.get("item_name"):
            errors.append(f"السطر {i}: اسم الصنف مطلوب")
        if float(line.get("unit_price", 0)) < 0:
            errors.append(f"السطر {i}: السعر لا يمكن أن يكون سالباً")
        if float(line.get("quantity", 0)) <= 0:
            errors.append(f"السطر {i}: الكمية يجب أن تكون أكبر من صفر")

    # التحقق من الإجماليات
    total   = float(invoice.get("total_amount", 0))
    vat     = float(invoice.get("vat_amount", 0))
    sub     = float(invoice.get("subtotal", 0))
    if total <= 0:
        errors.append("إجمالي الفاتورة يجب أن يكون أكبر من صفر")
    if abs((sub * 0.15) - vat) > 0.05:
        warnings.append(f"تحقق من ضريبة القيمة المضافة: المتوقع {sub*0.15:.2f} والمحسوب {vat:.2f}")

    return {
        "valid":    len(errors) == 0,
        "errors":   errors,
        "warnings": warnings,
    }
