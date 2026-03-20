"""
Local simulation API: streams synthetic **customer** records as Server-Sent Events.

Run:  python -m hybrid_framework.simulation_api
       (from DBMS_ASG_2-main, with PYTHONPATH=. or pip install -e .)

Matches hybrid_framework/schema.json field names and value shapes.
"""
from __future__ import annotations

import json
import random
import asyncio
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI
from starlette.responses import StreamingResponse
from faker import Faker

random.seed(42)
fake = Faker()
Faker.seed(42)

app = FastAPI(title="Customer simulation API", version="2.0")

# --- Stable customer pool (login -> id, name, email) ---
CUSTOMER_LOGINS = [fake.user_name() + str(i % 97) for i in range(500)]
CUSTOMER_METADATA: dict[str, dict[str, Any]] = {}
for i, login in enumerate(CUSTOMER_LOGINS):
    CUSTOMER_METADATA[login] = {
        "customer_id": 500_000 + i,
        "full_name": fake.name(),
        "email": fake.email(),
    }

PRODUCT_SKUS = [f"SKU-{fake.lexify('????').upper()}-{1000 + j}" for j in range(80)]
CHANNELS = ["email", "chat", "phone", "portal"]
PRIORITIES = ["low", "normal", "high", "urgent"]
THEMES = ["light", "dark", "system"]
CATEGORIES = ["electronics", "apparel", "home", "sports", "books", "grocery", "beauty"]

# Presence weights (0–1); some fields intentionally sparse for classification variety
FIELD_WEIGHTS: dict[str, float] = {
    "account_balance": 0.88,
    "loyalty_points": 0.82,
    "is_premium": 0.75,
    "phone": 0.70,
    "signup_year": 0.80,
    "billing_address": 0.72,
    "shipping_address": 0.55,
    "order_totals_history": 0.50,
    "active_orders": 0.62,
    "support_tickets": 0.48,
    "preferences": 0.58,
    "tags": 0.45,
}


def _maybe_sparse(include: bool, miss_rate: float = 0.18) -> bool:
    return include and random.random() > miss_rate


def _billing_address() -> dict[str, str]:
    return {
        "street": fake.street_address(),
        "city": fake.city(),
        "region": fake.state() if random.random() > 0.2 else fake.country_code(),
        "postal_code": fake.postcode(),
        "country": fake.country(),
    }


def _shipping_address() -> dict[str, str]:
    line2 = fake.secondary_address() if random.random() > 0.55 else ""
    return {
        "line1": fake.street_address(),
        "line2": line2,
        "city": fake.city(),
        "postal_code": fake.postcode(),
    }


def _active_orders() -> list[dict[str, Any]]:
    n = random.randint(1, 5)
    if random.random() < 0.12:
        n = random.randint(10, 18)
    rows = []
    for _ in range(n):
        rows.append(
            {
                "order_id": f"ORD-{random.randint(100_000, 999_999)}",
                "sku": random.choice(PRODUCT_SKUS),
                "quantity": random.randint(1, 12),
                "unit_price": round(random.uniform(4.99, 899.99), 2),
            }
        )
    return rows


def _support_tickets() -> list[dict[str, Any]]:
    n = random.randint(1, 6)
    rows = []
    for _ in range(n):
        rows.append(
            {
                "ticket_id": f"TCK-{random.randint(10_000, 99_999)}",
                "channel": random.choice(CHANNELS),
                "opened_at": datetime.now(timezone.utc).isoformat(),
                "priority": random.choice(PRIORITIES),
            }
        )
    return rows


def _preferences() -> dict[str, Any]:
    return {
        "theme": random.choice(THEMES),
        "notifications_enabled": random.choice([True, False]),
        "favorite_categories": random.sample(CATEGORIES, k=random.randint(1, min(4, len(CATEGORIES)))),
    }


def generate_record() -> dict[str, Any]:
    login = random.choice(CUSTOMER_LOGINS)
    meta = CUSTOMER_METADATA[login]

    record: dict[str, Any] = {
        "customer_login": login,
        "customer_id": meta["customer_id"],
        "full_name": meta["full_name"],
        "email": meta["email"],
    }

    if _maybe_sparse(random.random() < FIELD_WEIGHTS["account_balance"]):
        record["account_balance"] = round(random.uniform(-120.0, 15_000.0), 2)

    if _maybe_sparse(random.random() < FIELD_WEIGHTS["loyalty_points"]):
        record["loyalty_points"] = random.randint(0, 250_000)

    if _maybe_sparse(random.random() < FIELD_WEIGHTS["is_premium"]):
        record["is_premium"] = random.choice([True, False])

    if _maybe_sparse(random.random() < FIELD_WEIGHTS["phone"]):
        record["phone"] = fake.phone_number() if random.random() > 0.25 else ""

    if _maybe_sparse(random.random() < FIELD_WEIGHTS["signup_year"]):
        record["signup_year"] = random.randint(2018, datetime.now().year)

    if random.random() < FIELD_WEIGHTS["billing_address"]:
        record["billing_address"] = _billing_address()

    if random.random() < FIELD_WEIGHTS["shipping_address"]:
        record["shipping_address"] = _shipping_address()

    if random.random() < FIELD_WEIGHTS["order_totals_history"]:
        record["order_totals_history"] = [
            round(random.uniform(12.0, 2_500.0), 2) for _ in range(random.randint(2, 8))
        ]

    if random.random() < FIELD_WEIGHTS["active_orders"]:
        record["active_orders"] = _active_orders()

    if random.random() < FIELD_WEIGHTS["support_tickets"]:
        record["support_tickets"] = _support_tickets()

    if random.random() < FIELD_WEIGHTS["preferences"]:
        record["preferences"] = _preferences()

    if random.random() < FIELD_WEIGHTS["tags"]:
        record["tags"] = [fake.word() for _ in range(random.randint(1, 5))]

    return record


@app.get("/")
async def root() -> dict[str, Any]:
    return generate_record()


@app.get("/record/{count}")
async def stream_records(count: int) -> StreamingResponse:
    async def event_generator() -> Any:
        for _ in range(max(1, min(count, 50_000))):
            yield f"data: {json.dumps(generate_record(), default=str)}\n\n"
            await asyncio.sleep(0.005)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
