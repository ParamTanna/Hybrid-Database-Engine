from fastapi import FastAPI
from faker import Faker
from sse_starlette.sse import EventSourceResponse
import random
import asyncio
import json

random.seed(42)
app = FastAPI()
faker = Faker()

# ─── Unique pools ────────────────────────────────────────────────────────────
CUSTOMER_ID_POOL = random.sample(range(10_000, 99_999), 5000)
EMAIL_POOL       = [faker.unique.email() for _ in range(5000)]
CUSTOMER_POOL    = [
    {"customer_id": cid, "email": email}
    for cid, email in zip(CUSTOMER_ID_POOL, EMAIL_POOL)
]

# ─── Static value sets ───────────────────────────────────────────────────────
THEMES     = ["light", "dark", "system", "high-contrast", "solarized"]
LANGUAGES  = ["en", "fr", "de", "es", "pt", "zh", "ja", "ar"]
COUNTRIES  = ["US", "UK", "CA", "AU", "IN", "DE", "FR", "JP", "BR", "SG"]
STATUSES   = ["pending", "shipped", "delivered", "cancelled", "returned"]
ISSUES     = [
    "Login problem", "Payment failed", "Order not received",
    "Wrong item sent", "Refund request", "Account locked",
    "Slow loading", "App crash", "Discount not applied",
]
TAGS_POOL  = ["vip", "loyalty", "beta", "early-adopter", "premium",
              "trial", "student", "corporate", "influencer", "new-user"]
PROMO_POOL = ["SAVE10", "WELCOME20", "FLASH50", "REFER15", "SUMMER30",
              "WINTER40", "FIRST25", "LOYAL5", "VIP35", "DEAL60"]

_ORDER_COUNTER   = [200_000]
_TICKET_COUNTER  = [500_000]
_ADDRESS_COUNTER = [700_000]

def _next(counter):
    counter[0] += 1
    return counter[0]


# ─── Sub-document generators ─────────────────────────────────────────────────

def gen_profile():
    return {
        "bio":     faker.sentence(nb_words=10),
        "website": faker.url(),
    }


def gen_preferences():
    return {
        "theme":         random.choice(THEMES),
        "language":      random.choice(LANGUAGES),
        "notifications": random.choice([True, False]),
    }


def gen_orders():
    """1-4 orders per customer.  avg ~2.5 → but independent_query forces SQL."""
    return [
        {
            "order_id": _next(_ORDER_COUNTER),
            "amount":   round(random.uniform(5.0, 999.99), 2),
            "status":   random.choice(STATUSES),
        }
        for _ in range(random.randint(1, 4))
    ]


def gen_addresses():
    """1-2 addresses per customer.  appendable=false → SQL child (one_to_many)."""
    return [
        {
            "address_id": _next(_ADDRESS_COUNTER),
            "street":     faker.street_address(),
            "city":       faker.city(),
            "country":    random.choice(COUNTRIES),
        }
        for _ in range(random.randint(1, 2))
    ]


def gen_tags():
    """1-3 tags per customer.  avg_size ~2 ≤ 5 → Mongo.embed."""
    return [
        {"tag": t}
        for t in random.sample(TAGS_POOL, random.randint(1, 3))
    ]


def gen_reviews():
    """6-9 reviews per customer.  avg_size ~7.5 > 5 → Mongo.reference."""
    return [
        {
            "product_id": random.randint(1, 1000),
            "rating":     random.randint(1, 5),
            "comment":    faker.sentence(nb_words=12),
        }
        for _ in range(random.randint(6, 9))
    ]


def gen_support_tickets():
    """6-8 tickets per customer.  avg_size ~7 > 5 → Mongo.reference."""
    return [
        {
            "ticket_id": _next(_TICKET_COUNTER),
            "issue":     random.choice(ISSUES),
            "resolved":  random.choice([True, False]),
        }
        for _ in range(random.randint(6, 8))
    ]


# ─── Appearance weights ──────────────────────────────────────────────────────
#
#  Target classifications:
#    freq > 50%         -> SQL main table
#    10% <= freq <= 50% -> Mongo.document
#    freq < 10%         -> BUFFER
#
#  Primitives:
#    name, age, signup_date  ~ 85-95%  -> SQL.customers
#    phone, nickname         ~ 25-40%  -> Mongo.document  (10-50%)
#    promo_code              ~  5%     -> Buffer           (<10%)
#    beta_flag               ~  7%     -> Buffer           (<10%)
#
#  Arrays / objects appear with their own probability; their classification
#  is driven by schema flags + avg_size vs AVG_SIZE_THRESHOLD (5).
#    tags             avg ~2   ≤ 5  -> Mongo.embed
#    reviews          avg ~7.5 > 5  -> Mongo.reference
#    support_tickets  avg ~7   > 5  -> Mongo.reference

FIELD_WEIGHTS = {
    # high frequency -> SQL
    "name":        0.92,
    "age":         0.88,
    "signup_date": 0.95,
    # medium frequency -> Mongo.document
    "phone":       0.32,
    "nickname":    0.24,
    # very low frequency -> Buffer (< 10%)
    "promo_code":  0.05,
    "beta_flag":   0.07,
    # nested / array (classification by schema flags, not frequency)
    "orders":           0.75,
    "addresses":        0.65,
    "profile":          0.70,
    "preferences":      0.68,
    "tags":             0.60,
    "reviews":          0.72,
    "support_tickets":  0.55,
}

NESTED_GEN = {
    "orders":           gen_orders,
    "addresses":        gen_addresses,
    "profile":          gen_profile,
    "preferences":      gen_preferences,
    "tags":             gen_tags,
    "reviews":          gen_reviews,
    "support_tickets":  gen_support_tickets,
}


def generate_record():
    customer = random.choice(CUSTOMER_POOL)
    record = {
        "customer_id": customer["customer_id"],
        "email":       customer["email"],
    }

    for field, weight in FIELD_WEIGHTS.items():
        if random.random() > weight:
            continue
        if field in NESTED_GEN:
            record[field] = NESTED_GEN[field]()
        elif field == "name":
            record[field] = faker.name()
        elif field == "age":
            record[field] = random.randint(18, 80)
        elif field == "signup_date":
            record[field] = faker.date_between(start_date="-5y").isoformat()
        elif field == "phone":
            record[field] = faker.phone_number()
        elif field == "nickname":
            record[field] = faker.user_name()
        elif field == "promo_code":
            record[field] = random.choice(PROMO_POOL)
        elif field == "beta_flag":
            record[field] = random.choice([True, False])

    return record


# ─── API endpoints ────────────────────────────────────────────────────────────

@app.get("/")
async def single_record():
    return generate_record()


@app.get("/record/{count}")
async def stream_records(count: int):
    async def event_generator():
        for _ in range(count):
            await asyncio.sleep(0.01)
            yield {"event": "record", "data": json.dumps(generate_record())}
    return EventSourceResponse(event_generator())
