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
app = FastAPI()

# --- Pools ---
USERNAMES = [fake.user_name() for _ in range(500)]
COURSES = [
    {"course_code": f"CS{100+i}", "course_name": f"Computer Science {i}", "credits": random.randint(2, 4)}
    for i in range(30)
]
DEPARTMENTS = ["Computer Science", "Mathematics", "Physics", "Electrical Engineering", "Mechanical Engineering", "Civil Engineering", "Chemistry", "Biology"]

# Mapping username -> student_id, name, email
USER_METADATA = {}
for i, uname in enumerate(USERNAMES):
    USER_METADATA[uname] = {
        "student_id": 1000 + i,  # unique, deterministic
        "name": fake.name(),
        "email": fake.email()
    }

# Field appearance weights
# ... (rest of the weights remain the same)
FIELD_WEIGHTS = {
    "age": random.uniform(0.05, 0.95),
    "gpa": random.uniform(0.05, 0.95),
    "department": random.uniform(0.05, 0.95),
    "year_of_study": random.uniform(0.05, 0.95),
    "is_active": random.uniform(0.05, 0.95),
    "phone": random.uniform(0.05, 0.95),
    "address": 0.70,
    "cgpa_history": 0.50,
    "enrolled_courses": 0.65,
    "submissions": 0.50,
    "research_interests": 0.40
}

def generate_record() -> dict[str, Any]:
    uname = random.choice(USERNAMES)
    meta = USER_METADATA[uname]
    
    # Base fields
    record = {"username": uname}
    
    # Always emit student_id, never studentId
    record["student_id"] = meta["student_id"]
    record["name"] = meta["name"]
    record["email"] = meta["email"]
    
    # Weighted fields
    if random.random() < FIELD_WEIGHTS["age"] and random.random() > 0.25: # sparse/missing
        record["age"] = random.randint(18, 30)
        
    if random.random() < FIELD_WEIGHTS["gpa"]:
        # GPA always float, no type drift
        record["gpa"] = round(random.uniform(4.0, 10.0), 1)
            
    if random.random() < FIELD_WEIGHTS["department"]:
        record["department"] = random.choice(DEPARTMENTS)
        
    if random.random() < FIELD_WEIGHTS["year_of_study"]:
        record["year_of_study"] = random.randint(1, 5)
        
    if random.random() < FIELD_WEIGHTS["is_active"]:
        record["is_active"] = random.choice([True, False])
        
    if random.random() < FIELD_WEIGHTS["phone"]:
        record["phone"] = fake.phone_number() if random.random() > 0.3 else None
        
    if random.random() < FIELD_WEIGHTS["address"]:
        record["address"] = {
            "city": fake.city(),
            "state": fake.state(),
            "pincode": fake.postcode()
        }
        
    if random.random() < FIELD_WEIGHTS["cgpa_history"]:
        record["cgpa_history"] = [round(random.uniform(6.0, 10.0), 2) for _ in range(random.randint(3, 6))]
        
    if random.random() < FIELD_WEIGHTS["enrolled_courses"]:
        record["enrolled_courses"] = [
            {
                "course_code": c["course_code"],
                "course_name": c["course_name"],
                "credits": c["credits"],
                "semester": f"Sem {random.randint(1, 8)}"
            }
            for c in random.sample(COURSES, random.randint(1, 5))
        ]
        
    if random.random() < FIELD_WEIGHTS["submissions"]:
        sub_len = random.randint(1, 8)
        if random.random() < 0.15: sub_len = random.randint(10, 20) # large array
        record["submissions"] = [
            {
                "assignment_id": f"ASG-{random.randint(100, 999)}",
                "course_code": random.choice(COURSES)["course_code"],
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "score": round(random.uniform(50, 100), 1),
                "feedback": fake.sentence()
            }
            for _ in range(sub_len)
        ]
        
    if random.random() < FIELD_WEIGHTS["research_interests"]:
        record["research_interests"] = [fake.word() for _ in range(random.randint(1, 4))]
        
    return record

@app.get("/")
async def root():
    return generate_record()

@app.get("/record/{count}")
async def stream_records(count: int):
    async def event_generator():
        for _ in range(count):
            yield f"data: {json.dumps(generate_record())}\n\n"
            await asyncio.sleep(0.005)
    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
