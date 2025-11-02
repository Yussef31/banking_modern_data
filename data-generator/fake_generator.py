import os
import time
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import random
import argparse
import sys
from sqlalchemy import create_engine

import os
from sqlalchemy import create_engine

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "banking")

# Hard fail on unexpected characters
for name, val in [("USER", DB_USER), ("PASS", DB_PASS), ("HOST", DB_HOST), ("DB", DB_NAME)]:
    try:
        val.encode("utf-8")
    except Exception:
        raise RuntimeError(f"Environment variable contains invalid UTF-8 in {name}")

url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"Connecting to: postgresql+psycopg2://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")

engine = create_engine(url, pool_pre_ping=True)
with engine.connect() as c:
    one = c.execute("SELECT 1").scalar()
    print("DB OK:", one)

# -----------------------------
# Config
# -----------------------------
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TXN_AMOUNT = 1000.00
CURRENCY = "USD"

INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")

DEFAULT_LOOP = True
SLEEP_SECONDS = 2

parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

# -----------------------------
# Database connection (host or container)
# From Windows host: use localhost:5432 (mapped by docker-compose)
# From inside a container on same network: use postgres:5432
# -----------------------------
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")   # IMPORTANT: localhost on host
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "banking")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
conn = engine.raw_connection()
conn.autocommit = False
cur = conn.cursor()

def run_iteration():
    try:
        customers = []
        for _ in range(NUM_CUSTOMERS):
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.unique.email()
            cur.execute(
                "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
                (first_name, last_name, email),
            )
            customer_id = cur.fetchone()[0]
            customers.append(customer_id)

        accounts = []
        for customer_id in customers:
            for _ in range(ACCOUNTS_PER_CUSTOMER):
                account_type = random.choice(["SAVINGS", "CHECKING"])
                initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
                cur.execute(
                    "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
                    (customer_id, account_type, initial_balance, CURRENCY),
                )
                account_id = cur.fetchone()[0]
                accounts.append(account_id)

        txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
        for _ in range(NUM_TRANSACTIONS):
            account_id = random.choice(accounts)
            txn_type = random.choice(txn_types)
            amount = round(random.uniform(1, MAX_TXN_AMOUNT), 2)
            related_account = None
            if txn_type == "TRANSFER" and len(accounts) > 1:
                related_account = random.choice([a for a in accounts if a != account_id])
            cur.execute(
                "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, 'COMPLETED')",
                (account_id, txn_type, amount, related_account),
            )

        conn.commit()
        print(f"✅ Generated {len(customers)} customers, {len(accounts)} accounts, {NUM_TRANSACTIONS} transactions.")
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()

try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---")
        run_iteration()
        print(f"--- Iteration {iteration} finished ---")
        if not LOOP:
            break
        time.sleep(SLEEP_SECONDS)
except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")
finally:
    cur.close()
    conn.close()
    sys.exit(0)
