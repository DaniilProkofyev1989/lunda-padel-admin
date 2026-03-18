"""Daily snapshot of tournament player counts.

Run once per day (after the scraper) to capture current state
of all active tournaments into event_snapshots table.

Usage:
    python snapshot.py
"""

from database import Database

if __name__ == "__main__":
    db = Database()
    count = db.save_daily_snapshots()
    print(f"Saved {count} snapshots")
