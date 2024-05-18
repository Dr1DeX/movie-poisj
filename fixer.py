import sqlite3
import json

from collections import Counter

conn = sqlite3.connect("db.sqlite")

counter = Counter()

for row in conn.execute("select writer, writers from movies"):
    writer = row[0]
    if writer:
        counter[writer] += 1
    else:
        writers = json.loads(row[1])
        for w in writers:
            counter[w['id']] += 1

writers_ids = [writer_id for writer_id, _ in counter.most_common()[:2]]
in_placeholders = ','.join(['?'] * len(writers_ids))

row = conn.execute(
    f"select name from writers where id in ({in_placeholders}) and name != 'N/A' limit 1",
    writers_ids,
).fetchone()

print('Most productive writer:', row[0])

conn.close()