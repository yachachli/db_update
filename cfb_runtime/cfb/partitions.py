"""Select schedule partitions by their latest completed kickoff, including postseason."""
def recent(rows, count=None):
    if count is None:
        return rows
    if not 1 <= count <= 6:
        raise ValueError('Recent partition count must be 1..6')
    latest = {}
    for row in rows:
        key = (row[1], row[2])
        latest[key] = max(latest.get(key, row[-1]), row[-1])
    selected = set(sorted(latest, key=latest.get, reverse=True)[:count])
    return [row for row in rows if (row[1], row[2]) in selected]
