"""
Kalshi rules watcher
--------------------
Pulls all open markets, snapshots rules_primary / rules_secondary into SQLite,
and alerts when a market's resolution rules change.

  python kalshi_rules_watch.py              # run once
  python kalshi_rules_watch.py --loop 900   # check every 15 minutes
  python kalshi_rules_watch.py --threshold 0.9

First run builds a baseline (no alerts). Every run after that compares.

A change is flagged MAJOR when either:
  * the text similarity drops below --threshold (default 0.85), or
  * any number, date, or strike in the rules changed (e.g. "above 50" -> "above 55"),
    which is drastic even when 99% of the wording is the same.
Everything else is MINOR (rewording, typo fixes).

Alert channels (all optional, set as environment variables):
  NTFY_TOPIC         push to your phone via https://ntfy.sh (free, no account)
  DISCORD_WEBHOOK    Discord channel webhook URL
  SLACK_WEBHOOK      Slack incoming webhook URL
Console output and dashboard.html are always produced.
"""

import argparse
import difflib
import hashlib
import html
import os
import re
import sqlite3
import time
from datetime import datetime, timezone

import pandas as pd
import requests

BASE = "https://api.elections.kalshi.com/trade-api/v2"
DB_PATH = "kalshi_rules.db"
DASHBOARD_PATH = "dashboard.html"
FIELDS = ["rules_primary", "rules_secondary"]


# ---------------------------------------------------------------- fetching
def fetch_events(statuses=("open",)):
    """Your pagination loop, plus retries for rate limits / blips."""
    all_events = []
    for status in statuses:
        cursor = None
        while True:
            params = {"status": status, "limit": 200, "with_nested_markets": "true"}
            if cursor:
                params["cursor"] = cursor
            for attempt in range(5):
                r = requests.get(f"{BASE}/events", params=params, timeout=30)
                if r.status_code == 429 or r.status_code >= 500:
                    time.sleep(2 ** attempt)
                    continue
                r.raise_for_status()
                break
            else:
                raise RuntimeError(f"Kalshi API kept failing: {r.status_code}")
            data = r.json()
            all_events.extend(data.get("events", []))
            cursor = data.get("cursor")
            if not cursor:
                break
    return all_events


def to_dataframe(all_events):
    """Your flattening step."""
    records = []
    for event in all_events:
        event_fields = {f"event_{k}": v for k, v in event.items() if k != "markets"}
        for market in event.get("markets", []):
            records.append({**event_fields, **market})
    return pd.DataFrame(records)


# ---------------------------------------------------------------- comparison
def normalize(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def text_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()


NUM_RE = re.compile(r"\$?\d[\d,]*\.?\d*%?")


def numbers_in(text):
    return [n.rstrip(".,") for n in NUM_RE.findall(text)]


def classify(old, new, threshold):
    similarity = difflib.SequenceMatcher(None, old, new).ratio()
    nums_changed = numbers_in(old) != numbers_in(new)
    reasons = []
    if similarity < threshold:
        reasons.append(f"text {similarity:.0%} similar")
    if nums_changed:
        reasons.append("numbers/dates changed")
    severity = "MAJOR" if reasons else "MINOR"
    return severity, similarity, "; ".join(reasons) or "minor rewording"


def word_diff_html(old, new):
    a, b = old.split(" "), new.split(" ")
    out = []
    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(None, a, b).get_opcodes():
        if op == "equal":
            out.append(html.escape(" ".join(a[i1:i2])))
        if op in ("delete", "replace"):
            out.append(f"<del>{html.escape(' '.join(a[i1:i2]))}</del>")
        if op in ("insert", "replace"):
            out.append(f"<ins>{html.escape(' '.join(b[j1:j2]))}</ins>")
    return " ".join(out)


def before_after_html(old, new):
    """Full old text and full new text, with removed words marked in the old
    version and added words marked in the new version."""
    a, b = old.split(" "), new.split(" ")
    before, after = [], []
    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(None, a, b).get_opcodes():
        old_part, new_part = html.escape(" ".join(a[i1:i2])), html.escape(" ".join(b[j1:j2]))
        if op == "equal":
            before.append(old_part)
            after.append(new_part)
        else:
            if old_part:
                before.append(f"<del>{old_part}</del>")
            if new_part:
                after.append(f"<ins>{new_part}</ins>")
    return " ".join(before) or "<em>(empty)</em>", " ".join(after) or "<em>(empty)</em>"


# ---------------------------------------------------------------- storage
def init_db(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS rules_current (
        ticker TEXT PRIMARY KEY, event_ticker TEXT, title TEXT, category TEXT,
        rules_primary TEXT, rules_secondary TEXT,
        hash_primary TEXT, hash_secondary TEXT,
        first_seen TEXT, last_seen TEXT
    );
    CREATE TABLE IF NOT EXISTS rules_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        detected_at TEXT, ticker TEXT, event_ticker TEXT, title TEXT, field TEXT,
        old_text TEXT, new_text TEXT, similarity REAL, severity TEXT, reason TEXT
    );
    """)


def check_for_changes(conn, df, threshold):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    first_run = conn.execute("SELECT COUNT(*) FROM rules_current").fetchone()[0] == 0
    existing = {row[0]: row for row in conn.execute(
        "SELECT ticker, rules_primary, rules_secondary, hash_primary, hash_secondary FROM rules_current")}

    changes = []
    for _, m in df.iterrows():
        ticker = m["ticker"]
        title = m.get("title") or m.get("event_title") or ""
        event_ticker = m.get("event_ticker", "")
        texts = {f: normalize(m.get(f)) for f in FIELDS}
        hashes = {f: text_hash(texts[f]) for f in FIELDS}

        prev = existing.get(ticker)
        if prev:
            prev_text = {"rules_primary": prev[1], "rules_secondary": prev[2]}
            prev_hash = {"rules_primary": prev[3], "rules_secondary": prev[4]}
            for f in FIELDS:
                if hashes[f] != prev_hash[f]:
                    sev, sim, reason = classify(prev_text[f], texts[f], threshold)
                    change = dict(detected_at=now, ticker=ticker, event_ticker=event_ticker,
                                  title=title, field=f, old_text=prev_text[f],
                                  new_text=texts[f], similarity=sim, severity=sev, reason=reason)
                    changes.append(change)
                    conn.execute(
                        "INSERT INTO rules_changes (detected_at, ticker, event_ticker, title, field, "
                        "old_text, new_text, similarity, severity, reason) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        tuple(change.values()))

        conn.execute("""
            INSERT INTO rules_current (ticker, event_ticker, title, category, rules_primary,
                rules_secondary, hash_primary, hash_secondary, first_seen, last_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(ticker) DO UPDATE SET
                title=excluded.title, rules_primary=excluded.rules_primary,
                rules_secondary=excluded.rules_secondary, hash_primary=excluded.hash_primary,
                hash_secondary=excluded.hash_secondary, last_seen=excluded.last_seen
        """, (ticker, event_ticker, title, m.get("event_category", ""), texts["rules_primary"],
              texts["rules_secondary"], hashes["rules_primary"], hashes["rules_secondary"], now, now))

    conn.commit()
    return first_run, changes


# ---------------------------------------------------------------- alerts
def send_alerts(changes, major_only=True):
    to_send = [c for c in changes if c["severity"] == "MAJOR" or not major_only]
    if not to_send:
        return
    lines = [f"[{c['severity']}] {c['ticker']} ({c['field']}): {c['reason']}" for c in to_send[:20]]
    if len(to_send) > 20:
        lines.append(f"...and {len(to_send) - 20} more (see dashboard.html)")
    message = f"Kalshi rules changed on {len(to_send)} market field(s):\n" + "\n".join(lines)

    try:
        if os.getenv("NTFY_TOPIC"):
            requests.post(f"https://ntfy.sh/{os.environ['NTFY_TOPIC']}", data=message.encode(),
                          headers={"Title": "Kalshi rules changed", "Priority": "high"}, timeout=10)
        if os.getenv("DISCORD_WEBHOOK"):
            requests.post(os.environ["DISCORD_WEBHOOK"], json={"content": message[:1900]}, timeout=10)
        if os.getenv("SLACK_WEBHOOK"):
            requests.post(os.environ["SLACK_WEBHOOK"], json={"text": message}, timeout=10)
    except requests.RequestException as e:
        print(f"  ! alert delivery failed: {e}")


# ---------------------------------------------------------------- dashboard
def write_dashboard(conn, n_markets, path=DASHBOARD_PATH, limit=300):
    rows = conn.execute("""
        SELECT detected_at, severity, title, field, similarity, reason, old_text, new_text,
               event_ticker, ticker
        FROM rules_changes ORDER BY id DESC LIMIT ?""", (limit,)).fetchall()
    n_major = sum(1 for r in rows if r[1] == "MAJOR")

    groups = {}
    for row in rows:
        ts, sev, title, field, sim, reason, old, new, event_ticker, ticker = row
        event_key = event_ticker or ticker
        key = (event_key, field, old, new, sev, reason, ts)
        if key not in groups:
            groups[key] = {
                "ts": ts, "severity": sev, "field": field, "similarity": sim,
                "reason": reason, "old": old, "new": new, "markets": [],
                "market_tickers": set(),
            }
        if ticker not in groups[key]["market_tickers"]:
            groups[key]["markets"].append({"title": title, "ticker": ticker})
            groups[key]["market_tickers"].add(ticker)

    cards = []
    for group in groups.values():
        before, after = before_after_html(group["old"], group["new"])
        markets = group["markets"]
        first_market = markets[0]
        market_link = (
            f'<a class="market-link" href="https://kalshi.com/markets/" target="_blank">'
            f'{html.escape(first_market["ticker"] or "")}</a>'
        )
        if len(markets) > 1:
            options = "".join(
                f'<option value="{i}" data-title="{html.escape(market["title"] or "", quote=True)}" '
                f'data-ticker="{html.escape(market["ticker"] or "", quote=True)}">'
                f'{html.escape(market["title"] or market["ticker"] or "Untitled market")} '
                f'({html.escape(market["ticker"] or "")})</option>'
                for i, market in enumerate(markets)
            )
            market_picker = (
                '<div class="market-picker"><label>Markets in this event '
                f'<select class="market-select" aria-label="Select a market">{options}</select></label></div>'
            )
        else:
            market_picker = ""
        cards.append(f"""
        <div class="card {group["severity"].lower()}" data-sev="{group["severity"]}">
          <div class="head"><span class="badge">{group["severity"]}</span>
            {market_link}
            <span class="muted">{html.escape(group["field"])} · {group["similarity"]:.0%} similar · {html.escape(group["reason"])} · {group["ts"]}</span></div>
          {market_picker}
          <div class="title">{html.escape(first_market["title"] or "")}</div>
          <div class="compare">
            <div class="side before"><div class="label">Previous rule</div>{before}</div>
            <div class="side after"><div class="label">New rule</div>{after}</div>
          </div>
        </div>""")

    page = f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="300"><title>Kalshi Rules Watch</title>
<style>
 body{{font-family:system-ui,sans-serif;max-width:1000px;margin:24px auto;padding:0 16px;background:#fafafa;color:#222}}
 .stats{{display:flex;gap:16px;margin:12px 0 20px}} .stat{{background:#fff;border:1px solid #ddd;border-radius:8px;padding:10px 16px}}
 .stat b{{font-size:1.4em;display:block}} .card{{background:#fff;border:1px solid #ddd;border-left:5px solid #aaa;border-radius:6px;padding:12px;margin:10px 0}}
 .card.major{{border-left-color:#d33}} .badge{{font-size:.75em;font-weight:700;padding:2px 6px;border-radius:4px;background:#eee}}
 .major .badge{{background:#d33;color:#fff}} .head{{display:flex;gap:8px;align-items:center;flex-wrap:wrap}}
 .muted{{color:#777;font-size:.85em}} .title{{margin:6px 0;font-weight:600}}
 .compare{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px}}
 @media (max-width:700px){{.compare{{grid-template-columns:1fr}}}}
 .side{{border:1px solid #e3e3e3;border-radius:6px;padding:10px;line-height:1.5;font-size:.93em}}
 .before{{background:#fff8f8}} .after{{background:#f7fff7}}
 .label{{font-size:.72em;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:#777;margin-bottom:4px}}
 del{{background:#fdd;color:#900}} ins{{background:#dfd;color:#060;text-decoration:none}}
 label{{cursor:pointer}} .market-picker{{margin-top:8px}} .market-picker select{{max-width:100%;padding:5px}}
 .hide-minor .card[data-sev=MINOR]{{display:none}}
</style></head><body>
<h1>Kalshi Rules Watch</h1>
<div class="muted">Last check: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · auto-refreshes every 5 min</div>
<div class="stats"><div class="stat"><b>{n_markets:,}</b>open markets tracked</div>
<div class="stat"><b>{len(rows)}</b>changes logged</div><div class="stat"><b>{n_major}</b>major</div></div>
<label><input type="checkbox" onchange="document.body.classList.toggle('hide-minor',this.checked)"> Major only</label>
{''.join(cards) or '<p>No rule changes detected yet.</p>'}
<script>
document.querySelectorAll(".market-select").forEach(select => {{
  select.addEventListener("change", () => {{
    const option = select.selectedOptions[0];
    const card = select.closest(".card");
    card.querySelector(".title").textContent = option.dataset.title;
    const link = card.querySelector(".market-link");
    link.textContent = option.dataset.ticker;
  }});
}});
</script>
</body></html>"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(page)


# ---------------------------------------------------------------- main
def run_once(threshold, alert_minor):
    print(f"{datetime.now():%H:%M:%S} fetching open markets...")
    df = to_dataframe(fetch_events())
    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        first_run, changes = check_for_changes(conn, df, threshold)
        write_dashboard(conn, len(df))

    if first_run:
        print(f"  baseline saved for {len(df):,} markets. Changes will be flagged from the next run.")
        return
    major = [c for c in changes if c["severity"] == "MAJOR"]
    print(f"  {len(df):,} markets checked · {len(changes)} changed · {len(major)} major")
    for c in changes:
        print(f"  [{c['severity']}] {c['ticker']} {c['field']}: {c['reason']}")
    send_alerts(changes, major_only=not alert_minor)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--loop", type=int, default=0, help="seconds between checks (0 = run once)")
    p.add_argument("--threshold", type=float, default=0.85, help="similarity below this = MAJOR")
    p.add_argument("--alert-minor", action="store_true", help="also push alerts for minor edits")
    args = p.parse_args()

    while True:
        try:
            run_once(args.threshold, args.alert_minor)
        except Exception as e:
            if not args.loop:
                raise
            print(f"  ! check failed: {e}")
        if not args.loop:
            break
        time.sleep(args.loop)


if __name__ == "__main__":
    main()
