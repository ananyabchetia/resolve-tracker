# Kalshi Rules Watch

This watcher snapshots the resolution rules for open Kalshi markets, records
changes in SQLite, and generates a static `dashboard.html`.

## GitHub Pages deployment

The workflow in `.github/workflows/watch.yml` checks the Kalshi API every 15
minutes, retains its SQLite database in the Actions cache, and deploys the
generated dashboard to GitHub Pages. The dashboard is public even though the
repository can remain private.

1. Push this project to a **private** GitHub repository.
2. In **Settings → Pages**, choose **GitHub Actions** as the build and
   deployment source.
3. Ensure the repository's plan allows Pages to be published from a private
   repository. Once enabled, the public site URL appears in the Pages settings.
4. The first workflow run creates the baseline; rule changes are detected on
   later runs. Use **Actions → Kalshi Rules Watch → Run workflow** to start it
   immediately.

The local database, virtual environment, notebook, and generated dashboard are
excluded from Git. The workflow initializes its own baseline and persists that
state between scheduled runs.

## Run locally

```sh
python3 -m pip install -r requirements.txt
python3 kalshi_rules_watch.py
```

Use `python3 kalshi_rules_watch.py --loop 900` to check every 15 minutes.
