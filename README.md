# Kalshi Rules Watch

This watcher snapshots the resolution rules for open Kalshi markets, records
changes in SQLite, and generates a static `dashboard.html`.
Changes to the same rule field across markets in one event are grouped into a
single dashboard card with a market selector for each market's rule comparison.
The dashboard shows five change cards per page, with search and pagination
through the full change history.
