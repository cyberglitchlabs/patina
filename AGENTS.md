# Patina

Groomed XMLTV guide service for Jellyfin. Terms: `GLOSSARY.md`. Run what `.github/workflows/ci.yml` runs before and after every change. For changes under `charts/patina`, also run `scripts/chart-check.sh all` (what `.github/workflows/chart.yml` runs).

- Commit with the no-reply identity in the repo-local git config; ask the user before any push (a push to `main` publishes a public image).
- This repo is public: write the example ZIP `55401` and example address `192.168.1.50` in docs, fixtures and commits, never the real ones.
- Open `CONTRIBUTING.md` when you change the Helm chart, migrations, XMLTV output, fetching or request gates, refactor code, or edit docs.
- Open `docs/adr/` when a design choice looks odd, and add one there when a choice is hard to reverse, surprising, and a real trade-off.
