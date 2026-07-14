# Vega plot dashboard (`vega-plot/`)

Static dashboard that loads **`meta.json`**, shows **tabs** (one per screen), and embeds **Vega-Lite** charts whose CSV data comes from the analytic HTTP **`/sql/…`** endpoint (see main rsiotmonitor docs for server setup).

### For AI assistants and tooling

| Resource | Use |
|----------|-----|
| **[llm.txt](./llm.txt)** | Plain-text bundle of assistant-oriented prose (repo layout, Vega/SQL hints, links) |
| **[AGENTS.md](./AGENTS.md)** | Short workflows: new chart, new tab, layout, troubleshooting — tuned for suggestions |
| **[agent-guide.json](./agent-guide.json)** | Structured JSON: concepts, task steps, `meta.json` quick reference, constraints |
| **[meta.schema.json](./meta.schema.json)** | Validates **`meta.json`** shape |

Serve this directory over **HTTP** (not `file://`) so `fetch()` can load `meta.json` and Vega JSON files.

```bash
cd vega-plot && python3 -m http.server 8000
```

Then open `http://localhost:8000/` (or your host). Query params **`?year=2026&month=4`** initialise the period filter.

---

## Files

| File | Purpose |
|------|---------|
| **`index.html`** | SPA: tabs, year/month filter (‹ › arrows), `{{YEAR}}` / `{{MONTH}}` substitution in spec URLs, lazy chart load per tab, CSV fetch → Vega embed |
| **`meta.json`** | Dashboard config: title, defaults, intro HTML, **`screens`** (tabs + spec paths) |
| **`meta.schema.json`** | JSON Schema for **`meta.json`** — set `"$schema": "./meta.schema.json"` in `meta.json` for editor validation |
| **`llm.txt`** | Plain-text guidance for assistants (not loaded by the UI) |
| **`AGENTS.md`** | Agent-oriented workflows and “what to edit” map |
| **`agent-guide.json`** | Machine-readable tasks, concepts, and constraints for assistants |
| **`events.json`**, **`vega-spec.json`** | Example Vega-Lite v5 specs (CSV via `data.url`) |
| **`exemples_graphs.md`** | Extra Vega URL examples (French labels / samples) |

---

## `meta.json` structure

Validated by **`meta.schema.json`**.

- **`pageTitle`** — Browser title and main heading.
- **`defaults`** — `{ "year", "month" }` for the first paint (overridden by `?year=` / `?month=`).
- **`introHtml`** — HTML under the title (trusted content only).
- **`screens`** — Array of **tabs**.

Each **screen** object:

| Field | Description |
|-------|-------------|
| **`id`** | Stable id for `#fragment` links and DOM (`a-z`, `A-Z`, `0-9`, `_`, `-`). |
| **`title`** | Tab label. |
| **`description`** / **`descriptionHtml`** | Optional copy inside the panel (`descriptionHtml` is not escaped). |
| **`spec`** | Relative path to **one** Vega-Lite JSON file. |
| **`charts`** | Non-empty array overrides **`spec`**: multiple charts on **one** tab. Each entry: **`spec`**, optional **`id`**, **`title`**, **`description`**, **`layout`**. |
| **`layout`** | Screen-level layout: `"stack"` \| `"row"` or `{ "flow": "stack"\|"row", "gap": "16px" }`. For a single-`spec` screen only: `{ "defaultChart": { "span": "half"\|"full"\|"auto" } }`. |

Per-chart **`layout`** (inside **`charts[]`**):

- **`span`**: `"half"` | `"full"` | `"auto"` — width in row flow.
- **`breakBefore`** / **`newLine`** / **`lineBreak`**: start a new row (flex line break); in **`stack`** flow, adds spacing / divider.

---

## Period filter and SQL URLs

The UI replaces placeholders **everywhere** in Vega **`url`** strings (whole spec tree):

| Placeholder | Replaced with |
|-------------|----------------|
| **`{{YEAR}}`** | Selected year (integer). |
| **`{{MONTH}}`** | Selected month **1–12** (no zero-padding). |

Legacy URLs may use **`year=2026`** and **`month=4`**; numeric **`year=`** / **`month=`** fragments are rewritten too.

Example fragment inside `data.url`:

```text
... and year={{YEAR}} and month={{MONTH}} ...
```

**Bookmarking:** `?year=2026&month=4` syncs with the dropdowns (History API).

---

## Vega-Lite spec files (e.g. `events.json`)

1. Use **`"$schema": "https://vega.github.io/schema/vega-lite/v5.json"`** for editor completion.
2. Point CSV data at the rsiotmonitor HTTP SQL route:

   ```json
   "data": {
     "url": "http://HOST:PORT/sql/<URL-encoded SQL>",
     "format": { "type": "csv" }
   }
   ```

3. Encode the SQL as the path segment after **`/sql/`** (spaces and quotes appear URL-encoded).
4. Align **`year=` / `month=`** (or **`{{YEAR}}` / `{{MONTH}}`**) with Hive-style partitions if your query filters `mqtt_hive` by month/year.
5. Ensure **CORS** allows your dashboard origin if the page host differs from `HOST:PORT`.
6. Prototype in the **[Vega Editor](https://vega.github.io/editor/)**, then save JSON under **`vega-plot/`** and reference it from **`meta.json`**.

---

## Lazy loading

Specs and CSV requests run **only when the user opens that tab** (first time). Switching year/month **refetches** charts whose Vega JSON has already been loaded (visited tabs). Tabs never opened stay unloaded until selected.

---

## Links

- [Vega Editor](https://vega.github.io/editor/)
- [Vega-Lite docs](https://vega.github.io/vega-lite/)
- [Vega-Lite v5 JSON Schema](https://vega.github.io/schema/vega-lite/v5.json) — use as `$schema` inside each chart JSON
- **`meta.schema.json`** in this folder — schema for **`meta.json`**

---

## Assistant-oriented prose

**`llm.txt`** holds the same kind of guidance that used to live under **`meta.json` → `llmDocumentation`**, as plain text. The dashboard **does not** fetch it; open it in the repo or editor when helping with specs and **`meta.json`**.

Use **`AGENTS.md`** + **`agent-guide.json`** for workflows and structured tasks; **`llm.txt`** for a single readable scratchpad of conventions and links.

---

## Troubleshooting

| Issue | What to check |
|-------|----------------|
| Blank meta / errors | Serve over HTTP; check browser console and Network tab for `meta.json` / Vega JSON / CSV |
| CORS errors on CSV | Server must send appropriate `Access-Control-Allow-Origin` for the page origin |
| Wrong month data | Confirm **`{{YEAR}}`/`{{MONTH}}`** or literals match your SQL `WHERE` clause |
| Vega “sanitize” / blob errors | This app fetches CSV and uses `data:` or inline values as needed; use current **`index.html`** |
