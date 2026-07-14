# Agent instructions — `vega-plot/` dashboard

Use this file with **`llm.txt`** (plain-text assistant bundle), **`agent-guide.json`** (machine-readable tasks), and **`README.md`** (full prose).

## Mental model

| Concept | Meaning |
|--------|---------|
| **Screen** | One **tab** → one element of **`meta.json` → `screens`** |
| **Chart** | One **Vega-Lite JSON file** (`*.json`) referenced by **`spec`** or **`charts[].spec`** |
| **Runtime** | **`index.html`** loads **`meta.json`**, builds tabs, **lazy-loads** each chart when its tab is first opened |
| **Period filter** | Global **Year / Month** (and ‹ ›) rewrite **`{{YEAR}}`**, **`{{MONTH}}`**, and numeric **`year=`** / **`month=`** inside every **`url`** in the Vega spec |

## Vega-Lite: LLM-efficient references (low context)

Chart files are **Vega-Lite v5** JSON. Prefer **`"$schema": "https://vega.github.io/schema/vega-lite/v5.json"`** in each spec. Fetch **one** source at a time, in this order (stop when you have enough):

1. **[Vega-Lite v5 JSON Schema](https://vega.github.io/schema/vega-lite/v5.json)** — densest: allowed keys, nesting, types for `data`, `mark`, `encoding`, `transform`, `config`, …
2. **[encoding](https://vega.github.io/vega-lite/docs/encoding.html)** — most edits touch `x` / `y` / `color` / `tooltip`, field types, aggregates, `timeUnit`, scales.
3. **[transform](https://vega.github.io/vega-lite/docs/transform.html)** — `calculate`, `filter`, `aggregate`, …
4. **[mark](https://vega.github.io/vega-lite/docs/mark.html)** — only if changing geometry beyond common marks (`bar`, `rect`, `point`, …).

**`calculate` expressions** use [Vega expressions](https://vega.github.io/vega/docs/expressions/) (not Vega-Lite-specific). **Smoke-test** a spec: [Vega Editor](https://vega.github.io/editor/).

**Raw Vega** (not used for these dashboard charts): [Vega v5 JSON Schema](https://vega.github.io/schema/vega/v5.json). Skip unless the user explicitly needs Vega.

## File map (what to edit)

| User goal | Primary files |
|-----------|----------------|
| New tab / “page” | **`meta.json`** — append to **`screens`** |
| New graphic / chart | Create **`vega-plot/<name>.json`** (Vega-Lite), then reference it from **`meta.json`** |
| Rename titles, intro, defaults | **`meta.json`** (`pageTitle`, `introHtml`, `defaults`, `screens[].title`, …) |
| Layout (side‑by‑side, new row) | **`meta.json`** — `screens[].layout`, `charts[].layout` |
| SQL / time range | Vega **`data.url`** in the spec file (`{{YEAR}}` / `{{MONTH}}`) |
| Validate config shape | **`meta.schema.json`** — set **`"$schema": "./meta.schema.json"`** in **`meta.json`** |

## Suggested responses when helping users

### “Add a new chart”

1. Duplicate **`events.json`** or **`vega-spec.json`** as a template.
2. Set **`$schema`** to `https://vega.github.io/schema/vega-lite/v5.json`.
3. Put **`data.url`** = `http://<host>:<port>/sql/<URL-encoded SQL>` with **`format.type: csv`**.
4. Use **`year={{YEAR}} and month={{MONTH}}`** (or literals) in SQL when filtering **`mqtt_hive`** partitions.
5. Add **`"spec": "<file>.json"`** on a screen, or add an entry under **`charts`** for multi-chart tabs.
6. Remind: serve **`vega-plot/`** over **HTTP**; **CORS** if page origin ≠ API host.

### “Add a new tab / page”

1. Append one object to **`meta.json` → `screens`** with unique **`id`** and **`title`**.
2. Either **`spec`: `"one-file.json"`** or **`charts`: [{ `"spec": "a.json"` }, …]`**.
3. Optional **`description`** / **`descriptionHtml`** for text under the tab title.

### “Charts don’t load / wrong data”

Work through: HTTP not `file://` → Network tab **`meta.json`** / Vega JSON / **`/sql/`** CSV → **CORS** → SQL matches **`year`/`month`** and placeholders → tab was opened at least once if debugging lazy load.

### “Change layout”

- Single column default: omit **`layout`** or **`"layout": "stack"`**.
- Two columns: **`"layout": { "flow": "row", "gap": "20px" }`** + per chart **`"layout": { "span": "half" }`**.
- Force full-width row after others: **`"layout": { "breakBefore": true, "span": "full" }`** on that chart entry.

## Structured task index

See **`agent-guide.json`** → **`tasks`** for step lists and **`constraints`** for pitfalls.

## Do not

- Rely on browsers loading **`file://`** for **`fetch(meta.json)`**.
- Put secrets in **`meta.json`** or Vega JSON (they are static assets).
- Assume unnamed **`charts[]`** slots without **`spec`** — runtime expects a path per chart.
