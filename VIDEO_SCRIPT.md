# OmniFlow — 2-Minute Video Blueprint

**Target Audience:** CTOs, founders, senior eng leaders (US / SV)
**Platform:** X (vertical ~9:16) + LinkedIn (square 1:1)
**Tone:** Direct, no fluff, pattern-interrupt opener

---

## SEGMENT 1 — HOOK (0:00–0:12)

| Screen | Visual | Audio |
|--------|--------|-------|
| Dark screen, cursor blinking in a terminal. Slowly a `docker-compose.yml` scrolls past, then a 200-line Python glue script, then a Kafka config. | "Every data pipeline starts simple. Then reality hits." Fade to: file after file of glue code, yaml, bash wrappers scrolling up. | **Speech:** *"Every data pipeline starts simple. Then you add a validator. Then a transformer. Then a router. And suddenly you're maintaining 2,000 lines of glue code between five microservices."* |
| Cut to black. White text appears letter by letter: **"There's a better way."** | | |

---

## SEGMENT 2 — WHAT IS IT (0:12–0:30)

| Screen | Visual | Audio |
|--------|--------|-------|
| Split screen. Left: messy spaghetti diagram. Right: clean linear pipeline with labeled boxes. The messy side dissolves. | Animated pipeline stages flowing left to right: `[Ingest] → [Validate] → [Transform] → [Enrich] → [Route] → [Store]` | **Speech:** *"Meet OmniFlow. An open-source document processing framework. You define the pipeline as a DAG — validation, transformation, enrichment, routing — each stage is a pluggable async processor."* |
| Stage boxes pop with icons: shield (validate), gear (transform), magnifier (enrich), fork (route), database (store). | | *"Write a processor, configure it in JSON, and the pipeline executor handles the rest."* |

---

## SEGMENT 3 — CODE SHOW (0:30–0:55)

| Screen | Visual | Audio |
|--------|--------|-------|
| Full screen code editor. Open `pipeline.json`. Clean, minimal JSON with stages array. | Syntax-highlighted JSON showing a 4-stage pipeline definition. | **Speech:** *"Here's what a pipeline looks like. JSON. Four stages. Each with a processor class and config. That's it."* |
| Scroll down. Show `SchemaValidator` config: `required_fields`, `field_constraints`. | Highlight the config keys as they're mentioned. | *"Validation? Schema-driven — required fields, type coercion, regex patterns. Transformation? Rename, flatten, compute fields with safe expressions."* |
| Quick cut to `DataEnricher` config with `dedup_fields` and `entity_types`. | | *"Enrichment with built-in dedup and entity extraction. Routing by tenant, payload, or tags."* |

---

## SEGMENT 4 — WHY IT MATTERS (0:55–1:25)

| Screen | Visual | Audio |
|--------|--------|-------|
| Animated architecture overview. Left: file/API sources. Middle: pipeline stages processing documents. Right: database, file, console storage. | Boxes with data flowing through. Each processor shows a checkmark or X with fail→reject routing. | **Speech:** *"What makes this different? First, it's async from the ground up — no blocking I/O. Second, every processor validates before it processes — bad data gets rejected, not corrupted."* |
| Overlay a metrics panel. Show counters incrementing, timing bars, audit log entries appearing live. | | *"Third, observability is built in — metrics collectors, audit logs, circuit breakers for resilience."* |
| Show `PipelineExecutor.execute_stream()` — async generator processing documents as they arrive. | | *"And fourth, streaming. Documents flow through the pipeline as they arrive — no batch delays."* |

---

## SEGMENT 5 — CALL TO ACTION (1:25–2:00)

| Screen | Visual | Audio |
|--------|--------|-------|
| Single screen with GitHub repo URL, star count animation. | `github.com/your-org/omniflow` with a counter ticking up. | **Speech:** *"We're open-sourcing OmniFlow today. If you're tired of maintaining brittle ingestion pipelines, go check it out. Star it, fork it, steal the architecture — it's MIT."* |
| Fade to: 3 bullet points: `Plug-and-play processors`, `Async-native`, `JSON-configured DAGs` | | *"Plug-and-play processors, async-native, configured in plain JSON. Link in the first comment."* |
| End screen: logo + `omniflow.dev` (or repo URL) | | *"What's the messiest pipeline you've had to untangle? Drop it in the comments — I'd love to hear."* |

---

## PRODUCTION NOTES

- **Total:** ~2 min (target 1:45–2:00 for retention)
- **Music:** Low-fi electronic or subtle ambient — no lyrics, no big drops
- **Captions:** Essential. X autoplays muted. Use captions for every line.
- **Pacing:** Fast cuts in segment 1–2 (every 3–4 seconds), slower in segment 3 (code needs time to read)
- **Color:** Dark theme (terminal vibe) with one accent color (cyan or green) for highlights
- **Format:** 9:16 vertical for X/Reels, 1:1 square for LinkedIn with letterboxing
