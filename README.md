# AToL Sample Metadata Validator & Pipeline Tracker

A toy version of the problem AToL's assembly pipeline actually has: messy
sample metadata arrives from an upstream portal, gets validated against a
schema, rejected records come back with useful field-level errors, accepted
records get stored, and the API reports where each sample sits in the
pipeline and whether it's still under embargo.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

(`requirements.txt` was frozen from a working install; `pip install
"fastapi[standard]" sqlmodel` gets you the same thing from scratch.)

## Run

```bash
source venv/bin/activate
fastapi dev main.py
```

Then open **http://127.0.0.1:8000/docs** for the interactive Swagger UI —
generated entirely from the Pydantic type annotations in `main.py`, no
hand-written docs involved.

A SQLite file `atol.db` is created next to `main.py` on first run. Delete
it any time to reset. The path is overridable via `ATOL_DATABASE_URL`
(e.g. `ATOL_DATABASE_URL="sqlite:////tmp/atol.db" fastapi dev main.py`) —
useful if you ever run this through a network-mounted or FUSE-backed
folder, since SQLite's journal/locking doesn't always get on with those.

If you have an `atol.db` from before the pipeline stages were renamed to
match the real Genome Engine's terminology (see below), the app migrates
it automatically and safely the next time it starts — old stage names get
renamed in place and the two new columns get added. Nothing is dropped.
See `_run_startup_migrations()` in `main.py` if you want to see exactly
what it does before trusting it with real data.

## Endpoints

The six required ones:

| Method | Path | Purpose |
|---|---|---|
| POST | `/samples` | Ingest one record, validate, store |
| POST | `/samples/batch` | Ingest many; per-record accept/reject with reasons |
| GET | `/samples` | List, optionally `?stage=ingested` etc. |
| GET | `/samples/{id}` | One sample, with embargoed fields stripped if still embargoed |
| PATCH | `/samples/{id}/stage` | Advance one step through the pipeline |
| GET | `/status` | Counts by stage, plus how many are blocked on validation |

Plus three that simulate the parts of the real Genome Engine this toy
didn't originally touch (see **Simulating the Genome Engine** below —
these aren't part of the required six and don't appear in `/docs`):

| Method | Path | Purpose |
|---|---|---|
| POST | `/samples/{id}/broker` | Simulate brokering to ENA; mints a fake accession |
| POST | `/samples/{id}/genome-note` | Simulate drafting the automatic Genome Note |
| GET | `/simulate/bioplatforms-ingest` | Fabricates a messy raw batch, shaped like a real portal response |

Pipeline stages, in order: `ingested` → `metadata_processed` → `assembled`
→ `annotated` → `brokered` → `note_generated`. `PATCH .../stage` only
allows moving to the *next* stage — no skipping, no going backward — with
one further restriction: it refuses to move a sample into `brokered` or
`note_generated` at all. Those two require the dedicated action endpoints
above, because getting there does real work (minting an accession, drafting
a note) rather than just changing a string. Try it: PATCH an annotated
sample straight to `brokered` and read the 400.

## Test console

A hand-built page for poking at the API without typing curl commands. It's
served by the app itself, so no separate server or CORS setup is needed —
just open it once `main.py` is running:

**http://127.0.0.1:8000/static/test.html**

One card per endpoint: fill in a form, hit send, and the actual status code
and JSON response show up right below it. Two buttons on the `POST
/samples` card are worth trying first — "Fill valid example" and "Fill
broken example" — the latter loads a record with a malformed `tolid` and
an `embargo_until` before `collection_date` at once, so you can see the
structured 422 error list without typing it out by hand. The batch card
comes pre-loaded with a mix of good, bad, and duplicate records for the
same reason.

This isn't one of the six required endpoints and doesn't appear in
`/docs` — it's a static file (`static/test.html`) mounted separately, purely
as a dev convenience. `/docs` is still the source of truth for the actual
API contract.

## Admin panel

A live log of every request under `/samples` and `/status` and exactly what
the API sent back, successful or not:

**http://127.0.0.1:8000/static/admin.html**

Click any row to expand it and see the raw request and response JSON side
by side. Filter by status class (2xx/4xx/5xx), or turn on auto-refresh to
watch it update while you use the test console in another tab. Backed by
`GET /admin/log`, which reads a `SubmissionLog` table populated by HTTP
middleware (`log_submissions` in `main.py`) — not by the endpoints
themselves, which don't know it exists. That's deliberate: audit logging is
a cross-cutting concern that belongs on the pipe every request already
flows through, the same job a Laravel middleware class registered in the
kernel does, just declared with `@app.middleware("http")` instead of a
kernel array.

Worth noting this is a different table from the `RejectedRecord` one behind
`blocked_on_validation` on `/status`: that one exists specifically to count
validation failures. `SubmissionLog` is the broader "what actually
happened" record — it captures successful requests too, and the exact
response body that went out, not just the fact that something failed.

Like the test console, this isn't one of the six required endpoints and
doesn't appear in `/docs`.

## Simulating the Genome Engine

The real AToL Genome Engine does six things: ingests raw sequence data from
the Bioplatforms Australia Data Portal, processes sampling/sequencing
metadata, assembles genomes, annotates them, brokers metadata/reads/
assemblies to the European Nucleotide Archive (ENA), and drafts an
automatic Genome Note. This toy always covered the shape of metadata
validation and pipeline tracking; it didn't originally touch the ends of
that list — where data comes from, and what happens after annotation. Three
additions close that gap, each clearly marked as simulation rather than a
real integration:

- **`GET /simulate/bioplatforms-ingest`** fabricates a batch of raw records
  shaped like an actual Bioplatforms Data Portal response — including the
  exact kind of messiness a real ingest step has to sort through: a package
  carrying the portal's own extra fields (`bpa_package_id`,
  `bpa_dataset_url`) that `SampleIn`'s `extra="forbid"` rejects outright, a
  record using the portal's dataset ID instead of a ToLID, and a duplicate
  package from listing-pagination overlap. None of it touches the
  database — it's meant to be fed straight into `POST /samples/batch` (the
  test console's "Load simulated Bioplatforms batch" button does exactly
  that), so the same validation boundary the rest of the app demonstrates
  is what sorts out this messiness too, same as it would for the real
  thing.
- **`POST /samples/{id}/broker`** simulates submission to ENA: mints a
  fake accession (`PRJEB#####`) rather than calling any real API, and only
  works once a sample is `annotated`.
- **`POST /samples/{id}/genome-note`** simulates the automatic report
  drafting step: templates a short markdown Genome Note (sampling,
  sequencing platform, ENA accession, assembly summary) from data already
  on the row, and only works once a sample is `brokered`.

What's *not* simulated, deliberately: there's no real Bioplatforms API
call, no real NCBI Taxonomy lookup to resolve `taxon_id`, no real ENA
submission, and no real genome assembly compute. Those are the genuinely
hard parts of the real system — modelling them honestly as "not attempted
here" is more useful for an interview than pretending a toy project
replicates them.

**End to end, via curl:**

```bash
# 1. ingest (from the simulated portal, or by hand)
curl -X POST http://127.0.0.1:8000/samples -H "Content-Type: application/json" -d '{
  "tolid": "maNotRufo1", "scientific_name": "Notamacropus rufogriseus",
  "taxon_id": 9317, "collection_date": "2026-03-02", "platform": "pacbio_hifi"
}'

# 2. walk it through metadata processing, assembly, annotation
curl -X PATCH http://127.0.0.1:8000/samples/1/stage -d '{"stage": "metadata_processed"}' -H "Content-Type: application/json"
curl -X PATCH http://127.0.0.1:8000/samples/1/stage -d '{"stage": "assembled"}'           -H "Content-Type: application/json"
curl -X PATCH http://127.0.0.1:8000/samples/1/stage -d '{"stage": "annotated"}'           -H "Content-Type: application/json"

# 3. broker to ENA (mints ena_accession, moves to 'brokered')
curl -X POST http://127.0.0.1:8000/samples/1/broker

# 4. draft the Genome Note (moves to 'note_generated', the final stage)
curl -X POST http://127.0.0.1:8000/samples/1/genome-note
```

## Try it

A valid record, this time via curl instead of the test console:

```bash
curl -X POST http://127.0.0.1:8000/samples -H "Content-Type: application/json" -d '{
  "tolid": "drMusMusc1",
  "scientific_name": "Mus musculus",
  "taxon_id": 10090,
  "collection_date": "2026-01-15",
  "platform": "pacbio_hifi"
}'
```

A deliberately broken one — bad tolid pattern, an extra field, and a
cross-field embargo violation all at once — to see the structured 422:

```bash
curl -X POST http://127.0.0.1:8000/samples -H "Content-Type: application/json" -d '{
  "tolid": "not-a-valid-id",
  "scientific_name": "Homo sapiens",
  "taxon_id": 9606,
  "collection_date": "2026-06-01",
  "embargo_until": "2026-01-01",
  "platform": "pacbio_hifi",
  "sequencing_centre": "unexpected extra field"
}'
```

Batch ingest with a mix of good and bad records:

```bash
curl -X POST http://127.0.0.1:8000/samples/batch -H "Content-Type: application/json" -d '[
  {"tolid": "drMusMusc3", "scientific_name": "Mus musculus", "taxon_id": 10090, "collection_date": "2026-02-01", "platform": "ont"},
  {"tolid": "BADID1234", "scientific_name": "Bad Id", "taxon_id": 1, "collection_date": "2026-01-01", "platform": "hic"}
]'
```

## Design notes 

**Three separate models, one lesson.** `SampleIn` (the validation
boundary), `Sample` (the SQLModel storage table), and `SampleOut` (the
response shape) are three different classes even though they share most
fields. That separation is what makes the embargo real: `SampleOut`
strips `scientific_name`, `taxon_id`, `collection_date` and `platform`
whenever `embargo_until` is still in the future, computed fresh against
today's date rather than trusting every caller to check it themselves.
This is the same shape as AToL's INSDC-compliant intermediary schema.

**The spec, the docs UI, and the 422 contract are all derived, not
written.** Nothing in `/docs` was authored by hand — it all comes from
the type annotations on `SampleIn` and friends. In Laravel, a form
request class, the controller, and any API documentation are three
separate artefacts you keep in sync by hand; here there's one source of
truth.

**Cross-field validation lives on the model, not the controller.** The
`embargo_until < collection_date` check is a `@model_validator(mode="after")`
on `SampleIn`, so it runs automatically on every request shaped like a
sample, single or batch, with no endpoint code needing to remember to
call it.

**The batch endpoint deliberately doesn't reuse `SampleIn` as its request
body type.** If it did, FastAPI would 422 the *entire* request the moment
one record failed, and the other thirty-nine would never be looked at.
Taking `list[dict]` and calling `SampleIn.model_validate()` per item
inside the loop is what makes "tell me which twelve of forty are wrong
and why" possible at all.

**Business rules that need database state don't live on the Pydantic
model.** `next_stage()` enforcing strictly sequential pipeline movement
can't be a `SampleIn` validator — it needs to know the sample's *current*
stored stage, and a Pydantic model validates a payload in isolation with
no idea what's already in the database. That rule lives in the endpoint
instead. Worth naming out loud: shape validation and business-rule
validation are different concerns that end up living in different places,
and Pydantic doesn't try to pretend otherwise.

**Rejected records aren't just dropped.** A `RejectedRecord` table (raw
payload + cleaned errors) backs the `blocked_on_validation` count in
`/status`. Without it, a rejected record vanishes the moment the 422
response is sent — fine for a demo, not fine for a researcher trying to
figure out, a day later, which of their forty samples never made it in
and why.

**A generic PATCH isn't always the right shape.** `PATCH /samples/{id}/stage`
refuses to move a sample into `brokered` or `note_generated`, even though
they're the mechanically "next" stage — reaching them mints a real ENA
accession or drafts a real note, which is a side effect a bare state flip
shouldn't quietly trigger. Those get their own `POST` action endpoints
instead. It's a small, concrete case of a bigger REST-design question:
when is a resource update just PATCH-a-field, and when does it need to be
a named action with its own endpoint because something actually happens?

**What FastAPI *doesn't* give you.** No ORM, no migrations, no auth, no
queue, no job runner — SQLModel/SQLAlchemy, Alembic, and everything else
here is assembled by hand. `_run_startup_migrations()` in `main.py` is
that gap made concrete: when the pipeline stages got renamed and two
columns got added, there was no `php artisan make:migration` to reach
for — just a hand-written function that inspects the existing schema and
patches it, run once at startup. Coming from Laravel's batteries-included
defaults, that's the real adjustment, and a more credible thing to say in
an interview than "the validation is nice."
