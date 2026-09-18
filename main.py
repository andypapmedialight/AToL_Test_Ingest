"""
AToL Sample Metadata Validator & Pipeline Status Tracker
==========================================================

A toy version of the problem AToL's assembly pipeline actually has:
messy sample metadata arrives from an upstream portal, gets validated
against a schema, rejected records get useful errors back, accepted
records get stored, and the API can report where each sample sits in
the assembly pipeline and whether it's still under embargo.

Run it with:
    fastapi dev main.py
Then open http://127.0.0.1:8000/docs for the interactive Swagger UI,
generated entirely from the type annotations below.
"""

from __future__ import annotations

import json
import os
import random
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from enum import Enum
from typing import Literal

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import inspect, text
from sqlmodel import Field as SQLField
from sqlmodel import Session, SQLModel, create_engine, select

# ---------------------------------------------------------------------------
# Pipeline stages
#
# Named after the real AToL Genome Engine's six described steps (ingest from
# the Bioplatforms Data Portal, process metadata, assemble, annotate, broker
# to ENA, draft a Genome Note) rather than generic placeholders -- a plain
# Enum, reused as: the `stage` type on the input/output models, the
# query-filter type on GET /samples, and the source of truth for what
# "advance to the next stage" means in PATCH /samples/{id}/stage.
#
# The last two stages, `brokered` and `note_generated`, can't be reached via
# the generic PATCH endpoint -- they require POST /samples/{id}/broker and
# POST /samples/{id}/genome-note respectively, because unlike a bare state
# flip, reaching them has a real side effect (an ENA accession gets minted;
# a Genome Note gets drafted). See advance_stage() below for where that's
# enforced.
# ---------------------------------------------------------------------------


class Stage(str, Enum):
    ingested = "ingested"
    metadata_processed = "metadata_processed"
    assembled = "assembled"
    annotated = "annotated"
    brokered = "brokered"
    note_generated = "note_generated"


STAGE_ORDER: list[Stage] = [
    Stage.ingested,
    Stage.metadata_processed,
    Stage.assembled,
    Stage.annotated,
    Stage.brokered,
    Stage.note_generated,
]

# Stages that a dedicated action endpoint must be used to reach, because
# getting there does real work beyond flipping a string -- see
# advance_stage() and the broker_to_ena / generate_genome_note endpoints.
_ACTION_ONLY_STAGES: dict[Stage, str] = {
    Stage.brokered: "POST /samples/{id}/broker",
    Stage.note_generated: "POST /samples/{id}/genome-note",
}


def next_stage(current: Stage) -> Stage | None:
    """The only stage a sample is allowed to move to next, or None if it's
    already at the end of the pipeline. Advancement is strictly sequential:
    no skipping stages, no moving backward. That's a business rule, not a
    shape rule, so it lives here rather than on SampleIn -- it needs to see
    the sample's *current* stored state, and a Pydantic model validates a
    payload in isolation, with no idea what's already in the database."""
    idx = STAGE_ORDER.index(current)
    if idx + 1 < len(STAGE_ORDER):
        return STAGE_ORDER[idx + 1]
    return None


# ---------------------------------------------------------------------------
# Input model -- the validation boundary
#
# Nothing reaches storage without passing through SampleIn first.
# `extra="forbid"` matters here: if the upstream portal renames or
# misspells a field, you want a loud 422 telling you that, not a silently
# dropped column.
# ---------------------------------------------------------------------------


class SampleIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tolid: str = Field(
        pattern=r"^[a-z]{2}[A-Z][a-z]{2}[A-Z][a-z]{3}\d+$",
        description="Tree of Life ID, e.g. 'drMusMusc1'",
    )
    scientific_name: str = Field(min_length=1)
    taxon_id: int = Field(gt=0)
    collection_date: date
    embargo_until: date | None = None
    platform: Literal["pacbio_hifi", "ont", "hic"]

    @model_validator(mode="after")
    def embargo_after_collection(self) -> "SampleIn":
        # Cross-field rule: this can't be expressed as a per-field Field(...)
        # constraint because it needs two fields at once. It lives on the
        # model, right next to the fields it checks, and runs automatically
        # on every request that reaches this shape -- no controller code
        # has to remember to call it.
        if self.embargo_until and self.embargo_until < self.collection_date:
            raise ValueError("embargo_until precedes collection_date")
        return self


# ---------------------------------------------------------------------------
# Storage model -- what actually lives in SQLite
#
# Deliberately a separate class from SampleIn. Storage needs columns the
# upstream payload doesn't carry (id, stage, received_at), and the input
# schema should be free to evolve without that meaning a database
# migration every time.
# ---------------------------------------------------------------------------


class Sample(SQLModel, table=True):
    id: int | None = SQLField(default=None, primary_key=True)
    tolid: str = SQLField(index=True, unique=True)
    scientific_name: str
    taxon_id: int
    collection_date: date
    embargo_until: date | None = None
    platform: str
    stage: str = SQLField(default=Stage.ingested.value)
    received_at: datetime = SQLField(default_factory=lambda: datetime.now(timezone.utc))
    # Populated only by the dedicated action endpoints below, once a sample
    # actually reaches that point in the pipeline -- never set directly.
    ena_accession: str | None = None
    genome_note: str | None = None


class RejectedRecord(SQLModel, table=True):
    """An audit trail of everything that *failed* validation, not just what
    passed. This is what GET /status's `blocked_on_validation` counts --
    without it, a rejected record vanishes with nothing to show a
    researcher except the one 422 response they got at the time."""

    id: int | None = SQLField(default=None, primary_key=True)
    raw_payload: str  # JSON-encoded, exactly what was posted
    errors: str  # JSON-encoded list of {loc, msg, type, input}
    received_at: datetime = SQLField(default_factory=lambda: datetime.now(timezone.utc))


class SubmissionLog(SQLModel, table=True):
    """Every request/response that hits the sample API, successful or not --
    what the admin panel reads. This is a broader net than RejectedRecord:
    RejectedRecord exists specifically to back the blocked_on_validation
    count and only ever holds validation failures; SubmissionLog exists to
    answer "what actually happened, end to end" for any request, and is
    populated by HTTP middleware rather than by endpoint code, so no
    endpoint has to remember to write to it."""

    id: int | None = SQLField(default=None, primary_key=True)
    timestamp: datetime = SQLField(default_factory=lambda: datetime.now(timezone.utc))
    method: str
    path: str
    status_code: int
    duration_ms: float
    request_body: str
    response_body: str


# ---------------------------------------------------------------------------
# Output model -- what the API actually returns
#
# Separate again from both SampleIn and Sample. This is the model that
# makes the embargo real: while a sample is still embargoed, the
# scientific details are stripped from the response entirely rather than
# trusting every caller to check embargo_until themselves.
# ---------------------------------------------------------------------------


class SampleOut(BaseModel):
    id: int
    tolid: str
    stage: Stage
    embargoed: bool
    scientific_name: str | None
    taxon_id: int | None
    collection_date: date | None
    embargo_until: date | None
    platform: str | None
    ena_accession: str | None
    genome_note: str | None

    @classmethod
    def from_db(cls, sample: Sample) -> "SampleOut":
        embargoed = sample.embargo_until is not None and sample.embargo_until > date.today()
        return cls(
            id=sample.id,
            tolid=sample.tolid,
            stage=Stage(sample.stage),
            embargoed=embargoed,
            scientific_name=None if embargoed else sample.scientific_name,
            taxon_id=None if embargoed else sample.taxon_id,
            collection_date=None if embargoed else sample.collection_date,
            embargo_until=sample.embargo_until,
            platform=None if embargoed else sample.platform,
            ena_accession=None if embargoed else sample.ena_accession,
            genome_note=None if embargoed else sample.genome_note,
        )


class StageUpdate(BaseModel):
    stage: Stage


class BatchResult(BaseModel):
    index: int
    status: Literal["accepted", "rejected"]
    tolid: str | None = None
    id: int | None = None
    errors: list[dict] | None = None


class BatchResponse(BaseModel):
    total: int
    accepted: int
    rejected: int
    results: list[BatchResult]


class StatusResponse(BaseModel):
    by_stage: dict[str, int]
    total_samples: int
    blocked_on_validation: int


class SubmissionLogOut(BaseModel):
    id: int
    timestamp: datetime
    method: str
    path: str
    status_code: int
    duration_ms: float
    request_body: str
    response_body: str


# ---------------------------------------------------------------------------
# Database wiring
#
# `Depends(get_session)` here is doing the same job as Laravel's service
# container resolving a dependency into a controller method -- except it's
# explicit in the function signature rather than resolved implicitly by
# type-hinting a constructor.
# ---------------------------------------------------------------------------

# Configurable the way a Laravel .env DB_* block is: a sensible default
# that just works for local dev, overridable without touching code.
DATABASE_URL = os.environ.get("ATOL_DATABASE_URL", "sqlite:///./atol.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})


def get_session():
    with Session(engine) as session:
        yield session


# Old stage names -> current ones, for anyone with a database from before the
# pipeline stages were renamed to match the real Genome Engine's terminology.
_LEGACY_STAGE_RENAMES = {
    "received": "ingested",
    "sequencing": "metadata_processed",
    "assembly": "assembled",
    "annotation": "annotated",
    "submitted": "brokered",
}


def _run_startup_migrations(engine) -> None:
    """A hand-rolled migration, run once at startup, before
    SQLModel.metadata.create_all(). This is exactly the kind of thing
    Alembic exists to manage -- FastAPI/SQLModel give you nothing here,
    so on a real project you'd either bring in a migration tool or, for
    something this small, do what this function does: check what's
    actually there and patch it by hand.

    Two jobs: rename any pre-existing rows still using the old stage
    names, and add the two columns (ena_accession, genome_note) that
    didn't exist before the ENA-brokering and Genome-Note endpoints did.
    Both are no-ops on a fresh database, since create_all() builds the
    current schema directly when the table doesn't exist yet."""
    inspector = inspect(engine)
    if "sample" not in inspector.get_table_names():
        return

    existing_columns = {col["name"] for col in inspector.get_columns("sample")}
    with engine.begin() as conn:
        for old_name, new_name in _LEGACY_STAGE_RENAMES.items():
            conn.execute(
                text("UPDATE sample SET stage = :new_name WHERE stage = :old_name"),
                {"new_name": new_name, "old_name": old_name},
            )
        if "ena_accession" not in existing_columns:
            conn.execute(text("ALTER TABLE sample ADD COLUMN ena_accession VARCHAR"))
        if "genome_note" not in existing_columns:
            conn.execute(text("ALTER TABLE sample ADD COLUMN genome_note VARCHAR"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    _run_startup_migrations(engine)
    SQLModel.metadata.create_all(engine)
    yield


app = FastAPI(
    title="AToL Sample Metadata Validator",
    description="Toy validator + pipeline tracker mirroring AToL's ingestion problem.",
    version="0.1.0",
    lifespan=lifespan,
)

# A hand-built test console, served from the same origin as the API so the
# browser's fetch() calls need no CORS setup at all. Doesn't count as one of
# the six required endpoints -- it's a dev convenience, not part of the API
# surface. Visit http://127.0.0.1:8000/static/test.html once the server is
# running.
_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
os.makedirs(_static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# Which paths the admin log middleware below bothers recording. Everything
# under /samples and /status is the actual API surface; /docs, /openapi.json
# and /static are noise an admin panel doesn't need, and /admin/log itself
# is deliberately excluded so reading the log doesn't recursively log itself.
_LOGGED_PREFIXES = ("/samples", "/status", "/simulate")


@app.middleware("http")
async def log_submissions(request: Request, call_next):
    """Records every request/response under _LOGGED_PREFIXES to
    SubmissionLog, for the admin panel. This is the FastAPI equivalent of a
    Laravel HTTP middleware class (LogRequests, say) registered in the
    kernel: it wraps every matching request without any endpoint needing to
    know it exists, which is exactly why it's the right place for
    cross-cutting concerns like audit logging -- unlike validation, which
    belongs on the model, logging belongs on the pipe every request already
    flows through.

    Capturing the response body requires draining and rebuilding it, since
    a streaming Response's body_iterator can only be consumed once."""
    should_log = request.url.path.startswith(_LOGGED_PREFIXES)
    if not should_log:
        return await call_next(request)

    request_body = await request.body()
    started = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - started) * 1000

    response_body = b""
    async for chunk in response.body_iterator:
        response_body += chunk

    with Session(engine) as session:
        session.add(
            SubmissionLog(
                method=request.method,
                path=str(request.url.path)
                + (f"?{request.url.query}" if request.url.query else ""),
                status_code=response.status_code,
                duration_ms=round(duration_ms, 2),
                request_body=request_body.decode("utf-8", errors="replace"),
                response_body=response_body.decode("utf-8", errors="replace"),
            )
        )
        session.commit()

    # The original response's body_iterator is now exhausted, so this
    # request is served from a fresh Response built from the bytes we just
    # captured, rather than the (now-empty) original.
    return Response(
        content=response_body,
        status_code=response.status_code,
        media_type=response.media_type,
    )


def clean_errors(exc: ValidationError) -> list[dict]:
    """Trim Pydantic's default error shape (which includes a docs URL and,
    for custom validators, the raw exception object in `ctx`) down to the
    four fields a researcher actually needs to fix their data: where the
    problem is, what's wrong, what kind of problem it is, and what they
    sent."""
    return [
        {
            "loc": list(e.get("loc", [])),
            "msg": e.get("msg"),
            "type": e.get("type"),
            "input": e.get("input"),
        }
        for e in exc.errors()
    ]


def log_rejection(session: Session, raw_payload, errors: list[dict]) -> None:
    session.add(
        RejectedRecord(
            raw_payload=json.dumps(raw_payload, default=str),
            errors=json.dumps(errors, default=str),
        )
    )
    session.commit()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Every 422 from a single-record endpoint (POST /samples, or a
    malformed batch body) passes through here. This is the FastAPI
    equivalent of a Laravel exception handler: one place that turns a
    thrown validation error into both an HTTP response and a stored audit
    record, instead of every controller doing its own try/except."""
    try:
        body = await request.json()
    except Exception:
        body = None
    errors = clean_errors(exc)
    with Session(engine) as session:
        log_rejection(session, body, errors)
    return JSONResponse(status_code=422, content={"detail": errors})


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.post("/samples", response_model=SampleOut, status_code=201)
def create_sample(sample_in: SampleIn, session: Session = Depends(get_session)):
    existing = session.exec(select(Sample).where(Sample.tolid == sample_in.tolid)).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"tolid '{sample_in.tolid}' already exists")

    sample = Sample(**sample_in.model_dump(), stage=Stage.ingested.value)
    session.add(sample)
    session.commit()
    session.refresh(sample)
    return SampleOut.from_db(sample)


@app.post("/samples/batch", response_model=BatchResponse)
def create_samples_batch(
    payload: list[dict] = Body(...),
    session: Session = Depends(get_session),
):
    """Deliberately does NOT reuse SampleIn as the request body type. If it
    did, FastAPI would reject the whole request with one 422 the moment
    item #3 out of 40 fails, and items 4-40 would never even be looked at.
    Taking `list[dict]` and validating each item manually inside the loop
    is what lets one bad record be reported without blocking the other
    thirty-nine good ones -- the actual point of a batch endpoint."""
    results: list[BatchResult] = []
    accepted = 0

    for idx, raw in enumerate(payload):
        try:
            sample_in = SampleIn.model_validate(raw)
        except ValidationError as exc:
            errors = clean_errors(exc)
            log_rejection(session, raw, errors)
            results.append(BatchResult(index=idx, status="rejected", errors=errors))
            continue

        existing = session.exec(select(Sample).where(Sample.tolid == sample_in.tolid)).first()
        if existing:
            errors = [
                {
                    "loc": ["tolid"],
                    "msg": f"tolid '{sample_in.tolid}' already exists",
                    "type": "value_error.duplicate",
                    "input": sample_in.tolid,
                }
            ]
            log_rejection(session, raw, errors)
            results.append(BatchResult(index=idx, status="rejected", tolid=sample_in.tolid, errors=errors))
            continue

        sample = Sample(**sample_in.model_dump(), stage=Stage.ingested.value)
        session.add(sample)
        session.commit()
        session.refresh(sample)
        accepted += 1
        results.append(BatchResult(index=idx, status="accepted", tolid=sample.tolid, id=sample.id))

    return BatchResponse(
        total=len(payload),
        accepted=accepted,
        rejected=len(payload) - accepted,
        results=results,
    )


@app.get("/samples", response_model=list[SampleOut])
def list_samples(
    stage: Stage | None = Query(default=None, description="Filter to samples at this pipeline stage"),
    session: Session = Depends(get_session),
):
    statement = select(Sample)
    if stage is not None:
        statement = statement.where(Sample.stage == stage.value)
    samples = session.exec(statement).all()
    return [SampleOut.from_db(s) for s in samples]


@app.get("/samples/{sample_id}", response_model=SampleOut)
def get_sample(sample_id: int, session: Session = Depends(get_session)):
    sample = session.get(Sample, sample_id)
    if sample is None:
        raise HTTPException(status_code=404, detail="sample not found")
    return SampleOut.from_db(sample)


@app.patch("/samples/{sample_id}/stage", response_model=SampleOut)
def advance_stage(sample_id: int, body: StageUpdate, session: Session = Depends(get_session)):
    sample = session.get(Sample, sample_id)
    if sample is None:
        raise HTTPException(status_code=404, detail="sample not found")

    current = Stage(sample.stage)
    expected = next_stage(current)
    if expected is None:
        raise HTTPException(
            status_code=400,
            detail=f"sample is already at the final stage '{current.value}'",
        )
    if expected in _ACTION_ONLY_STAGES:
        # A bare state flip isn't enough here -- reaching this stage means
        # something real has to happen first (an ENA accession minted, a
        # Genome Note drafted). PATCH stays a pure state transition; stages
        # with a side effect get their own POST action endpoint instead.
        raise HTTPException(
            status_code=400,
            detail=(
                f"stage '{expected.value}' can't be reached via PATCH; "
                f"use {_ACTION_ONLY_STAGES[expected]} instead"
            ),
        )
    if body.stage != expected:
        raise HTTPException(
            status_code=400,
            detail=(
                f"cannot move from '{current.value}' to '{body.stage.value}'; "
                f"the next valid stage is '{expected.value}'"
            ),
        )

    sample.stage = body.stage.value
    session.add(sample)
    session.commit()
    session.refresh(sample)
    return SampleOut.from_db(sample)


@app.post("/samples/{sample_id}/broker", response_model=SampleOut)
def broker_to_ena(sample_id: int, session: Session = Depends(get_session)):
    """Simulates brokering sample metadata, sequence reads, and the genome
    assembly to the European Nucleotide Archive. The real Genome Engine
    actually submits over ENA's API and gets a real accession back; there's
    no real ENA integration here, so this fabricates one in the same shape
    (a PRJEB-style project accession) instead. Only allowed once a sample
    has reached 'annotated' -- you don't broker an incomplete assembly."""
    sample = session.get(Sample, sample_id)
    if sample is None:
        raise HTTPException(status_code=404, detail="sample not found")

    current = Stage(sample.stage)
    if current != Stage.annotated:
        raise HTTPException(
            status_code=400,
            detail=f"cannot broker from stage '{current.value}'; sample must be 'annotated' first",
        )

    sample.ena_accession = f"PRJEB{random.randint(10000, 99999)}"
    sample.stage = Stage.brokered.value
    session.add(sample)
    session.commit()
    session.refresh(sample)
    return SampleOut.from_db(sample)


@app.post("/samples/{sample_id}/genome-note", response_model=SampleOut)
def generate_genome_note(sample_id: int, session: Session = Depends(get_session)):
    """Simulates the Genome Engine's automatic Genome Note drafting step.
    Real Genome Notes are short structured write-ups covering sampling,
    sequencing and assembly metrics; this templates one from data already
    on the row rather than calling out to anything. Only allowed once a
    sample has been brokered -- the note references the ENA accession."""
    sample = session.get(Sample, sample_id)
    if sample is None:
        raise HTTPException(status_code=404, detail="sample not found")

    current = Stage(sample.stage)
    if current != Stage.brokered:
        raise HTTPException(
            status_code=400,
            detail=f"cannot draft a Genome Note from stage '{current.value}'; sample must be 'brokered' first",
        )

    sample.genome_note = (
        f"# Genome Note: {sample.scientific_name}\n\n"
        f"**ToLID:** {sample.tolid}  \n"
        f"**NCBI taxon ID:** {sample.taxon_id}  \n"
        f"**ENA accession:** {sample.ena_accession}  \n"
        f"**Sequencing platform:** {sample.platform}  \n"
        f"**Collection date:** {sample.collection_date}\n\n"
        f"## Summary\n\n"
        f"A reference genome assembly for *{sample.scientific_name}* was generated from "
        f"{sample.platform} sequence data collected on {sample.collection_date}. Sample "
        f"metadata, sequence reads and the genome assembly have been brokered to the "
        f"European Nucleotide Archive under accession {sample.ena_accession}.\n\n"
        f"## Sampling and sequencing\n\n"
        f"Raw sequence data and associated metadata were ingested from the Bioplatforms "
        f"Australia Data Portal and validated against the AToL sample metadata schema "
        f"prior to assembly.\n\n"
        f"*This Genome Note was generated automatically by a simulated Genome Engine "
        f"pipeline (toy project, not a real submission).*\n"
    )
    sample.stage = Stage.note_generated.value
    session.add(sample)
    session.commit()
    session.refresh(sample)
    return SampleOut.from_db(sample)


@app.get("/status", response_model=StatusResponse)
def get_status(session: Session = Depends(get_session)):
    samples = session.exec(select(Sample)).all()
    by_stage = {s.value: 0 for s in STAGE_ORDER}
    for sample in samples:
        by_stage[sample.stage] = by_stage.get(sample.stage, 0) + 1

    blocked = session.exec(select(RejectedRecord)).all()

    return StatusResponse(
        by_stage=by_stage,
        total_samples=len(samples),
        blocked_on_validation=len(blocked),
    )


@app.get("/admin/log", response_model=list[SubmissionLogOut])
def get_submission_log(
    limit: int = Query(default=100, le=500, description="Most recent entries first"),
    session: Session = Depends(get_session),
):
    """Backs the admin panel at /static/admin.html. Not one of the six
    required endpoints -- a dev/ops convenience for seeing exactly what
    was submitted and exactly what the API sent back, populated by the
    log_submissions middleware above rather than by any endpoint here."""
    statement = select(SubmissionLog).order_by(SubmissionLog.id.desc()).limit(limit)
    return session.exec(statement).all()


@app.get("/simulate/bioplatforms-ingest")
def mock_bioplatforms_ingest():
    """Fabricates a batch of raw records shaped like what the real
    Bioplatforms Australia Data Portal API actually hands the Genome
    Engine at the very first pipeline step -- some clean, one carrying
    portal-specific fields our schema doesn't recognise, one using the
    portal's own dataset ID instead of a ToLID, one embargoed, one a
    duplicate of an earlier package (pagination overlap happens).

    None of this touches the database -- it's meant to be pasted, or
    loaded with one click from the test console, into POST /samples/batch,
    so the exact same messiness the real ingest step has to sort through
    gets sorted by the same validation boundary the rest of this app
    demonstrates. Not one of the six required endpoints."""
    return [
        {
            "tolid": "maNotRufo1",
            "scientific_name": "Notamacropus rufogriseus",
            "taxon_id": 9317,
            "collection_date": "2026-03-02",
            "platform": "pacbio_hifi",
        },
        {
            # A real portal payload before it's been mapped onto our schema --
            # extra="forbid" on SampleIn rejects this loudly rather than
            # silently dropping the fields it doesn't recognise.
            "tolid": "maNotRufo2",
            "scientific_name": "Notamacropus rufogriseus",
            "taxon_id": 9317,
            "collection_date": "2026-03-04",
            "platform": "hic",
            "bpa_package_id": "102.100.100/12345",
            "bpa_dataset_url": "https://data.bioplatforms.com/dataset/12345",
        },
        {
            # The portal's own dataset identifier, not a ToLID -- exactly the
            # kind of upstream-ID mismatch a real ingest mapping step has to
            # catch before this ever reaches assembly.
            "tolid": "BPA-WALLABY-004",
            "scientific_name": "Notamacropus rufogriseus",
            "taxon_id": 9317,
            "collection_date": "2026-03-02",
            "platform": "pacbio_hifi",
        },
        {
            "tolid": "plXanPrei1",
            "scientific_name": "Xanthorrhoea preissii",
            "taxon_id": 91223,
            "collection_date": "2026-01-20",
            "embargo_until": "2026-12-01",
            "platform": "hic",
        },
        {
            "tolid": "maPsePere1",
            "scientific_name": "Pseudocheirus peregrinus",
            "taxon_id": 34899,
            "collection_date": "2026-04-11",
            "platform": "ont",
        },
        {
            # Same package as the first record, sent again -- portal listing
            # pagination overlaps in the real world, and POST /samples/batch
            # already handles this the same way it handles any duplicate.
            "tolid": "maNotRufo1",
            "scientific_name": "Notamacropus rufogriseus",
            "taxon_id": 9317,
            "collection_date": "2026-03-02",
            "platform": "pacbio_hifi",
        },
    ]
