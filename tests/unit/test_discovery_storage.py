"""Unit tests for discovery persistence backends."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
from pydantic import HttpUrl

from denbust.config import Config
from denbust.data_models import RawArticle
from denbust.discovery.models import (
    BackfillBatch,
    CandidateProvenance,
    CandidateStatus,
    DiscoveryRun,
    FetchStatus,
    PersistentCandidate,
    ProducerKind,
    ScrapeAttempt,
    ScrapeAttemptKind,
)
from denbust.discovery.source_native import (
    SourceDiscoveryAdapter,
    persist_discovered_candidates,
    raw_article_to_discovered_candidate,
)
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import (
    CompositeDiscoveryPersistence,
    DiscoveryPersistence,
    NullDiscoveryPersistence,
    StateRepoDiscoveryPersistence,
    SupabaseDiscoveryPersistence,
    create_discovery_persistence,
)
from denbust.models.common import DatasetName


def build_raw_article(
    url: str = "https://www.ynet.co.il/news/article/abc?utm_source=test",
    *,
    source_name: str = "ynet",
    title: str = "פשיטה על בית בושת",
    snippet: str = "המשטרה ביצעה פשיטה.",
    published_at: datetime | None = None,
) -> RawArticle:
    """Build a raw article fixture."""
    return RawArticle(
        url=HttpUrl(url),
        title=title,
        snippet=snippet,
        date=published_at or datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
        source_name=source_name,
    )


def build_candidate(
    candidate_id: str = "candidate-1",
    *,
    status: CandidateStatus = CandidateStatus.NEW,
    canonical_url: str = "https://www.ynet.co.il/news/article/abc",
    current_url: str = "https://www.ynet.co.il/news/article/abc?utm_source=test",
    backfill_batch_id: str | None = None,
) -> PersistentCandidate:
    """Build a persistent candidate fixture."""
    return PersistentCandidate(
        candidate_id=candidate_id,
        canonical_url=HttpUrl(canonical_url),
        current_url=HttpUrl(current_url),
        titles=["title"],
        snippets=["snippet"],
        discovered_via=["source_native"],
        discovery_queries=["בית בושת"],
        source_hints=["ynet"],
        first_seen_at=datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
        last_seen_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
        candidate_status=status,
        backfill_batch_id=backfill_batch_id,
    )


def build_provenance(candidate_id: str = "candidate-1") -> CandidateProvenance:
    """Build a provenance event fixture."""
    return CandidateProvenance(
        run_id="run-1",
        candidate_id=candidate_id,
        producer_name="ynet",
        producer_kind=ProducerKind.SOURCE_NATIVE,
        raw_url=HttpUrl("https://www.ynet.co.il/news/article/abc"),
        normalized_url=HttpUrl("https://www.ynet.co.il/news/article/abc"),
        title="title",
        snippet="snippet",
        discovered_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
    )


def build_attempt(candidate_id: str = "candidate-1") -> ScrapeAttempt:
    """Build a scrape-attempt fixture."""
    return ScrapeAttempt(
        candidate_id=candidate_id,
        started_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
        finished_at=datetime(2026, 4, 11, 9, 5, tzinfo=UTC),
        attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
        fetch_status=FetchStatus.SUCCESS,
        diagnostics={"status": "ok"},
    )


def build_backfill_batch(batch_id: str = "batch-1") -> BackfillBatch:
    """Build a backfill-batch fixture."""
    return BackfillBatch(
        batch_id=batch_id,
        requested_date_from=datetime(2026, 1, 1, tzinfo=UTC),
        requested_date_to=datetime(2026, 1, 7, tzinfo=UTC),
    )


class FakeSource:
    """Simple source stub for the source-native adapter."""

    def __init__(self, name: str, articles: list[RawArticle]) -> None:
        self.name = name
        self._articles = articles
        self.calls: list[tuple[int, list[str]]] = []

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        self.calls.append((days, keywords))
        return self._articles


class RecordingPersistence(NullDiscoveryPersistence):
    """Persistence stub that records write calls."""

    def __init__(self) -> None:
        self.find_calls = 0
        self.written_run: DiscoveryRun | None = None
        self.candidates: list[PersistentCandidate] = []
        self.provenance: list[CandidateProvenance] = []

    def find_candidate_by_urls(
        self, *, canonical_url: str | None, current_url: str
    ) -> PersistentCandidate | None:
        del canonical_url, current_url
        self.find_calls += 1
        return None

    def write_run(self, run: DiscoveryRun) -> None:
        self.written_run = run

    def upsert_candidates(self, candidates: list[PersistentCandidate]) -> None:
        self.candidates = list(candidates)

    def append_provenance(self, events: list[CandidateProvenance]) -> None:
        self.provenance = list(events)


def test_source_discovery_adapter_name_and_discovered_at_override() -> None:
    """Source-native adapters should expose the source name and preserve context timestamps."""

    async def run_test() -> None:
        article = build_raw_article()
        source = FakeSource("ynet", [article])
        adapter = SourceDiscoveryAdapter(source)
        discovered_at = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)

        from denbust.discovery.base import SourceDiscoveryContext

        discovered = await adapter.discover_candidates(
            SourceDiscoveryContext(
                run_id="run-1",
                source_names=["ynet"],
                days=3,
                keywords=["בית בושת"],
                metadata={"discovered_at": discovered_at},
            )
        )

        assert adapter.name == "ynet"
        assert source.calls == [(3, ["בית בושת"])]
        assert discovered[0].discovered_at == discovered_at

    import asyncio

    asyncio.run(run_test())


def test_source_discovery_adapter_requires_days() -> None:
    """Source-native discovery should reject missing day windows."""

    async def run_test() -> None:
        from denbust.discovery.base import SourceDiscoveryContext

        adapter = SourceDiscoveryAdapter(FakeSource("ynet", []))
        try:
            await adapter.discover_candidates(
                SourceDiscoveryContext(
                    run_id="run-1",
                    source_names=["ynet"],
                    days=None,
                    keywords=["בית בושת"],
                )
            )
        except ValueError as exc:
            assert "SourceDiscoveryContext.days is required" in str(exc)
        else:
            raise AssertionError("expected ValueError")

    import asyncio

    asyncio.run(run_test())


def test_persist_discovered_candidates_reuses_identity_map_and_sets_failed_status() -> None:
    """In-memory dedupe within one persistence pass should avoid repeated lookups."""
    first = raw_article_to_discovered_candidate(build_raw_article(title="title-1"))
    second = raw_article_to_discovered_candidate(
        build_raw_article(
            "https://ynet.co.il/news/article/abc?Partner=searchResults",
            title="title-2",
        )
    )
    persistence = RecordingPersistence()
    run = DiscoveryRun(run_id="run-1", errors=["ynet: boom"])

    persisted = persist_discovered_candidates(
        run=run,
        discovered_candidates=[first, second],
        persistence=persistence,
    )

    assert persistence.find_calls == 1
    assert len(persisted.candidates) == 1
    assert persisted.run.status.value == "partial"

    failed = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-2", errors=["ynet: boom"]),
        discovered_candidates=[],
        persistence=RecordingPersistence(),
    )
    assert failed.run.status.value == "failed"


def test_null_discovery_persistence_is_noop() -> None:
    """The null persistence backend should be safely inert."""
    store = NullDiscoveryPersistence()
    assert DiscoveryPersistence.close(store) is None

    store.write_run(DiscoveryRun(run_id="run-1"))
    store.upsert_candidates([build_candidate()])
    store.append_provenance([build_provenance()])
    store.append_attempts([build_attempt()])

    assert store.get_candidate("missing") is None
    assert store.list_candidates() == []
    assert (
        store.find_candidate_by_urls(canonical_url=None, current_url="https://example.com") is None
    )
    assert store.list_provenance("missing") == []
    assert store.list_attempts("missing") == []
    assert store.close() is None


def test_state_repo_discovery_persistence_round_trips(tmp_path: Path) -> None:
    """The state-repo persistence backend should read and write candidates, provenance, and attempts."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    store = StateRepoDiscoveryPersistence(paths)
    queued = build_candidate("queued", status=CandidateStatus.QUEUED)
    backfill = build_candidate(
        "backfill",
        status=CandidateStatus.SCRAPE_FAILED,
        current_url="https://www.walla.co.il/item",
        canonical_url="https://www.walla.co.il/item",
        backfill_batch_id="batch-1",
    )

    store.write_run(
        DiscoveryRun(run_id="run-1", started_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC))
    )
    store.upsert_candidates([queued, backfill])
    store.upsert_backfill_batches([build_backfill_batch()])
    store.append_provenance([build_provenance("queued"), build_provenance("backfill")])
    store.append_attempts([build_attempt("queued"), build_attempt("backfill")])

    assert store.provenance_path == paths.candidates_dir / "candidate_provenance.jsonl"
    assert store.attempts_path == paths.candidates_dir / "scrape_attempts.jsonl"
    assert store.get_candidate("queued") is not None
    assert store.get_candidate("missing") is None
    assert len(store.list_candidates()) == 2
    assert store.get_backfill_batch("batch-1") is not None
    assert len(store.list_backfill_batches(limit=1)) == 1
    assert (
        store.list_candidates(statuses=[CandidateStatus.QUEUED], limit=1)[0].candidate_id
        == "queued"
    )
    assert (
        store.find_candidate_by_urls(
            canonical_url="https://www.walla.co.il/item",
            current_url="https://ignored.example.com",
        )
        is not None
    )
    assert (
        store.find_candidate_by_urls(
            canonical_url=None,
            current_url="https://www.walla.co.il/item",
        )
        is not None
    )
    assert len(store.list_provenance("queued", limit=1)) == 1
    assert len(store.list_attempts("backfill", limit=1)) == 1
    assert len(store.list_attempts("backfill")) == 1
    assert paths.retry_queue_path.exists()
    assert paths.backfill_queue_path.exists()
    assert paths.latest_backfill_batches_path.exists()
    assert store.close() is None


def test_state_repo_discovery_persistence_handles_missing_and_blank_jsonl(tmp_path: Path) -> None:
    """JSONL readers should tolerate missing files and blank lines."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    store = StateRepoDiscoveryPersistence(paths)

    assert store.list_candidates() == []

    paths.latest_candidates_path.parent.mkdir(parents=True, exist_ok=True)
    paths.latest_candidates_path.write_text("\n\n", encoding="utf-8")
    store.append_provenance([])
    store.append_attempts([])
    store.upsert_candidates([])

    assert store.list_candidates() == []


class FakeHttpClient:
    """Record HTTP requests and return queued responses."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []
        self.closed = False

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str] | None = None,
        json: object | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "json": json,
                "headers": headers,
            }
        )
        response = self.responses.pop(0)
        response.request = httpx.Request(method, url)
        return response

    def close(self) -> None:
        self.closed = True


def test_supabase_discovery_persistence_crud_and_headers() -> None:
    """The Supabase backend should translate persistence calls into PostgREST requests."""
    client = FakeHttpClient(
        [
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[build_candidate().model_dump(mode="json")]),
            httpx.Response(200, json=[build_candidate().model_dump(mode="json")]),
            httpx.Response(200, json=[build_candidate().model_dump(mode="json")]),
            httpx.Response(200, json=[build_backfill_batch().model_dump(mode="json")]),
            httpx.Response(200, json=[build_candidate().model_dump(mode="json")]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[build_provenance().model_dump(mode="json")]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[build_attempt().model_dump(mode="json")]),
        ]
    )
    store = SupabaseDiscoveryPersistence(
        base_url="https://supabase.example.com/",
        service_role_key="service-role",
        schema="custom",
        table_names={
            "discovery_runs": "discovery_runs",
            "persistent_candidates": "persistent_candidates",
            "backfill_batches": "backfill_batches",
            "candidate_provenance": "candidate_provenance",
            "scrape_attempts": "scrape_attempts",
        },
        client=client,
    )

    store.write_run(DiscoveryRun(run_id="run-1", errors=["boom"]))
    store.upsert_candidates([build_candidate()])
    store.upsert_backfill_batches([build_backfill_batch()])
    assert store.get_candidate("candidate-1") is not None
    assert len(store.list_candidates(statuses=[CandidateStatus.NEW], limit=2)) == 1
    assert store.get_backfill_batch("batch-1") is not None
    assert (
        store.find_candidate_by_urls(
            canonical_url="https://www.ynet.co.il/news/article/abc",
            current_url="https://ignored.example.com",
        )
        is not None
    )
    assert (
        store.find_candidate_by_urls(
            canonical_url=None,
            current_url="https://missing.example.com",
        )
        is None
    )
    store.append_provenance([build_provenance()])
    assert len(store.list_provenance("candidate-1", limit=1)) == 1
    store.append_attempts([build_attempt()])
    assert len(store.list_attempts("candidate-1", limit=1)) == 1
    store.close()

    assert client.closed is True
    assert client.calls[0]["url"] == "https://supabase.example.com/rest/v1/discovery_runs"
    assert client.calls[0]["json"]["errors"] == ["boom"]  # type: ignore[index]
    headers = client.calls[0]["headers"]
    assert isinstance(headers, dict)
    assert headers["Accept-Profile"] == "custom"
    assert headers["Authorization"] == "Bearer service-role"


def test_supabase_discovery_persistence_handles_empty_payload_shapes() -> None:
    """Non-list responses should degrade to empty reads rather than crashing."""
    client = FakeHttpClient(
        [
            httpx.Response(200, json={"unexpected": True}),
            httpx.Response(200, json={"unexpected": True}),
            httpx.Response(200, json={"unexpected": True}),
            httpx.Response(200, json={"unexpected": True}),
        ]
    )
    store = SupabaseDiscoveryPersistence(
        base_url="https://supabase.example.com",
        service_role_key="service-role",
        schema="public",
        table_names={
            "discovery_runs": "discovery_runs",
            "persistent_candidates": "persistent_candidates",
            "backfill_batches": "backfill_batches",
            "candidate_provenance": "candidate_provenance",
            "scrape_attempts": "scrape_attempts",
        },
        client=client,
    )

    assert store.get_candidate("missing") is None
    assert store.list_candidates() == []
    assert store.list_provenance("missing") == []
    assert store.list_attempts("missing") == []
    store.upsert_candidates([])
    store.append_provenance([])
    store.append_attempts([])


def test_supabase_discovery_persistence_finds_candidate_by_current_url() -> None:
    """Current-url lookups should return a candidate when canonical_url is absent."""
    client = FakeHttpClient([httpx.Response(200, json=[build_candidate().model_dump(mode="json")])])
    store = SupabaseDiscoveryPersistence(
        base_url="https://supabase.example.com",
        service_role_key="service-role",
        schema="public",
        table_names={
            "discovery_runs": "discovery_runs",
            "persistent_candidates": "persistent_candidates",
            "backfill_batches": "backfill_batches",
            "candidate_provenance": "candidate_provenance",
            "scrape_attempts": "scrape_attempts",
        },
        client=client,
    )

    found = store.find_candidate_by_urls(
        canonical_url=None,
        current_url="https://www.ynet.co.il/news/article/abc?utm_source=test",
    )

    assert found is not None
    assert found.candidate_id == "candidate-1"


def test_composite_discovery_persistence_fans_out_writes_and_reads_from_primary(
    tmp_path: Path,
) -> None:
    """Composite persistence should mirror writes while delegating reads to the primary."""
    primary = StateRepoDiscoveryPersistence(
        resolve_discovery_state_paths(
            state_root=tmp_path / "primary-discovery-test",
            dataset_name=DatasetName.NEWS_ITEMS,
        )
    )
    mirror = RecordingPersistence()
    candidate = build_candidate()
    provenance = build_provenance()
    attempt = build_attempt()
    batch = build_backfill_batch()
    composite = CompositeDiscoveryPersistence(primary, [mirror])

    composite.write_run(DiscoveryRun(run_id="run-1"))
    composite.upsert_candidates([candidate])
    composite.upsert_backfill_batches([batch])
    composite.append_provenance([provenance])
    composite.append_attempts([attempt])

    assert composite.get_candidate(candidate.candidate_id) is not None
    assert composite.get_backfill_batch(batch.batch_id) is not None
    assert composite.list_candidates(limit=1)[0].candidate_id == candidate.candidate_id
    assert (
        composite.find_candidate_by_urls(
            canonical_url="https://www.ynet.co.il/news/article/abc",
            current_url="https://unused.example.com",
        )
        is not None
    )
    assert (
        composite.list_provenance(candidate.candidate_id, limit=1)[0].candidate_id
        == candidate.candidate_id
    )
    assert (
        composite.list_attempts(candidate.candidate_id, limit=1)[0].candidate_id
        == candidate.candidate_id
    )
    assert mirror.written_run is not None
    assert mirror.candidates[0].candidate_id == candidate.candidate_id
    assert mirror.provenance[0].candidate_id == candidate.candidate_id
    composite.close()


def test_create_discovery_persistence_selects_state_or_composite(
    monkeypatch, tmp_path: Path
) -> None:
    """The discovery persistence factory should return state-only or composite backends."""
    config = Config(store={"state_root": tmp_path / "discovery-factory-state"})
    state_only = create_discovery_persistence(config)
    assert isinstance(state_only, StateRepoDiscoveryPersistence)

    monkeypatch.setenv("DENBUST_SUPABASE_URL", "https://supabase.example.com")
    monkeypatch.setenv("DENBUST_SUPABASE_SERVICE_ROLE_KEY", "service-role")
    composite = create_discovery_persistence(
        Config(
            operational={"provider": "supabase"},
            store={"state_root": tmp_path / "discovery-factory-composite"},
        )
    )
    assert isinstance(composite, CompositeDiscoveryPersistence)
    assert isinstance(composite.primary, StateRepoDiscoveryPersistence)
    assert isinstance(composite.mirrors[0], SupabaseDiscoveryPersistence)
    composite.close()
