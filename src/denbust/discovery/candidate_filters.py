"""Candidate-level filtering policy for discovery search results."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlparse

from denbust.discovery.models import DiscoveredCandidate, ProducerKind

_UTILITY_SEARCH_DOMAINS: frozenset[str] = frozenset(
    {
        "morfix.co.il",
        "context.reverso.net",
        "dictionary.reverso.net",
        "wiktionary.org",
        "pealim.com",
    }
)

_APP_STORE_DOMAINS: frozenset[str] = frozenset(
    {"play.google.com", "apps.apple.com", "itunes.apple.com"}
)

_SOCIAL_PROFILE_DOMAINS: frozenset[str] = frozenset(
    {
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "tiktok.com",
        "twitter.com",
        "x.com",
        "youtube.com",
    }
)

# Domains that consistently produce off-topic candidates and waste classifier
# API budget.  All subdomains are matched (e.g. sport1.maariv.co.il matches
# "sport1.maariv.co.il").  Add new entries here when a domain is confirmed
# irrelevant via review-workbench bulk-exclusion or diagnostics evidence.
_IRRELEVANT_CONTENT_DOMAINS: frozenset[str] = frozenset(
    {
        "sport1.maariv.co.il",  # sports vertical — consistently off-topic
        "he.wikipedia.org",  # encyclopedia — never enforcement news
        "srugim.co.il",  # Orthodox community site — off-topic
        "themarker.com",  # financial/business news — off-topic
        "collab.mako.co.il",  # Mako user-generated content subdomain
        "kikar.co.il",  # ultra-Orthodox news — off-topic
        "atzat-nefesh.org",  # mental health org — off-topic
        "il.bongogirls.ru",  # noise
        "he.wikiquote.org",  # Hebrew Wikiquote — never enforcement news
        "parks.org.il",  # Israel Nature and Parks Authority — off-topic
        "vietnam.vn",  # Vietnamese government portal — off-topic
        "dok.co.il",  # Israeli job listings site — off-topic
        "ealaw.co.il",  # Israeli law reference site — off-topic
        "calcalist.co.il",  # financial/business newspaper — off-topic
        "globes.co.il",  # Globes financial newspaper — off-topic
        "nevo.co.il",  # Israeli legal database — off-topic
        "lawbuzz.co.il",  # Israeli law reference — off-topic
        "bizportal.co.il",  # business/finance portal — off-topic
        "kolzchut.org.il",  # social-rights info site — off-topic
        "itim.org.il",  # Jewish civil status NGO — off-topic
        "din.org.il",  # legal info NGO — off-topic
        "todojustice.co.il",  # legal info site — off-topic
        "tomoko.co.il",  # unrelated e-commerce — off-topic
        "polisa.news",  # insurance news site — off-topic
        # ── Stage B2 sweep 2026-06: escort / massage / webcam sex-service spam ──
        "healworlds.blogspot.com",  # "women for sale" spam blog
        "business-ladies.co.il",  # escort listing
        "serviciosfiat.com",  # massage-ad spam (hijacked domain)
        "getsdiscreet.com",  # discreet-massage/escort ad
        "getsmassage.com",  # massage ad
        "doska777.co.il",  # escort listing board
        "sharapplus.co.il",  # private-clinic SEO — not news
        "il.blablacams.com",  # webcam models
        "escortinisrael.co.il",  # escort listing
        "swipecherry.com",  # escort listing
        "il.rulet-18.com",  # sex-cam roulette
        "il.chat-rulet-18.com",  # sex-cam roulette
        "il.chatrulet-18.com",  # sex-cam roulette
        "il.didichat.ru",  # sex-cam
        "il.nextchat.ru",  # sex-cam
        "il.russianroulette.su",  # live sex-cam
        "xmassage.co.il",  # massage ad
        "alternabe.co.il",  # massage ad
        "boobpedia.com",  # adult-content wiki / escort agency posts
        "bigboyztoyz69.com",  # escort listing
        "shop69.co.il",  # sex-toy shop
        "opulentcharm.co.il",  # OnlyFans agency SEO
        "spaplus.co.il",  # spa/massage packages
        "skdance.co.il",  # cabaret dancers for hire — not news
        "healthwise.co.il",  # sex/health SEO content
        # ── Stage B2 sweep 2026-06: off-topic commerce / real-estate / SEO ──
        "weconnex.co.il",  # office-rental listings
        "gal-gefen.co.il",  # local PR/advertising site
        "b144.co.il",  # business directory
        "rea-me.co.il",  # real-estate listings
        "astlv.co.il",  # real-estate listings
        "okyanus.co.il",  # office-rental listings
        "supply-chain1.co.il",  # logistics directory
        "rent.org.il",  # equipment-rental listings
        "nmrk.co.il",  # commercial real-estate
        "carwiz.co.il",  # car sales
        "zapcars.co.il",  # car sales
        "litrom.org.il",  # presentation-design SEO
        "pc.co.il",  # networking/branding SEO
        "mivzaklive.co.il",  # fraud-awareness SEO content
        "shvirega.co.il",  # therapist SEO
        # ── Stage B2 sweep 2026-06: legal-advice SEO / lawyer directories ──
        "findlaw.co.il",  # legal Q&A SEO
        "dobin-law.com",  # immigration-lawyer SEO
        "din.co.il",  # lawyer directory
        "ese.co.il",  # fraud-lawyer SEO
        # ── Stage B2 sweep 2026-06: religious admin / Q&A ──
        "rabanut.co.il",  # religious-council admin pages
        "hidabroot.org",  # religious Q&A
        "be7.co.il",  # B'Sheva religious opinion
        "bshch.blogspot.com",  # haredi community blog
        # ── Stage B2 sweep 2026-06: reference / foreign advocacy / off-topic ──
        "he.wikisource.org",  # source-text wiki
        "en.wikipedia.org",  # encyclopedia
        "commons.wikimedia.org",  # media repository
        "timesofmalta.com",  # foreign outlet — not Israeli enforcement
        "swopbehindbars.org",  # US sex-worker advocacy
        "abolition2014.blogspot.com",  # foreign anti-prostitution blog
        "one.co.il",  # sports outlet — off-topic
        "tlvtimes.co.il",  # local lifestyle/PR magazine — off-topic
    }
)

_SOCIAL_PROFILE_PATH_PREFIXES: dict[str, tuple[str, ...]] = {
    "linkedin.com": ("/company/", "/in/", "/school/", "/showcase/"),
    "youtube.com": ("/@", "/channel/", "/c/", "/user/"),
}

_SOCIAL_POST_PATH_PREFIXES: dict[str, tuple[str, ...]] = {
    "facebook.com": ("/permalink.php", "/posts/", "/share/", "/story.php", "/watch/"),
    "instagram.com": ("/p/", "/reel/", "/tv/"),
    "linkedin.com": ("/feed/update/", "/posts/", "/pulse/"),
    "youtube.com": ("/shorts/", "/watch"),
}


# Matches any Hebrew letter (Unicode block U+05D0–U+05EA).
# Used to gate-keep candidates whose title and snippet contain no Hebrew at all.
_HEBREW_RE: re.Pattern[str] = re.compile(r"[א-ת]")

_EXCLUDED_TITLE_TERMS: frozenset[str] = frozenset(
    {
        # ── military / geopolitical ──────────────────────────────────────────
        'צה"ל',  # IDF — ASCII double-quote form
        "צה״ל",  # IDF — Hebrew gershayim form
        "צהל",  # IDF — no punctuation form
        "איראן",  # Iran
        "חיזבאללה",  # Hezbollah
        "עזה",  # Gaza
        "קטאר",  # Qatar — geopolitical/sports noise
        "ונצואלה",  # Venezuela
        "מדורו",  # Maduro — Venezuelan politics
        "ממדאני",  # Madani — UN SG noise
        # ── politics ─────────────────────────────────────────────────────────
        "נתניהו",  # Netanyahu
        "טראמפ",  # Trump
        "בלפור",  # Balfour St protests — covers בלפור and בבלפור
        "פוליטי",  # generic political commentary
        # ── finance / business ───────────────────────────────────────────────
        "מניות",  # stocks / financial markets
        "שוק ההון",  # capital markets
        "גלובס",  # Globes financial news brand
        "themarker",  # TheMarker financial news brand (case-insensitive match)
        # ── supermarkets / retail noise ──────────────────────────────────────
        "שופרסל",  # Shufersal supermarket chain
        "ויקטורי",  # Victory supermarket chain
        "רמי לוי",  # Rami Levy supermarket chain
        "ksp",  # KSP electronics chain (case-insensitive)
        # ── sports ───────────────────────────────────────────────────────────
        "ספורט",  # sports
        "מכבי",  # Maccabi sports teams (Maccabi Haifa, Tel Aviv, etc.)
        # ── media brand names appearing as title suffixes ────────────────────
        "ויקיפדיה",  # Wikipedia entries
        "סרוגים",  # Srugim site brand name
        "כיכר השבת",  # Kikar HaShabbat ultra-Orthodox site brand
        "וואלה חדשות",  # Walla News brand in title (topic/nav pages)
        # ── celebrity / entertainment ────────────────────────────────────────
        "אייל גולן",  # Israeli singer — consistently off-topic
        # ── food / hospitality ───────────────────────────────────────────────
        "בית קפה",  # coffee shop — covers "בית קפה", "לבית קפה"
        "בית הקפה",  # the coffee shop — covers "בית הקפה", "לבית הקפה"
        "מסעדה",  # restaurant — covers "המסעדה", "למסעדה"
        "אולם אירועים",  # event hall
        "אולם האירועים",  # the event hall
        "מתחם אירועים",  # event complex
        "מתחם האירועים",  # the event complex
        "האירועים",  # events (residual event-venue noise)
        "המבורגר",  # hamburger — covers "המבורגרים", "ההמבורגרים"
        "ארומה",  # Aroma coffee chain
        "עסק מזון",  # food business
        "עסקי מזון",  # food businesses (plural construct)
        # ── construction / real-estate noise ─────────────────────────────────
        "אתר בנייה",  # construction site
        "לאתר בנייה",  # to a construction site (dative)
        "אתר הבנייה",  # the construction site
        "דירה למכירה",  # apartment for sale
        "דירות למכירה",  # apartments for sale
        "משופצת",  # renovated (real-estate listing noise)
        # ── publication-ban / non-enforcement closure orders ──────────────────
        "איסור פרסום",  # publication gag order (non-enforcement context)
        "איסור הפרסום",  # the publication gag order
        "איסור פירסום",  # alt spelling
        "איסור הפירסום",  # alt spelling with def. article
        "צו פתיחת הליכים",  # bankruptcy opening-of-proceedings order
        "צו ביניים",  # interim injunction (non-prostitution court context)
        "צו ראשון",  # first order (non-enforcement court context)
        "צו סגירה למפעל",  # factory closure order
        "צו סגירה לסניף",  # branch closure order
        "צו סגירה למאפיה",  # bakery closure order (מאפיה = bakery in Hebrew)
        "צו סגירה לעסק",  # business closure order (generic)
        "צו סגירה ניתן לעסק",  # business closure order issued to business
        "צו סגירה הוצא לעסק",  # business closure order issued to business (alt form)
        # ── retail / brand noise ─────────────────────────────────────────────
        "המשביר",  # HaMashbir department-store chain
        "איקאה",  # IKEA
        # ── finance / trading ────────────────────────────────────────────────
        "מסחר בבורסה",  # stock-market trading
        "מסחר עצמאי",  # independent trading / freelance commerce
        # ── AI / tech noise ──────────────────────────────────────────────────
        "גרוק",  # Grok AI (Hebrew)
        "grok",  # Grok AI (English, case-insensitive)
        "ג'מיני",  # Gemini AI (Hebrew)
        "gemini",  # Gemini AI (English, case-insensitive)
        "chatgpt",  # ChatGPT (case-insensitive)
        "claude",  # Claude AI (case-insensitive)
        "מודל שפה",  # language model
        # ── unrelated legal / civic ───────────────────────────────────────────
        "בגצ",  # HCJ (no punctuation)
        'בג"צ',  # HCJ (ASCII double-quote)
        "בגץ",  # HCJ (final tsadi form, no punctuation)
        'בג"ץ',  # HCJ (final tsadi + ASCII double-quote)
        "יציאה מהארץ",  # leaving the country (travel/immigration noise)
        "גיוס",  # military/job recruitment noise
        # ── industrial / factory noise ────────────────────────────────────────
        "מפעל",  # factory
        "מאפיה",  # bakery (Hebrew; "mafia" is a false cognate)
        "מוסך",  # garage / auto-repair shop
        # ── adult-content / unrelated noise ──────────────────────────────────
        "פורנהאב",  # Pornhub (site name, not enforcement news)
        "קלפים",  # playing/tarot cards
        "טארוט",  # tarot
        "מצלמות",  # cameras (cam-site noise)
        "cams",  # cam-site noise (case-insensitive)
        "מעשים מגונים",  # indecent acts (non-trafficking legal noise)
        # ── politics / government (second batch) ─────────────────────────────
        "ביבי",  # Netanyahu nickname
        "בנט",  # Naftali Bennett — political noise
        "בן גביר",  # Itamar Ben Gvir
        "בן-גביר",  # Ben Gvir with hyphen
        # ── economy / tax / budget noise ─────────────────────────────────────
        "כלכלה",  # economy
        "רשות המסים",  # Tax Authority
        "מסים",  # taxes — covers "המסים" substring too
        "המס",  # the tax (shorter form; avoids over-blocking "מסים" in other contexts)
        "תקציב",  # budget — covers "התקציב"
        # "קנס" (fine) was here but removed — 98% of matched articles were enforcement-relevant
        # (prostitution-law fines, client-penalisation statistics). Replaced with specific
        # non-enforcement bigrams below.
        "קנס חנייה",  # parking fine
        "קנס תנועה",  # traffic fine
        "קנס רכב",  # vehicle fine
        "קנס מנהלי",  # administrative fine (generic regulatory noise)
        "נקנס המועדון",  # sports club was fined
        "נקנסה חברת",  # [company name] company was fined (business noise)
        "קנס פיפא",  # FIFA fine
        'קנס אופ"א',  # UEFA fine
        "עסקה",  # deal/transaction
        "עסקת",  # deal-of (construct form)
        "זכיינות",  # franchising
        # ── fuel / telecom brand noise ────────────────────────────────────────
        "פז",  # Paz fuel chain
        "דלק",  # Delek fuel company
        "סונול",  # Sonol fuel chain
        "בזק",  # Bezeq telecom
        "הוט",  # HOT cable/telecom
        "קרפור",  # Carrefour supermarket
        "שטראוס",  # Strauss food company
        "אבו לטיף",  # Abu Latif — recurring off-topic news subject
        # ── divorce / family-law noise ────────────────────────────────────────
        "גירושין",  # divorce (legal term) — covers "גירושים" substring
        "להתגרש",  # to divorce
        # ── sexual harassment (distinct from trafficking) ─────────────────────
        "הטרדה מינית",  # sexual harassment — covers "להטרדה מינית", "הוטרדה מינית"
        # ── AI / tech (second batch) ─────────────────────────────────────────
        "בינה מלאכותית",  # artificial intelligence
        # ── religious content noise ──────────────────────────────────────────
        "תורה",  # Torah — religious content, off-topic
        # ── generic reward / prize phrases ────────────────────────────────────
        "מגיע לך",  # "you deserve it" — prize/reward noise
        "מגיע לכם",  # "you all deserve it" — prize/reward noise
        # ── civic / protest noise ────────────────────────────────────────────
        "מפגינים",  # protesters
        # ── license revocation (driving/professional, non-enforcement) ────────
        "שלילת רישיון",  # license revocation — covers "ושלילת רישיון"
        # ── content offensiveness flags (non-enforcement context) ─────────────
        "פוגעני",  # offensive/harmful — covers "פוגעניים"
        "סם אונס",  # rape drug (news about the drug itself, not trafficking)
        # ── insolvency / social-security / civil-defense noise ────────────────
        "חדלות פירעון",  # insolvency — legal/financial, not enforcement
        "ביטוח לאומי",  # National Insurance Institute — social-security noise
        "פיקוד העורף",  # Home Front Command — civil-defense / military noise
        # ── named individuals (off-topic recurring subjects) ──────────────────
        "אסף דוק",  # Assaf Dok — consistently off-topic news subject
    }
)


def globally_excluded_title_terms() -> frozenset[str]:
    """Return the current set of title terms that short-circuit candidate processing."""
    return _EXCLUDED_TITLE_TERMS


class SearchNoiseReason(StrEnum):
    """Stable reason values for search-result noise classification."""

    APP_STORE = "app_store"
    SOCIAL_PROFILE = "social_profile"
    TITLE_KEYWORD_MATCH = "title_keyword_match"
    UNSUPPORTED_SEARCH_DOMAIN = "unsupported_search_domain"
    IRRELEVANT_CONTENT_DOMAIN = "irrelevant_content_domain"
    NO_HEBREW_CONTENT = "no_hebrew_content"


@dataclass(frozen=True)
class SearchNoiseClassification:
    """Classification for a retained but non-scrapeable search-result surface."""

    reason: SearchNoiseReason
    matched_domain: str = ""
    matched_keyword: str = ""


def globally_excluded_search_domains() -> frozenset[str]:
    """Return the set of domains excluded from all broad/taxonomy search queries.

    These are domains that are structurally off-topic (sports verticals, utility
    sites, etc.) and should never consume search-engine quota.  The list mirrors
    ``_IRRELEVANT_CONTENT_DOMAINS``; callers should not duplicate it.
    """
    return _IRRELEVANT_CONTENT_DOMAINS


def normalize_domain(domain: str | None) -> str | None:
    """Normalize hosts for candidate-filter comparisons."""
    if domain is None:
        return None
    normalized = domain.strip().casefold()
    if not normalized:
        return None
    if normalized.startswith("www."):
        normalized = normalized[4:]
    return normalized or None


def candidate_domain(discovered: DiscoveredCandidate) -> str | None:
    """Return the normalized candidate host, preferring explicit model domain."""
    normalized_domain = normalize_domain(discovered.domain)
    if normalized_domain is not None:
        return normalized_domain
    return normalize_domain(
        urlparse(str(discovered.canonical_url or discovered.candidate_url)).netloc
    )


def candidate_path(discovered: DiscoveredCandidate) -> str:
    """Return the path for the current candidate identity URL."""
    parsed = urlparse(str(discovered.canonical_url or discovered.candidate_url))
    return (parsed.path or "/").casefold()


def match_domain(domain: str | None, configured_domains: frozenset[str]) -> str | None:
    """Return the configured base domain matched by a host, including subdomains."""
    if domain is None:
        return None
    return next(
        (
            configured
            for configured in sorted(configured_domains, key=len, reverse=True)
            if domain == configured or domain.endswith(f".{configured}")
        ),
        None,
    )


def _is_app_store_url(discovered: DiscoveredCandidate) -> bool:
    domain = candidate_domain(discovered)
    matched_domain = match_domain(domain, _APP_STORE_DOMAINS)
    if matched_domain is None:
        return False
    path = candidate_path(discovered)
    if matched_domain == "play.google.com":
        return path.startswith("/store/apps/")
    return path == "/app" or "/app/" in path


def _is_x_or_twitter_post_path(path: str) -> bool:
    segments = [segment for segment in path.split("/") if segment]
    return (
        len(segments) >= 3 and segments[1] in {"status", "statuses"} and segments[2].isdigit()
    ) or (
        len(segments) >= 4
        and segments[0] == "i"
        and segments[1] == "web"
        and segments[2] == "status"
        and segments[3].isdigit()
    )


def _is_tiktok_video_path(path: str) -> bool:
    segments = [segment for segment in path.split("/") if segment]
    return (
        len(segments) >= 3
        and segments[0].startswith("@")
        and segments[1] == "video"
        and segments[2].isdigit()
    )


def _is_social_profile_candidate(discovered: DiscoveredCandidate) -> bool:
    domain = candidate_domain(discovered)
    matched_domain = match_domain(domain, _SOCIAL_PROFILE_DOMAINS)
    if matched_domain is None:
        return False
    path = candidate_path(discovered)
    if matched_domain in {"x.com", "twitter.com"}:
        return not _is_x_or_twitter_post_path(path)
    if matched_domain == "tiktok.com":
        return not _is_tiktok_video_path(path)
    if matched_domain in _SOCIAL_PROFILE_PATH_PREFIXES:
        return any(
            path.startswith(prefix) for prefix in _SOCIAL_PROFILE_PATH_PREFIXES[matched_domain]
        )
    if matched_domain in _SOCIAL_POST_PATH_PREFIXES:
        return not any(
            path.startswith(prefix) for prefix in _SOCIAL_POST_PATH_PREFIXES[matched_domain]
        )
    return False


def classify_search_noise(
    discovered: DiscoveredCandidate,
) -> SearchNoiseClassification | None:
    """Classify obvious non-article search-result surfaces before scrape selection."""
    if discovered.producer_kind is not ProducerKind.SEARCH_ENGINE:
        return None
    domain = candidate_domain(discovered)
    if _is_app_store_url(discovered):
        matched_domain = match_domain(domain, _APP_STORE_DOMAINS)
        if matched_domain is not None:
            return SearchNoiseClassification(
                reason=SearchNoiseReason.APP_STORE,
                matched_domain=matched_domain,
            )
    if (matched_domain := match_domain(domain, _UTILITY_SEARCH_DOMAINS)) is not None:
        return SearchNoiseClassification(
            reason=SearchNoiseReason.UNSUPPORTED_SEARCH_DOMAIN,
            matched_domain=matched_domain,
        )
    if (matched_domain := match_domain(domain, _IRRELEVANT_CONTENT_DOMAINS)) is not None:
        return SearchNoiseClassification(
            reason=SearchNoiseReason.IRRELEVANT_CONTENT_DOMAIN,
            matched_domain=matched_domain,
        )
    if _is_social_profile_candidate(discovered):
        matched_domain = match_domain(domain, _SOCIAL_PROFILE_DOMAINS)
        if matched_domain is not None:
            return SearchNoiseClassification(
                reason=SearchNoiseReason.SOCIAL_PROFILE,
                matched_domain=matched_domain,
            )
    return None


def classify_title_noise(
    discovered: DiscoveredCandidate,
) -> SearchNoiseClassification | None:
    """Classify candidates whose title or snippet contains an excluded keyword.

    Applies to all producer kinds — this is a pre-scrape cost filter, not
    a search-result surface filter.  Both ``title`` and ``snippet`` are
    searched so that noise terms buried only in the snippet are caught too.
    Returns the first matching term.
    """
    title = (discovered.title or "").casefold()
    snippet = (discovered.snippet or "").casefold()
    text = title + " " + snippet
    if not text.strip():
        return None
    for term in sorted(_EXCLUDED_TITLE_TERMS, key=len, reverse=True):
        if term.casefold() in text:
            return SearchNoiseClassification(
                reason=SearchNoiseReason.TITLE_KEYWORD_MATCH,
                matched_keyword=term,
            )
    return None


def classify_no_hebrew_content(
    discovered: DiscoveredCandidate,
) -> SearchNoiseClassification | None:
    """Classify candidates that contain no Hebrew letters in title or snippet.

    Applies to all producer kinds.  Content without any Hebrew letter is
    overwhelmingly not relevant to the Israeli enforcement beat and can be
    dropped as early as possible in the pipeline.

    Returns ``SearchNoiseClassification(reason=NO_HEBREW_CONTENT)`` when
    neither ``title`` nor ``snippet`` contain at least one Hebrew character
    (U+05D0–U+05EA).  Returns ``None`` when Hebrew is present (i.e. the
    candidate should continue to the next filter).
    """
    title = discovered.title or ""
    snippet = discovered.snippet or ""
    if _HEBREW_RE.search(title) or _HEBREW_RE.search(snippet):
        return None
    return SearchNoiseClassification(reason=SearchNoiseReason.NO_HEBREW_CONTENT)
