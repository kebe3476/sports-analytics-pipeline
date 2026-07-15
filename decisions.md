# Decisions

ADR-style log of tradeoffs made during design, and why. Check here before revisiting a decision - if a past call turns out to be wrong, add a new entry rather than silently overriding the old one.

## D001 - Warehouse: BigQuery over Snowflake

BigQuery has a permanent free tier; Snowflake's free option is a time-limited trial, which matters for a project meant to keep running indefinitely. Keith already has professional Snowflake experience (Salesforce/Marketo/LMS star schema work), so BigQuery also broadens the platforms covered rather than repeating what's already familiar.

## D002 - Scoped as one complete pipeline, not split across separate projects

Originally scoped as two separate projects to cover more platform ground. Rescoped to a single pipeline spanning ingestion, transformation, orchestration, the AI extraction step, and BI, since splitting the work would have diluted focus without making either piece more complete. Better to have one fully connected system than several partial ones.

## D003 - Domain: sports data

Chosen because it naturally produces both structured data (stats, ideal for medallion/star schema) and unstructured data (recaps/commentary) that contains information the structured data doesn't - which is what makes the AI component a real requirement instead of a demo feature.

## D004 - AI component: LLM extraction step, not a chatbot

The AI's job is extracting structured fields (injury flags, momentum, key events) from recap text between the silver and gold layers - solving a real "unstructured text has no structured equivalent" problem. A RAG Q&A chatbot layer is real but secondary, deferred to v2, built on top of the extraction work rather than being the headline feature.

## D005 - Vector storage: bridge table, not a column on `fact_games`

Considered adding an embedding column directly to the fact table. Rejected because RAG-quality retrieval requires chunking recap text (multiple embeddings per game), which would either denormalize multiple vectors into one row (violates 1NF) or force a grain change on the core fact table. Instead: `recaps` bridges `fact_games` (one game → many recaps), and `recap_chunks` hangs off `recaps` at chunk grain, holding the `ARRAY<FLOAT64>` embeddings. `fact_games` is untouched. BigQuery doesn't enforce FK constraints, so integrity between these tables is enforced via dbt `relationships` tests.

## D006 - Orchestrator: Airflow over Dagster

Airflow is the more widely adopted orchestrator in production data engineering stacks and the one most commonly paired with dbt in practice. Dagster's tighter native dbt integration was considered and is a legitimate alternative, not chosen here.

## D007 - BI tool: Superset over Metabase

Metabase is easier to stand up. Superset is more complex to operate but is the more substantial open-source BI tool of the two, and operating it end to end was worth more than the easier setup.

## D008 - File naming: lowercase for internal docs, CLAUDE.md stays uppercase

Repo docs (architecture.md, guardrails.md, progress.md, decisions.md) use lowercase - no functional reason for caps, just convention. CLAUDE.md is the one exception: Claude Code looks for that exact filename automatically at session start, so it stays uppercase regardless of style preference. README.md also stays uppercase per standard GitHub convention.

## D009 - Diagrams: Whimsical primary, Mermaid local fallback

Whimsical is cloud-hosted and directly editable, so it's the source of truth for diagrams going forward. Mermaid versions are kept in diagrams.md as the local, version-controlled copy - they live in git, render natively on GitHub, and can be edited online via Mermaid Live without needing Whimsical access. When the two would ever drift, Whimsical wins; Mermaid gets re-synced from it, not the other way around.

## D010 - Data sources: BALLDONTLIE for stats, ESPN's unofficial API for recap text - not StoryStats, not NewsAPI.org

Researched three candidates for the recap/commentary source and one for structured stats:

- **Structured stats: BALLDONTLIE.** Correction (see D011): the free tier does NOT include box scores, player stats, standings, or play-by-play - only Teams/Players/Games metadata. Actual stats data requires the paid GOAT tier ($39.99/mo per sport) or ALL-ACCESS ($299.99/mo, all sports). Sport/tier decision tracked in D011.

- **Recap text - rejected StoryStats (BALLDONTLIE's own content API) despite its clean permanent free tier ($0 forever, 10 req/day).** Its stories are AI-generated narrative built directly from the same play-by-play/stats data we already ingest ("OKC edged DEN 129-126 as Gilgeous-Alexander posted 35 points..." - a prose restatement of the box score, not independent reporting). Using it would mean extracting "injury flags" and "key events" from text that was itself generated from stats we already have structured - circular, and it quietly breaks the project's founding premise (D003/D004: the AI step exists because recap text has information the structured stats don't). Convenient, same-vendor, but wrong for what this pipeline actually needs.

- **Recap text - rejected NewsAPI.org free tier.** Explicit ToS blockers: development-and-testing only, no production or staging use, CORS restricted to localhost, 24-hour article delay. Doesn't fit a project meant to be run and shown working, and the paid tier that removes those restrictions is $449/mo.

- **Recap text - going with ESPN's unofficial `site.api.espn.com` news/article endpoints.** Undocumented, no formal ToS grant, and could change or get rate-limited without notice - a real, accepted risk, not swept under the rug. But it's actual journalist-written content (quotes, injury reports, coaching context) independent of the box score, which is what the AI extraction step needs to be a genuine problem instead of a circular one. Community consensus across multiple sources: reasonable for small-scale projects with light request volume and resilient error handling, risky for commercial/heavy-traffic use, which matches this project's actual scale. Mitigations: cache aggressively (same principle as the LLM cost-control guardrail), keep polling light, and have a documented fallback (StoryStats, accepting the weaker AI story) if the endpoint becomes unusable mid-build.

## D011 - Structured stats: open-source, per-sport ecosystem instead of one paid vendor

Rejected BALLDONTLIE's paid tiers (GOAT $39.99/mo/sport, ALL-ACCESS $299.99/mo) as an unnecessary recurring cost once genuinely free, actively-maintained open alternatives were confirmed for every sport in scope (NBA, NFL, NHL, MLB, NCAAF):

- **NFL: [nflverse](https://github.com/nflverse)** (nflreadr / nflreadpy / nfl_data_py) - flat files (CSV/parquet) on GitHub releases, CC-BY 4.0 licensed, no API key, no rate limit, play-by-play back to 1999, updated nightly in season.
- **NBA: [nba_api](https://github.com/swar/nba_api)** - MIT-licensed open source client wrapping stats.nba.com's own endpoints. Same risk category as the ESPN recap decision (D010): unofficial but stable and actively maintained.
- **NHL: [api-web.nhle.com](https://github.com/Zmalski/NHL-API-Reference)** - the NHL's own public API, no key required, free. Same "unofficial but stable" risk tier as nba_api.
- **MLB: [MLB Stats API](https://statsapi.mlb.com/)** (`statsapi.mlb.com`), not pybaseball's Baseball-Reference/FanGraphs scraping. Corrected after deeper research (see D012) - this is the actual official API powering MLB.com, the MLB app, and Statcast: real JSON REST endpoints, no auth required, same "official source, unofficial-but-stable" risk tier as nba_api and the NHL API. Community wrappers: `MLB-StatsAPI`, `python-mlb-statsapi`.
- **NCAAF: [CollegeFootballData (CFBD)](https://collegefootballdata.com/)** - genuinely documented free-tier REST API with an API key, 1,000 calls/month free. Safest and most conventional integration of the five.

**Tradeoff accepted:** five different ingestion patterns (bulk file download, two unofficial-but-stable REST wrappers, one scraping library, one documented REST API) instead of one unified vendor API. More integration work than a single API, but zero recurring cost, and arguably a stronger DE story - heterogeneous source integration is closer to real-world data engineering than hitting one clean endpoint.

**Recap/commentary text** (D010, ESPN's unofficial news endpoints) already covers all five sports under the same API pattern - no additional recap-source research needed per sport.

**Build sequencing recommendation:** prove the full pipeline (bronze → silver → LLM extraction → gold → Superset) against one sport first - NBA, since `nba_api` is the most mature of the five wrappers - before replicating the ingestion pattern across the other four. Reduces the risk of debugging five ingestion connectors and the dimensional model simultaneously.

## D012 - MLB source upgraded; build sequencing changed to NFL → NCAAF → NHL → NBA → MLB

**MLB deep-dive.** Went back to look for something better than pybaseball, since a scraping library was clearly the weakest of the five sources. Found it: **MLB Stats API** (`statsapi.mlb.com`) is the actual official data source that powers MLB.com, the MLB app, and Statcast - a genuine JSON REST API, no auth required, community-documented (`docs.statsapi.mlb.com`, community wrappers `MLB-StatsAPI` and `python-mlb-statsapi`). This is the same risk tier as `nba_api` and the NHL's `api-web.nhle.com`: official league source, no formal developer ToS, but stable and widely used - not a scraping library.

This also surfaced a real distinction worth having on record: pybaseball pulls from three different places, and they are not equally risky. Statcast comes from Baseball Savant, which is MLB's own site - comparable risk to the Stats API. But pybaseball's traditional-stats functions scrape Baseball-Reference and FanGraphs, and both of those sites have explicit anti-scraping language in their terms (Baseball-Reference: no scripts/bots/scrapers, no building tools on scraped data without permission; FanGraphs: doesn't scrape anyone else's data itself and relies on paid data-provider agreements, i.e. they don't want to be on the other end of it either). That's a materially different, and worse, risk category than "official league site with no formal ToS either way" - it's "explicitly against a third party's stated terms."

**Decision:** structured MLB stats come from the official MLB Stats API. If advanced Statcast metrics (exit velocity, spin rate, etc.) are wanted beyond what the Stats API returns, pybaseball's Statcast/Baseball Savant functions are an acceptable supplement later - its Baseball-Reference and FanGraphs scraping functions are out of scope for this project.

**Build sequencing, superseding D011's "NBA first" recommendation:** Keith's call - build in this order: **NFL → NCAAF → NHL → NBA → MLB**. MLB goes last in part because of timing (season/off-season considerations) and in part because it's the sport whose source just changed out from under the original plan - better to build the ingestion pattern against four sports on more settled sources first, and hit MLB once the pattern is proven and the new source has had a chance to be evaluated hands-on.

## D013 - Git workflow: trunk-based + feature branches + Conventional Commits + GitHub Actions, not GitFlow

Since Claude Code will be handling git operations directly, the workflow needed to be explicit rather than assumed. Full conventions in git-workflow.md; summary here:

- **Branching:** trunk-based - `main` always deployable, short-lived feature branches merged via PR (squash merge). Explicitly not GitFlow: GitFlow's develop/release/hotfix branches exist to manage parallel release trains and hotfixing live production separately from in-progress development, neither of which applies here: one contributor, one deployable target. Trunk-based + PRs still shows real review discipline without that overhead.
- **Commits:** Conventional Commits (`feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`) - industry standard, and a clean conventional history also makes the repo easier to navigate and review later.
- **CI:** GitHub Actions, free for public repos and the most commonly expected CI tool in DE/AE postings. Scaffolded incrementally alongside the stack - lint from day one, dbt test once the dbt project exists, DAG import validation once Airflow exists. No premature steps checking things that don't exist yet.
- **Guardrail (see guardrails.md):** never commit or push directly to `main`, never force-push, never merge a PR without Keith's review - applies to Claude Code the same as it would to a human contributor.

## D014 - Collaboration model: this is a learning project, Claude Code should not run on autopilot

Explicit, easy to lose track of once the build gets going: Keith wants this to be a genuinely collaborative, hands-on learning process, not a repo that Claude Code generates end-to-end while he watches. Added to guardrails.md as a standing rule rather than a one-time reminder, since "explain your reasoning" is exactly the kind of instruction that erodes over a long session if it only exists as a memory rather than a file that gets read every session.

Concretely: explain the approach before implementing anything non-trivial or done for the first time, keep PRs small enough to actually review and learn from (also reinforces git-workflow.md's PR discipline), explain the "why" and not just ship working code, and surface real tradeoffs as decisions for Keith rather than silently resolving them. Repeated patterns (e.g., ingestion for sport 2 through 5, once sport 1's approach is proven) don't need the same re-explanation every time - the goal is learning the pattern once, not narrating every repetition.

## D015 - Style rule: no emojis, no em dashes, added to CLAUDE.md and applied retroactively

Added a Style section to CLAUDE.md: no emojis anywhere, no em dashes anywhere, use a period, comma, colon, or parentheses instead. Applied retroactively across all existing docs (CLAUDE.md, README.md, architecture.md, guardrails.md, progress.md, decisions.md, git-workflow.md, diagrams.md), 122 em dashes replaced, no emojis found. This is a standing rule, not a one-time cleanup; new content going forward should not reintroduce either.

## D016 - README rewrite: show range through structure, don't narrate it

First version of README.md opened with a single sentence trying to announce "demonstrates range across DA/AE/DE," justify the AI component isn't "just for its own sake," and explain the single-project scoping decision, all before saying what the project actually does. Keith called it out directly: too in-your-face, too verbose. Rewritten to lead with a plain two-sentence description of what the pipeline does, then let short tables (stack, data sources, role coverage) carry the "range" signal instead of prose asserting it. Also caught while rewriting: the old README still described the single-sport, single-vendor version of the project from before D011 to D012, never updated after the sport list and sourcing changed.

General rule going forward, not just for this file: state what something does, let the reader draw the "this shows range" conclusion themselves. Don't pre-defend design choices in the lede against criticism nobody's made yet.

## D017 - Stripped portfolio/job-search framing from every doc, not just the README

Went further than D016. Every doc had some version of the same problem: "demonstrates range," "portfolio project," "job-search-driven," "interview story," "portfolio signal," plus explicit mentions of the secondary Databricks/PySpark project that was never meant to be visible here. Keith called it out across the whole repo, not just the README: this needs to read as a real system, not as a project narrating its own job-search purpose.

Fixed in CLAUDE.md, architecture.md, decisions.md (D001, D002, D006, D007, D010, D011, D013), git-workflow.md, and progress.md. Reasoning that was genuinely technical (Airflow's wider adoption, Superset's depth over Metabase's ease) kept its substance, just reframed without the "this helps my job search" framing. The secondary-project references were removed outright rather than reframed, since the point was to not reveal it exists at all.

Also re-verified no em dashes or emojis had crept back in from edits made after D015's original cleanup pass. Confirmed clean across all eight files.

## D018 - Code comment standard: why, not what, and keep it short

Added a code comments section to git-workflow.md after Keith flagged that generated comments were running too descriptive. Standard: comment reasoning and non-obvious decisions, not a narration of what the code already says. Keep it to a line or two; anything longer belongs in a PR description or decisions.md instead of inline. dbt models document through schema.yml descriptions, which dbt docs generate actually surfaces, not through SQL comment blocks. No commented-out code left in commits.
