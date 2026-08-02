-- Regression suite database.
--
-- WHY THIS EXISTS
--
-- The matrix is relational data — formats x oracles x platforms x fixtures x
-- runs — and it was previously written as prose tables in markdown. That is
-- unqueryable, drifts silently, and cannot be executed. Questions we actually
-- need answered ("what can we not verify anywhere?", "what did this platform's
-- run leave uncovered?", "has this case regressed?") are one-line queries here
-- and a careful human read of four documents otherwise.
--
-- THE THREE LAYERS
--
--   1. Declarative source of truth, in git, hand-editable:
--        data/formats.toml     what rb-cli reads and writes
--        data/oracles.toml     what can verify it, and where that tool exists
--        fixture-map*.tsv      the fixture catalogue
--        cases/**.toml         the cases themselves
--   2. This database. Built from layer 1 plus run results. REGENERABLE —
--      never hand-edit it, never treat it as precious.
--   3. Generated markdown for humans (VERIFICATION-MATRIX.md, GAPS.md,
--      COVERAGE.md). Outputs, not inputs. Never hand-edited.
--
-- Layer 1 stays as text files so changes are reviewable in a diff. The
-- database is the query engine, not the record.
--
-- SQLite because: single file, no server, lives next to the corpus on the
-- share, and needs no system package — Python ships a driver in its stdlib and
-- the Rust runner can link it statically.

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- What rusty-backup handles
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS format (
    id          TEXT PRIMARY KEY,          -- 'fs.fat32', 'fmt.chd', 'part.gpt'
    kind        TEXT NOT NULL CHECK (kind IN ('fs','container','optical','partition')),
    name        TEXT NOT NULL,
    we_read     INTEGER NOT NULL DEFAULT 0,
    we_write    INTEGER NOT NULL DEFAULT 0,
    -- The rb-cli verb that builds one, if any. Presence here means the WRITE
    -- path exists and therefore needs an oracle behind it; it is never itself
    -- evidence of correctness.
    builder     TEXT,
    notes       TEXT
);

-- ---------------------------------------------------------------------------
-- What can independently judge our output
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS oracle (
    id          TEXT PRIMARY KEY,          -- 'chdman', 'fsck_hfs', 'iris'
    tool        TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('package','mount','emulator','hardware')),
    notes       TEXT
);

-- An oracle is only useful on hosts where it actually exists. Kept separate
-- from `oracle` because availability is per-platform and changes over time.
CREATE TABLE IF NOT EXISTS oracle_availability (
    oracle_id   TEXT NOT NULL REFERENCES oracle(id) ON DELETE CASCADE,
    platform    TEXT NOT NULL,             -- windows | wsl | linux | macos | mister
    status      TEXT NOT NULL CHECK (status IN ('verified','expected','install','absent')),
    path_hint   TEXT,                      -- where it was found, if verified
    verified_on TEXT,                      -- ISO date of the last real check
    PRIMARY KEY (oracle_id, platform)
);

-- The core relation: this oracle can judge that format, in that direction.
CREATE TABLE IF NOT EXISTS verification (
    oracle_id   TEXT NOT NULL REFERENCES oracle(id) ON DELETE CASCADE,
    format_id   TEXT NOT NULL REFERENCES format(id) ON DELETE CASCADE,
    direction   TEXT NOT NULL CHECK (direction IN ('read','write')),
    -- authoritative = the vendor's own tool (IRIX checking EFS)
    -- structural    = a competent third party (qemu-img, fsck.vfat)
    -- smoke         = weak; opens without error, little more
    strength    TEXT NOT NULL CHECK (strength IN ('authoritative','structural','smoke')),
    status      TEXT NOT NULL CHECK (status IN ('proven','plausible','untested')),
    evidence    TEXT,                      -- what was actually observed
    PRIMARY KEY (oracle_id, format_id, direction)
);

-- ---------------------------------------------------------------------------
-- Fixtures and emulator images
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fixture (
    id              TEXT PRIMARY KEY,
    relpath         TEXT NOT NULL,
    bytes           INTEGER,
    sha256          TEXT,
    -- Provenance is a correctness property, not bookkeeping: a fixture we
    -- produced ourselves is not an independent opinion. 'rusty-backup' is
    -- never a legal value here.
    origin          TEXT,
    producer_tool   TEXT,
    redistributable TEXT,
    class           TEXT NOT NULL DEFAULT 'reference'
                    CHECK (class IN ('reference','populated','emulator-image')),
    location        TEXT NOT NULL DEFAULT 'corpus'
                    CHECK (location IN ('repo','corpus','annex','external')),
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS fixture_format (
    fixture_id  TEXT NOT NULL REFERENCES fixture(id) ON DELETE CASCADE,
    format_id   TEXT NOT NULL REFERENCES format(id) ON DELETE CASCADE,
    PRIMARY KEY (fixture_id, format_id)
);

-- A bootable OS install: a fixture that is also an oracle once running.
CREATE TABLE IF NOT EXISTS emulator_image (
    id          TEXT PRIMARY KEY,
    path        TEXT NOT NULL,
    bytes       INTEGER,
    emulator    TEXT,
    -- reads-ok: we can parse it. boots: it starts. oracle-wired: its native
    -- tools feed the harness. Three honest stages, not one boolean.
    status      TEXT NOT NULL CHECK (status IN ('reads-ok','boots','oracle-wired')),
    notes       TEXT
);

-- ---------------------------------------------------------------------------
-- Runs and results
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS run (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_utc TEXT NOT NULL,
    platform    TEXT NOT NULL,
    host        TEXT,
    rb_version  TEXT,
    git_sha     TEXT,
    bundle_path TEXT
);

CREATE TABLE IF NOT EXISTS result (
    run_id      INTEGER NOT NULL REFERENCES run(id) ON DELETE CASCADE,
    case_id     TEXT NOT NULL,
    tier        INTEGER,
    group_name  TEXT,
    verdict     TEXT NOT NULL CHECK (verdict IN
                  ('pass','fail','skip-fixture','skip-platform',
                   'skip-tool','skip-hardware','error')),
    duration_ms INTEGER,
    fixture_id  TEXT,
    detail      TEXT,
    PRIMARY KEY (run_id, case_id)
);

CREATE INDEX IF NOT EXISTS idx_result_case ON result(case_id);
CREATE INDEX IF NOT EXISTS idx_result_verdict ON result(verdict);

CREATE TABLE IF NOT EXISTS finding (
    id          TEXT PRIMARY KEY,          -- 'R-009'
    title       TEXT NOT NULL,
    severity    TEXT CHECK (severity IN ('high','medium','low','doc')),
    status      TEXT NOT NULL DEFAULT 'open'
                CHECK (status IN ('open','fixed','accepted','wontfix')),
    first_seen  TEXT,
    detail      TEXT
);

-- ---------------------------------------------------------------------------
-- Views: the questions that used to require reading four documents
-- ---------------------------------------------------------------------------

-- Formats we can write with no oracle behind them. Per README.md, a write
-- path with no independent check is unverified regardless of how many of our
-- own tests pass. This is the single most important query in the suite.
CREATE VIEW IF NOT EXISTS v_unverified_writes AS
SELECT f.id, f.kind, f.name, f.builder
FROM format f
WHERE f.we_write = 1
  AND NOT EXISTS (
      SELECT 1 FROM verification v
      WHERE v.format_id = f.id AND v.direction = 'write'
  )
ORDER BY f.kind, f.id;

-- Formats we read with no reference fixture — nothing third-party to read.
CREATE VIEW IF NOT EXISTS v_unfixtured_reads AS
SELECT f.id, f.kind, f.name
FROM format f
WHERE f.we_read = 1
  AND NOT EXISTS (
      SELECT 1 FROM fixture_format ff WHERE ff.format_id = f.id
  )
ORDER BY f.kind, f.id;

-- Per format: does it have a reference fixture, and a write oracle?
CREATE VIEW IF NOT EXISTS v_coverage AS
SELECT
    f.id, f.kind, f.name, f.we_read, f.we_write,
    (SELECT COUNT(*) FROM fixture_format ff WHERE ff.format_id = f.id) AS fixtures,
    (SELECT COUNT(*) FROM verification v
      WHERE v.format_id = f.id AND v.direction = 'write')             AS write_oracles,
    (SELECT MAX(v.strength = 'authoritative') FROM verification v
      WHERE v.format_id = f.id)                                       AS has_authoritative
FROM format f
ORDER BY f.kind, f.id;

-- Which oracles can actually run on a given platform.
CREATE VIEW IF NOT EXISTS v_oracle_reach AS
SELECT o.id AS oracle, o.kind, a.platform, a.status, v.format_id, v.direction, v.strength
FROM oracle o
JOIN oracle_availability a ON a.oracle_id = o.id
LEFT JOIN verification v   ON v.oracle_id = o.id
WHERE a.status IN ('verified','expected');

-- Formats verifiable ONLY on one platform — these decide which machines the
-- suite genuinely requires.
CREATE VIEW IF NOT EXISTS v_platform_pins AS
SELECT format_id, direction, MIN(platform) AS only_platform, COUNT(DISTINCT platform) AS n
FROM v_oracle_reach
WHERE format_id IS NOT NULL
GROUP BY format_id, direction
HAVING n = 1
ORDER BY format_id;

-- Latest verdict per case per platform, for regression comparison.
CREATE VIEW IF NOT EXISTS v_latest_result AS
SELECT r.case_id, r.verdict, r.tier, r.group_name, run.platform, run.started_utc
FROM result r
JOIN run ON run.id = r.run_id
WHERE run.id = (
    SELECT MAX(r2.run_id) FROM result r2
    JOIN run q ON q.id = r2.run_id
    WHERE r2.case_id = r.case_id AND q.platform = run.platform
);
