# STATUS

Working plan for the code-quality pass on `agage-archive`. Read
[AGENTS.md](AGENTS.md) first — in particular the rule that output files and archive
structure must not change.

**Started:** 2026-07-28
**Last updated:** 2026-07-31 (A1–A4 combined-file attributes: owner union, baseline
provenance, comment de-dup)

Update the checkboxes and the "Last updated" date as items land. Keep the findings
register at the bottom in sync — if an item turns out to be a non-issue, mark it
`WONTFIX` with a one-line reason rather than deleting it.

The full suite currently has an existing baseline failure: eight Picarro checksum
differences in the golden manifest, reproduced on `origin/main` with the current
environment. Do not regenerate the manifest until the cause is resolved.

---

## Baseline

Measured on 2026-07-28, before any changes, in the `agage` conda environment:

- **44 tests passing**, ~25 s
- **67% line coverage** overall
- One `combine_datasets('agage_test', 'ch3ccl3', 'CGO')` call: **5.27 s**, 354 file
  opens, 373 `Paths()` constructions, 343 `config.yaml` parses

Per-module coverage:

| Module | Coverage |
| --- | --- |
| `run.py` | 29% |
| `config.py` | 64% |
| `util.py` | 64% |
| `formatting.py` | 85% |
| `data_selection.py` | 86% |
| `io.py` | 89% |
| `definitions.py` | 89% |
| `convert.py` | 93% |
| `io_other_formats.py` | 0% |
| `visualise.py` | 0% |
| `widgets.py` | 0% |

Re-measure these at the end of each phase.

---

## Phase 0a — Fixture closure ✅ DONE (2026-07-29)

Had to come first: the `agage_test` release schedules had **114 live cells but only 8
input data files**, so ~90 site/species combinations failed silently into
`error_log_individual.txt` on every run. Generating a golden manifest before fixing this
would have frozen a ragged, half-broken archive as the reference.

- [x] **Trim release schedules to match available data.** Data-less cells marked `x`
      rather than deleted, so the files keep their shape and still exercise schedule
      parsing (general-release-date fill, explicit per-cell dates, `x` markers,
      whitespace stripping). ALE/GAGE/GCMD/Picarro edited; Magnum/Medusa/flask were
      already correct.
- [x] **Two determinism bugs fixed** (see CHANGELOG). Found by running `run_all` twice and
      diffing manifests — the archive was not reproducible, which would have made the
      golden test flaky:
      - `read_data_combination` iterated `set(instruments)` → combined-file global
        attributes varied with the Python hash seed
      - `run_all` processed instruments in filesystem glob order → which instrument won a
        contested top-level file varied
- [x] **B9 fixed** — flask + `baseline=True` crashed and produced no output at all.
- [x] **B3 fixed** — `run_timestamp_checks` used Dataset truthiness.
- [x] **Missing fixtures added**, each unlocking a previously dead code path:
      `README.md`/`CHANGELOG.md` in `data/agage_test/` (`copy_to_archive`);
      `attributes_site_species_instrument.json` (the `attrs[attr] += ...` append);
      `scale_defaults_GCMD_CGO.csv` (instrument-specific scale defaults, which now do a
      real SIO-05 → SIO-98 conversion on the individual GCMD file while the combined
      file stays on the network default).
- [x] **Picarro-1/Picarro-2 assessed and dropped.** On inspection it buys almost nothing:
      the numbered-instrument attribute path (`instrument_1` … `instrument_4`) is already
      fully exercised by the combined CGO `ch3ccl3` file, and `get_instrument_number`
      would take the exact-match path, not the partial match. Not worth two binary
      fixtures plus instrument renumbering. Revisit only if the partial-match bug is
      being fixed.

**Result:** `run_all` on `agage_test` produces 125 netCDF files across all six directory
kinds, writes no error log, and is byte-identical across runs and across
`PYTHONHASHSEED` values.

## Phase 0 — Safety net (#38) ✅ DONE (2026-07-30)

- [x] **Golden archive manifest test** — `tests/test_archive.py`, reference at
      `tests/reference/archive_manifest.json` (127 entries). Pins per-file paths,
      variables, dtypes, encoding, variable and global attributes, `n_time`, and a data
      checksum rounded to 6 significant figures (float32 carries ~7, so this absorbs
      last-bit noise between library versions while still catching real changes).
      Regenerate with `AGAGE_UPDATE_MANIFEST=1 python -m pytest tests/test_archive.py`.
- [x] **Assert the error logs are empty** — `test_archive_has_no_errors`.
- [x] **`conftest.py` fixture** isolating `data/agage_test/output`; `test_cf_compliance`
      no longer deletes the output tree as a side effect.
- [x] **Run the golden test against zip output as well as directory output.** ✅ Done
      2026-07-30, once its prerequisite (T3/B4/B5) landed. The `archive` fixture in
      `tests/test_archive.py` is parametrised over `{directory, zip}`: the zip variant
      redirects `output_path` to `output.zip` by patching the config loader (no change to
      the user's `config.yaml`), runs the full archive, then extracts the zip and reuses
      the existing reader. Zip members carry no stem prefix, so the extracted tree and the
      directory tree compare against the *same* reference manifest. The zip run's output is
      byte-identical to the directory run — no zip-specific regression — which also
      exercises the recent zip write-path fixes (member replacement, leading-slash
      normalisation, kept-open handle) end to end. Both variants are `slow`-marked and
      independently selectable (`-k zip` / `-k directory`).

**Exit criteria met:** perturbing one character of a `long_name` in `data/variables.json`
produces 84 manifest differences and fails the suite.

**Suite:** 89 passed (85 fast + 4 slow), fast tests ~32 s; the archive test now runs the
full archive twice (directory + zip), ~2.5 min total. Deselect the slow pair with
`-m "not slow"`.
**Coverage:** 67% → 77% overall; `run.py` 29% → 87%.

---

## Phase 1b — Combined-file attributes (#167, #169)

These need tackling **together**: they are all the same underlying problem, which is that a
combined file inherits one contributing instrument's global attributes wholesale instead of
deriving attributes that describe the combination. #167 (which instrument's station
metadata wins) is only the first slice of it.

- [x] **Station metadata from the most recently operating instrument** (#167). Done
      2026-07-29 via `most_recent_dataset()` in [io.py](agage_archive/io.py), applied in
      both `combine_datasets` and `combine_baseline`. Affects `inlet_latitude`,
      `inlet_longitude`, `inlet_base_elevation_masl`, `inlet_comment`.

- [x] **A1 — Data owners must be the union across contributing instruments** (#169).
      ✅ Done 2026-07-31. `data_owner`/`data_owner_email` were single-valued, taken from
      the one instrument whose attributes were inherited. `combine_data_owners`
      ([io.py](agage_archive/io.py)) now de-duplicates the union across contributors,
      ordered by the `data_combination` file, kept as the existing `", "`-separated
      **string** (decided over a netCDF list to avoid an encoding change and round-trip
      churn; the value already comma-joins multiple people within one instrument). Owner
      and email are paired positionally so they stay aligned. No effect in the fixture
      (every CGO instrument shares one owner), so the rule is pinned by a direct unit test,
      `test_combine_data_owners`, per A4.

- [x] **A2 — The combined baseline-flags file misreports its provenance** (#168).
      ✅ Done 2026-07-31. `combine_baseline` inherited one instrument's identity, so the
      combined CGO `ch3ccl3` baseline said `instrument = instrument_type = GCMS-Medusa`
      for a file spanning ALE+GAGE+GCMD+Medusa. It now summarises all contributors via
      `format_attributes_global_instruments` — `instrument`, `instrument_1` … `instrument_n`
      and `instrument_type` (`ALE/GAGE/GCMD/GCMS-Medusa`), mirroring the mole fraction
      file. Install dates aren't in the baseline inputs, so `instrument_date*` records each
      instrument's first baseline timestamp in its `data_combination` window. Changed 7
      combined baseline files in the manifest; single-instrument combined baselines gain
      `instrument_date`/`instrument_comment`, matching single-instrument combined MF files.

- [x] **A3 — Audit every other global attribute for the same class of error.**
      ✅ Done 2026-07-31. Worked through the two attribute paths. **Mole fraction path**
      (schema-driven `format_attributes`): `inlet_*` already handled by #167,
      `calibration_scale` validated identical across instruments (single-valued correct),
      `doi` empty; the one live item was `comment` (#175), now de-duplicated via
      `combine_comments` — identical instrument comments are listed once (no fixture change,
      the CGO comments happen to differ; pinned by `test_combine_comments`). **Baseline
      path** (inherits raw attrs): `inlet_*`/`station_long_name` correct under #167;
      `citation`/`contact` come from `baseline_attrs` and are identical across instruments;
      `instrument`/`instrument_type` fixed by A2.

- [x] **A4 — Pin the decisions in a test.** ✅ Done 2026-07-31. `test_combine_data_owners`
      and `test_combine_comments` assert the union/de-dup rules directly (the fixture does
      not exercise them); `test_combine_baseline` asserts the combined baseline lists every
      contributing instrument; `test_combine_datasets` asserts the combined owner.

**Constraint:** every item here changes published attribute values, so each needs an
explicit decision recorded in the log below before it lands.

## Phase 1 — Confirmed bugs

Each gets a failing test first, then the fix. Items 1–3 are the ones that can corrupt or
silently mis-align published data.

- [x] **B1 — `sortby(inplace=True)` raises.** ✅ Fixed 2026-07-29.
      [io.py:307-308](agage_archive/io.py#L307-L308).
      `Dataset.sortby()` has no `inplace` argument, and the result is discarded anyway, so
      the non-monotonic branch was dead code that crashed when reached. Replaced with
      `ds = ds.sortby("time")` and covered by an in-memory shuffled input test in
      [tests/test_io.py](tests/test_io.py).
- [x] **B2 — the data/baseline timestamp check could never fire.** ✅ Fixed 2026-07-29.
      Worse than first recorded. `(ds_baseline.time != ds.time).any()` is an xarray
      comparison, which **aligns both operands on the time coordinate before comparing**,
      so every timestamp was compared with itself and the result was always `False` — for
      any input, including two datasets with no timestamps in common. The check guarding
      data/baseline alignment was dead code, in both `run_timestamp_checks` and
      `run_individual_site`. Replaced with a `timestamps_match()` helper comparing the raw
      values via `np.array_equal`. Enabling it broke nothing: the invariant does hold
      throughout the fixture archive, it was simply never being verified. Covered by
      [tests/test_run.py](tests/test_run.py).
- [x] **B3 — `if ds_baseline:` is Dataset truthiness.** ✅ Fixed 2026-07-29 in
      `run_timestamp_checks` and `run_individual_site`. Covered by
      [tests/test_run.py](tests/test_run.py).
- [x] **B4 — `data_file_path` returns `None` silently.** ✅ Fixed 2026-07-30.
      [config.py:335-340](agage_archive/config.py#L335-L340). Zip + missing member +
      `errors="ignore"` returns `None`; callers fail later with an unrelated
      `AttributeError`. The directory branch with `errors="raise"` returns a path to a
      non-existent file *without* raising — the two branches disagreed about what `errors`
      means. `data_file_path` now raises consistently for missing files in `raise` mode.
- [x] **B5 — `errors=` is four undocumented magic strings.** ✅ Fixed 2026-07-30.
      `raise`, `ignore`,
      `ignore_inputs`, `ignore_outputs`; the `Paths` docstring says the default is `raise`
      when it is `ignore`; [config.py:261](agage_archive/config.py#L261) uses substring
      matching (`"ignore" in errors`) which matches all three ignore variants. Invalid
      values now raise `ValueError` with the accepted modes.
- [x] **B6 — unbound local in `read_release_schedule`.** ✅ Fixed 2026-07-30.
      [data_selection.py:97-103](agage_archive/data_selection.py#L97-L103). `pos` is only
      assigned inside the comment-scanning loop, so a schedule file whose first line is not
      a comment raised `UnboundLocalError` at `f.seek(pos)`. Initialize the rewind position
      before scanning; covered by a no-comment-header in-memory schedule test.
- [x] **B7 — species case mismatch between validation and lookup.** ✅ Fixed 2026-07-30.
      [data_selection.py:122-131](agage_archive/data_selection.py#L122-L131) and
      [data_selection.py:197-200](agage_archive/data_selection.py#L197-L200) validate with
      `format_species(species)` then indexed with the raw `species`. Normalize the matched
      index label before lookup; covered by a mixed-case species test.
- [x] **B8 — `instrument_type` is accidentally optional.** ✅ Fixed 2026-07-30.
      `data/variables.json` had `"optional": ""`, and
      [formatting.py:261](agage_archive/formatting.py#L261) treated anything other than the
      exact string `"False"` as optional — so a missing `instrument_type` was silently
      dropped rather than raising. Set it to `"False"` and covered it with the schema test.
- [x] **B9 — flask + baseline fails via the generic handler.** ✅ Fixed 2026-07-29.
      Baseline and monthly products are skipped when `read_baseline_function is None`;
      the `NotImplementedError` for "monthly without baseline" is preserved.
- [x] **B10 — `warnings.simplefilter` used as a global toggle.** ✅ Fixed 2026-07-30.
      [formatting.py:275-280](agage_archive/formatting.py#L275-L280) clobbers the caller's
      warning configuration and did not restore it on exception. Wrapped the cast in
      `warnings.catch_warnings()` and covered filter restoration with a regression test.
- [x] **B11 — module-level filesystem walk at import.** ✅ Fixed 2026-07-30.
      [io_other_formats.py:11](agage_archive/io_other_formats.py#L11) runs `Paths()` at
      import time and could raise on import; [line 52](agage_archive/io_other_formats.py#L52)
      used `delim_whitespace=True`, removed in pandas 3.0. Path resolution is now lazy and
      whitespace parsing uses `sep=r"\s+"`; covered by import and parser tests.
- [x] **B12 — `start_date` is too early in individual-instrument files.** ✅ Fixed 2026-07-30.

      All four readers call `format_attributes` before removing NaN mole fractions
      ([io.py:269](agage_archive/io.py#L269), [643](agage_archive/io.py#L643),
      [920](agage_archive/io.py#L920), [1111](agage_archive/io.py#L1111); `dropna` at
      [314](agage_archive/io.py#L314), [677](agage_archive/io.py#L677),
      [950](agage_archive/io.py#L950), [1141](agage_archive/io.py#L1141)). `start_date`
      therefore records the start of the *source record*, not of this species'
      measurements. The error is largest for the readers whose source files are
      multi-species — every GAGE species at CGO reports the same `1981-11-30 14:01`, and
      every Magnum species at MHD the same `1994-10-13 23:54`, whatever the species.

      | File | `start_date` | First real measurement | Error |
      | --- | --- | --- | --- |
      | `gage-gcmd_cgo_ch4` | 1981-11-30 14:01 | 1986-05-13 03:54 | 4.4 years |
      | `gage-gcmd_cgo_cfc-113` | 1981-11-30 14:01 | 1982-06-01 21:08 | 6 months |
      | `gcms-magnum_mhd_ch2cl2` | 1994-10-13 23:54 | 1995-04-06 23:56 | 6 months |

      15 of 25 individual files in the fixture are affected.

      Only `start_date`: `end_date` is correct because `output_dataset` recomputes it
      ([io.py:1491](agage_archive/io.py#L1491)) whenever an end date is passed, which
      `run_individual_site` always does. There is no equivalent for `start_date`.

      Combined files are correct only incidentally — `combine_datasets` also calls
      `format_attributes` before its own `dropna`, and gets away with it because the
      contributing datasets were each read with `dropna=True`. With `dropna=False` they
      would be wrong too.

      **Fix:** compute `start_date` and `end_date` in `output_dataset`, at write time, where
      the dataset is definitely final. This removes the whole class of "derived attribute
      computed too early" rather than this one instance, and subsumes the existing
      `end_date` special case. Changes an attribute on ~618 files in the real archive.

- [x] **B13 — Magnum inlet attributes have inconsistent types** (#171). ✅ Fixed 2026-07-30.
      Magnum files
      expose `inlet_base_elevation_masl`, `inlet_latitude`, and `inlet_longitude` as
      `np.float64`, while other readers expose them as strings. Confirm the archive schema's
      intended type, add a regression test, and record the output-format decision before
      changing published attributes. Global inlet attributes are now serialized as strings
      at the formatting boundary, matching the archive schema and other readers.

### Minor (batch into one commit)

- [x] `fnmatch.filter` shadows the builtin and raises `IndexError` rather than
      `FileNotFoundError` on no match. ✅ Fixed 2026-07-30. ZIP member lookup now raises
      `FileNotFoundError` with the requested member name.
- [x] `open_data_file` returns a handle from a closed `ZipFile`; works only via
      `ZipExtFile` refcounting. ✅ Fixed 2026-07-30. The archive is now kept open for the
      returned handle's lifetime and closed deterministically when the handle is closed;
      output is unchanged. Regression test asserts the backing `ZipFile` is open while the
      handle is live.
- [x] Leading-slash zip member when `output_subpath=""`. ✅ Fixed 2026-07-30. ZIP output
      now normalizes empty or slash-terminated subpaths without creating a leading slash.
- [x] Zip `"a"` mode creates duplicate members if a run is not preceded by `delete_archive`. ✅ Fixed
      2026-07-30. Existing members are replaced when writing the same archive path.
- [x] `if "time" in var` should be `var == "time"`. ✅ Fixed 2026-07-30. Variables whose
      names contain `time` are now processed normally during exclusions.
- [x] Instrument partial-matching uses dict insertion order, not longest match. ✅ Fixed
      2026-07-30. Partial matches now prefer the longest instrument name.
- [x] `choose_scale_defaults_file` broke ties using `data_file_list` order, which is
      filesystem glob order. ✅ Fixed 2026-07-30. Ties now use a stable filename sort.
- [x] `site_code` casing differs between readers. ✅ Fixed 2026-07-30. `read_nc` and
      `read_baseline` already upper-cased `site_code`; `read_ale_gage`, `read_gcms_magnum`
      and `read_gcwerks_flask` stored the raw `site` argument instead, so a lowercase or
      mixed-case site code would silently fail the `attributes_site.json` lookup in
      `format_attributes` (dict keys are upper-case) with no error. All readers now
      upper-case `site_code` consistently; no effect on the fixture, whose site codes are
      already upper-case.

---

## Phase 2 — Performance

All of this is provably output-preserving once Phase 0 is in place, which makes it the
best-value work in the plan.

- [x] **P1 — Cache config and JSON loads.** ✅ Done 2026-07-30. `config.py` now caches the
      `.git` walk, the `*_archive` package glob and the `config.yaml` parse (keyed on
      path+mtime), and adds `config.load_json` — a parse memoised by path+mtime that returns
      a fresh deep copy per call. Copy-on-return is required: `format_attributes` overlays
      network/site attributes onto `attributes.json` and `read_data_exclude` does
      `variable_defaults.update(...)` on `variables.json`, so a shared instance would poison
      the cache. Applied to `variables.json`, `attributes.json`, `standard_names.json`,
      `variables_not_public.json`. Per cold `combine_datasets` call: config parses **322 → 1**,
      package glob **357 → 1**, JSON parses **283 → 6**.
- [x] **P2 — Cache `define_instrument_number`.** ✅ Done 2026-07-30. Memoised on network,
      returns a fresh dict per call; **22 → 1** directory listings per cold call.
- [x] **P3 — Cache `ale_gage_sites.json`.** ✅ Done 2026-07-30. The three hot-path reads
      (`read_ale_gage`, `read_gcms_magnum`, and `tz_local_to_utc`, the last called once per
      monthly ALE/GAGE file) now go through `load_json`. `io_other_formats.py` reads the
      same file directly but is off the hot path (0% coverage) and was left untouched.

**Result (P1–P3 together):** `combine_datasets('agage_test','ch3ccl3','CGO')` **2.26 s → 1.90 s
(16% faster)** on repeated calls, same machine. Golden manifest (directory + zip) byte-identical
under `PYTHONHASHSEED` 0 and 12345. PR bundles the three as separate commits.
- [x] **P4 — Vectorise `drop_duplicates`.** ✅ Done 2026-07-31. The old loop did an O(n)
      `ds.sel(time=timestamp)` per duplicated timestamp — O(n²) overall. Replaced with a
      single priority sort (`time`, then real-over-NaN, then instrument priority, then
      position) plus `drop_duplicates("time", keep="first")`, then `isel` of the kept
      positions. Behaviour is identical, including the subtlety that an all-NaN group keeps
      its *first* row regardless of instrument (the priority key is zeroed for NaN rows).
      Five direct tests added first — pinned against the old code, then re-run against the
      new — including the all-NaN branch (was unexecuted, covers T5's duplicate case) and a
      check that extra data variables ride along. Micro-benchmark (6000 rows, 3000 dropped):
      **2185 ms → 5.6 ms (~390×)**; golden manifest (directory + zip) byte-identical.
- [x] **P5 — Stop calling `format_variables` two or three times per dataset.** ❌ WONTFIX
      (2026-07-31) — subsumed by P1. It is still called 5× per `combine_datasets`, but now
      costs **6 ms (0% of the call)**: P1's `load_json` cache removed the JSON re-reads that
      were its whole rationale. Removing the calls would save ~5 ms while touching
      output-defining code (Phase 4 S2 territory) — bad risk/reward. Profiling instead
      pointed at the real costs: the fixed-width ALE/GAGE file reads (~53%, addressed by P6)
      and the sympy calibration-scale conversion (~33%, logged as P7).
- [x] **P6 — Cache the raw per-instrument read shared across species and workflows.** ✅ Done
      2026-07-31. The originally-planned "split before `scale_convert`" boundary turned out
      non-uniform (`read_gcwerks_flask` bakes scale in via `format_attributes`/
      `format_variables`, not a trailing `scale_convert`), and profiling showed the dominant
      redundant cost is the ALE/GAGE *file read*, not resample. `read_ale_gage` opens a tar
      of monthly fixed-width files and builds a dataframe of **all species**; species
      selection happens only afterwards. Extracted that species-independent core into
      `_read_ale_gage_raw(network, site, instrument, utc)`, memoised (copy-on-return). It is
      shared across every species *and* across the individual/combined workflows, which each
      re-read the whole archive today. **Full `run_all` on `agage_test`: 65.0 s → 35.6 s
      (45%)**; golden manifest (directory + zip) byte-identical.
- [ ] **P7 — Cache the calibration-scale conversion (new, from P5 profiling).** ~33% of a
      `combine_datasets` call is `openghg_calscales.convert` rebuilding the sympy scale
      graph (`_scale_graph` → `nsimplify`/`pslq`) on every read. The derivation depends only
      on `(species, scale_from, scale_to)`, not the data, so it is cacheable in principle —
      but conversions can be **non-linear** (polynomial), so a scalar-factor cache is unsafe,
      and doing it properly means caching the library's private `_scale_graph` or an upstream
      contribution to `openghg_calscales`. Higher effort, dependency-bound; assess before
      committing.

**Target:** ≥40% reduction in wall time for a full `agage_test` run, byte-identical output
modulo the three volatile attributes. **Met by P6 alone (45%);** P1–P3 (config/JSON) and
P4 (drop_duplicates) compound on top.

---

## Phase 3 — Test coverage

- [x] **T1 — `run.py` unit tests** (29% → 87%, target 80%). ✅ Done 2026-07-30 in
      [tests/test_run.py](tests/test_run.py). Covers the release-schedule `"x"` skip; the
      unknown-species skip in `run_individual_instrument`; the single-instrument promotion
      to the top-level directory; the early-return deferral when a combined file already
      owns the top-level slot (`cfc-113` at CGO); the `top_level_only` conflict raise; the
      error-log path (a broken species is logged with its site/species/message while a
      healthy one in the same call is not); and the `run_all` isinstance validation wall
      (S3). Written ahead of S6, so that refactor can be checked against intended behaviour
      rather than only the byte-level manifest.
- [x] **T2 — Timestamp-mismatch tests** for `run_timestamp_checks` covering B2 and B3.
      ✅ Done 2026-07-29 in [tests/test_run.py](tests/test_run.py), along with targeted
      tests for the determinism fixes, `data_combination_species`, the contested
      top-level file error and the flask baseline skip. Writing them is what exposed the
      true severity of B2.
- [x] **T3 — `config.py` error-mode matrix.** ✅ Done 2026-07-30. Parametrised
      `{raise, ignore, ignore_inputs, ignore_outputs}` × `{zip, dir}` ×
      `{file present, absent}` and pinned the corrected behavior in `tests/test_config.py`.
      It remains the regression test for B4/B5.
- [x] **T4 — Schema test over `data/variables.json` and `attributes.json`.** ✅ Done
      2026-07-30. Every entry has
      `optional` ∈ {`"True"`, `"False"`}, `remove_flagged` ∈ {`"True"`, `"False"`, `"Zero"`},
      an `encoding.dtype` present in `nc4_types`, and a `resample_method` handled by
      `define_agg_dict` where applicable. Catches B8 and future typos.
- [x] **T5 — Non-monotonic and duplicate-timestamp input fixtures.** ✅ Done 2026-07-31.
      The in-memory non-monotonic `read_nc` case (B1) was already covered; the
      duplicate-timestamp cases for `drop_duplicates` landed with P4 (non-NaN preference,
      instrument priority, the all-NaN branch, and multi-timestamp/extra-variable
      preservation).
- [x] **T6 — `util.archive_to_csv` end-to-end.** ✅ Done 2026-07-31. `tests/test_util.py`
      builds a small real archive (one GCMS-Medusa species/site, no combined/baseline, so
      no slow ALE/GAGE reads), runs `archive_to_csv`, and checks each `.nc` gains a `.csv`
      counterpart and non-nc files are copied verbatim. Writing it exposed a crash: on a
      directory archive `data_file_list` yields trailing-slash subdirectory entries, which
      `archive_to_csv` fed to `read_text` — fixed by skipping them (see CHANGELOG). Zip
      archives (the usual release format) had no such entries, which is why 0% coverage let
      it hide.
- [x] **T7 — Input configuration consistency checks** (#97). ✅ Done 2026-07-31. #97 asked
      for a pre-flight *utility*, not just a test, so this is a new module
      `agage_archive/checks.py` with `check_input_files(network)`: species used in the
      release schedules and `data_combination` files must be defined in the network's
      `scale_defaults.csv` or `standard_names.json`; every date must parse; no end date may
      precede its start. It reports every problem at once, each naming the file and value.
      The underlying `check_*` helpers are pure (take parsed dataframes), so failure cases
      are unit-tested directly; an integration test asserts the real `agage_test` config is
      clean. `run_all` runs it by default (`check_inputs=True`) before touching the
      archive and aborts if any problem is found; pass `check_inputs=False` to skip.
      Covered in `tests/test_checks.py`.

---

## Phase 4 — Structural simplification

Highest risk to output format — do last, once Phases 0–3 are green.

- [ ] **S1 — Remove the `eval`/`locals()` dispatch.**
      [formatting.py:560-569](agage_archive/formatting.py#L560-L569) does
      `eval(f"format_{v}('{attrs[v]}')")` and takes `locals()` as an argument, coupling
      `format_attributes` and `format_variables` to each other's local variable *names*.
      Replace with a `{"species": format_species, ...}` dict and explicit arguments.
- [ ] **S2 — Split `format_attributes`.** One 130-line function doing default loading,
      network/site/site-species-instrument overlay merging, and derived-attribute
      computation, dispatched by substring-matching attribute names
      (`if "instrument" in attr`). [formatting.py:456-457](agage_archive/formatting.py#L456-L457)
      does `attrs[attr] += ...`, which `KeyError`s if the attribute is absent.
      **This function defines the output format — change it with the golden test watching.**
- [ ] **S3 — Replace the `isinstance` wall in `run_all`.**
      [run.py:487-518](agage_archive/run.py#L487-L518) — ~30 lines of hand-written type
      checks that will grow with every new feature. Use a validation table or a config
      dataclass.
- [ ] **S4 — Generalise reader-specific keyword threading.** `flask_pair_agreement` is
      passed through six call sites guarded by
      `read_function.__name__ == "read_gcwerks_flask"`
      ([io.py:1176](agage_archive/io.py#L1176), [run.py:117](agage_archive/run.py#L117)).
      This will not scale to the next feature — use a per-reader options dict, or have
      readers accept and ignore `**kwargs`.
- [ ] **S5 — Simplify the `errors=` contract in `config.py`** (see B4/B5), against the T3
      matrix.
- [ ] **S6 — Collapse the parallelisation residue in `run_all`.** The per-site/per-species
      split across `run_individual_instrument`/`run_individual_site` and
      `run_combined_instruments`/`run_combined_site` — each inner function processing one
      unit of work, returning a `(site, species, error)` tuple, the caller collecting the
      tuples into a list and reducing it to an error log — is the leftover shape of an
      abandoned attempt to map the inner function across a worker pool. Serially it is pure
      indirection: it is why `run_individual_site` must be handed `rs`, `read_function`,
      `read_baseline_function` and `instrument_out` as arguments instead of deriving them.
      Simplify the control flow and argument threading. Safe against the golden manifest,
      which pins output bytes and not call structure, **but gate on T1** — this refactor
      moves exactly the error-log and `top_level_only`-conflict paths that T1 is meant to
      cover, so write those tests first.

**Note — the two workflows are deliberately *not* merged.** Reading each source file twice
(once individual, once combined) is real but the two products differ in scale, exclusions
and date-windowing, and the real cost is config re-parsing. See decisions log 2026-07-30;
the shared raw read is addressed by P6, not by a structural merge here.

---

## Housekeeping

### Now

- [ ] Add CI — there is no `.github/workflows`. The suite runs in ~25 s; running it on
      every PR is the cheapest guarantee that the golden manifest keeps working.
- [ ] Add `pytest` and `pytest-cov` as test dependencies — neither is declared anywhere,
      despite `cfchecker` sitting in `requirements.txt` under a "for testing" comment.
      Folds into H3 below once packaging is migrated.

### Later — tooling and packaging

Deferred deliberately: none of this changes archive output, and doing it mid-refactor
would churn the diff. Tackle after Phase 2, or whenever the environment next needs
rebuilding.

- [ ] **H1 — Move from conda to [uv](https://docs.astral.sh/uv/).** Faster installs, a
      real lockfile, and reproducible environments across the downstream archive repos —
      which currently each stand up their own conda env by hand. Blocks on H3 (uv wants
      dependencies in `pyproject.toml`). Note that [AGENTS.md](AGENTS.md) and
      [docs/workflow.md](docs/workflow.md) both document the conda workflow and will need
      updating in the same change; the test suite currently assumes the `agage` env.

- [ ] **H2 — pre-commit hooks, including [gitleaks](https://github.com/gitleaks/gitleaks).**
      Defence in depth against committing credentials. The concrete motivation: a live
      Google Cloud **service-account private key** sits at the repository root as
      `agage-gdrive.json`. It has never been committed (verified against full history) and
      is ignored — but only by a literal filename rule at `.gitignore:136`. Rename it, copy
      it, or add a second service account and that protection is gone. `.dvc/config.local`
      is in the same position for the DVC remote credentials. Suggested hook set:
      - `gitleaks` — secret scanning on staged content
      - `check-added-large-files` — the `agage_test` fixture is committed to git on
        purpose, so the guard is against someone adding a *real* archive file by accident
      - `end-of-file-fixer`, `trailing-whitespace` — would have caught the missing trailing
        newline in `requirements.txt`
      - `check-json`, `check-yaml` — `data/variables.json` and the `attributes*.json` files
        are the archive schema; a malformed one currently fails deep inside a run
      - optionally `ruff` for lint, once the Phase 4 cleanups have landed

- [ ] **H3 — Retire `requirements.txt` in favour of `pyproject.toml`.** Three problems it
      would fix at once:
      - `pyproject.toml` declares **no `dependencies` at all**, so `pip install
        agage_archive` yields a package that cannot import. Downstream repos depend on this
        package, so the real dependency list living in an un-consumed `requirements.txt` is
        a genuine packaging bug, not just untidiness.
      - `requires-python = ">=3.7"` is stale. The code calls `DataFrame.map`, which needs
        pandas ≥ 2.1, which needs Python ≥ 3.9; the working environment is 3.11.
      - The version is duplicated between `pyproject.toml` and
        `agage_archive/__init__.py`. This one matters beyond tidiness: `__version__` is
        written into every output file as `processing_code_version`, so the two drifting
        apart mislabels the archive. Use a single source of truth
        (`[tool.setuptools.dynamic]` or `importlib.metadata`).

      Move `cfchecker`/`pytest`/`pytest-cov` into an optional `test` extra or a
      `[dependency-groups]` entry rather than a comment block.

---

## Decisions log

Record anything that changes the plan, especially anything touching output format.

| Date | Decision |
| --- | --- |
| 2026-07-28 | Plan created. Output format frozen; changes require an explicit decision recorded here. |
| 2026-07-29 | Determinism treated as a bug, not a format change: instrument order in `run_all` and `read_data_combination` now sorted / column-ordered. Combined-file global attributes may differ from previously published archives, but are now reproducible. |
| 2026-07-29 | `agage_test` release schedules trimmed to match available input data (data-less cells marked `x`) so a full run is completely determined and error-free. Schedules now document the fixture, not the real network. |
| 2026-07-29 | Picarro-1/Picarro-2 fixtures dropped — the code paths they would cover are already covered. |
| 2026-07-29 | Combined-file global attributes now come from the most recently operating instrument (#167). Follow-on attribute work tracked as Phase 1b together with #169. |
| 2026-07-29 | Version bumped to 0.3.0. This branch changes published output, and `processing_code_version` is written into every file, so archives built from it must not claim to be 0.2.1. |
| 2026-07-29 | B2 fixed after targeted tests showed the check was not merely weak but completely dead. Enabling it changes no output; it only means a genuine data/baseline mismatch now fails instead of being published. |
| 2026-07-29 | B12 (`start_date` too early in individual-instrument files) was approved for a later PR. Preferred approach: compute the date attributes at write time in `output_dataset` rather than in each reader. |
| 2026-07-30 | B12 fix approved for landing: recompute `start_date` and `end_date` in `output_dataset` after final filtering. This intentionally changes published attributes on affected files; regenerate the golden manifest after review. |
| 2026-07-30 | **Reviewed `run_all`/`combine_datasets` structure.** Two issues confirmed. (1) The per-site/per-species split and `(site, species, error)` tuple threading in `run_all` and the `run_*_instruments` functions is residue from an abandoned parallelisation attempt — added as S6 (Phase 4, gated on T1). (2) Each source file is read twice (individual + combined workflows), but the two products legitimately differ (instrument-specific vs `"combined"` scale, the extra `combined=True` exclusion, `data_combination` date-windowing) and the dominant cost is config re-parsing, not raw I/O. **Merging the two workflows was considered and rejected** — high output-risk for little gain, and it would also have to preserve the combined-before-individual ordering that the top-level-file existing check depends on. The genuinely shared work (raw read+resample, before the products diverge) is captured as a caching item, P6 (Phase 2), not a structural merge. |
| 2026-07-31 | **A1 (#169) — combined data owners are the union across contributing instruments.** Chosen representation: keep `data_owner`/`data_owner_email` as the existing `", "`-separated string (not a netCDF list), de-duplicated on (name, email) pairs, ordered by the `data_combination` file. Rationale: a single instrument already comma-joins multiple people, so this extends the existing convention without changing attribute encoding or risking netCDF round-trip / manifest churn; `nc_to_csv`'s comma→semicolon rewrite then applies uniformly. Changes published owner attributes on real combined files; no change in the fixture (uniform owners), so the rule is pinned by a unit test. |
| 2026-07-31 | **A2 (#168) — combined baseline-flags files report full instrument provenance.** Chosen to mirror the combined mole fraction file: `instrument`/`instrument_1…n` + joined `instrument_type`, built with `format_attributes_global_instruments`. Because instrument install dates are absent from the baseline inputs, `instrument_date*` uses each instrument's first baseline timestamp within its `data_combination` window rather than the install date the MF file carries — a deliberate divergence. Changed 7 combined baseline files in the manifest; manifest regenerated. |
| 2026-07-31 | **A3/#175 — combined comments de-duplicate identical instrument comments.** Distinct comments are still enumerated in `data_combination` order; identical ones are listed once. No fixture change (the CGO instrument comments differ); pinned by a unit test. Audit otherwise found no further combined-attribute errors (`calibration_scale` single-valued is correct; `inlet_*` handled by #167). |
| 2026-07-29 | **Contested top-level files now raise.** If more than one instrument is eligible to write `{species}/` because the species has no row in the site's `data_combination` file, processing fails with an error naming the species and site, rather than letting run order decide. Sites in the real archive have already been corrected by hand; this makes a regression impossible to miss. |

### Open questions

- ~~**Which instrument should supply a combined file's global attributes?**~~ ✅ Resolved
  2026-07-29 (#167). Follow-on work is now tracked as Phase 1b (#167, #169).

- ~~**Should a contested top-level file warn?**~~ ✅ Resolved 2026-07-29: it now raises.
  See "contested top-level files" below.

- ~~**B12 — `start_date` is too early in individual-instrument files.**~~ ✅ Decision taken
  2026-07-29: this **will** be fixed, but not in the current PR — see B12 in Phase 1.
  All four readers call `format_attributes` *before* removing NaN mole fractions
  ([io.py:269](agage_archive/io.py#L269), [643](agage_archive/io.py#L643),
  [920](agage_archive/io.py#L920), [1111](agage_archive/io.py#L1111), with
  `dropna` at [314](agage_archive/io.py#L314), [677](agage_archive/io.py#L677),
  [950](agage_archive/io.py#L950), [1141](agage_archive/io.py#L1141)), so
  `start_date` records the start of the *source record* rather than of this species'
  measurements. Worst case in the fixture: GAGE `ch4` at CGO claims
  `1981-11-30 14:01` when its first CH4 measurement is `1986-05-13 03:54` — **4.4 years**
  early. 15 of 25 individual files are affected.

  Only `start_date`. `end_date` is correct because `output_dataset` recomputes it
  ([io.py:1491](agage_archive/io.py#L1491)) whenever an end date is passed, which
  `run_individual_site` always does. There is no equivalent recomputation for
  `start_date`.

  Combined files are correct, but only incidentally: `combine_datasets` calls
  `format_attributes` after the contributing datasets have each been read with
  `dropna=True`, so the concatenated time axis already starts at a real measurement. With
  `dropna=False` the combined files would be wrong too.

  Worth fixing at the same point as any of the resample/attribute refactors. Changes an
  attribute on ~618 files in the real archive, so it needs a decision first.
