# STATUS

Working plan for the code-quality pass on `agage-archive`. Read
[AGENTS.md](AGENTS.md) first — in particular the rule that output files and archive
structure must not change.

**Started:** 2026-07-28
**Last updated:** 2026-07-30

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

## Phase 0 — Safety net (#38) ✅ MOSTLY DONE (2026-07-29)

- [x] **Golden archive manifest test** — `tests/test_archive.py`, reference at
      `tests/reference/archive_manifest.json` (127 entries). Pins per-file paths,
      variables, dtypes, encoding, variable and global attributes, `n_time`, and a data
      checksum rounded to 6 significant figures (float32 carries ~7, so this absorbs
      last-bit noise between library versions while still catching real changes).
      Regenerate with `AGAGE_UPDATE_MANIFEST=1 python -m pytest tests/test_archive.py`.
- [x] **Assert the error logs are empty** — `test_archive_has_no_errors`.
- [x] **`conftest.py` fixture** isolating `data/agage_test/output`; `test_cf_compliance`
      no longer deletes the output tree as a side effect.
- [ ] **Run the golden test against zip output as well as directory output.** Still
      outstanding — these take different code paths through `config.py` and only the
      directory path is covered. Needs the `errors=` behaviour pinned first (T3/B4/B5),
      since the zip path is where `data_file_path` returns `None`.

**Exit criteria met:** perturbing one character of a `long_name` in `data/variables.json`
produces 84 manifest differences and fails the suite.

**Suite:** 46 passed, ~2.5 min (the archive test is ~1 min; deselect with `-m "not slow"`).
**Coverage:** 67% → 73% overall; `run.py` 29% → 72%.

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

- [ ] **A1 — Data owners must be the union across contributing instruments** (#169).
      `data_owner` and `data_owner_email` are currently single-valued and taken from one
      instrument, so a combined record silently credits one owner and drops the others.
      These need to become a de-duplicated list of every owner whose data is in the file,
      order-stable. Decide the delimiter and make sure it survives the netCDF attribute
      round-trip and the CSV archive (`nc_to_csv` in [util.py](agage_archive/util.py)
      rewrites commas to semicolons, so a comma-delimited list would be mangled there).

- [ ] **A2 — The combined baseline-flags file misreports its provenance** (#168). Pre-existing,
      exposed by #167. `combine_baseline` inherits one instrument's attributes, so the
      combined CGO `ch3ccl3` baseline file says `instrument: GCMS-Medusa` and
      `instrument_type: GCMS-Medusa` even though it spans ALE+GAGE+GCMD+Medusa. The
      combined *mole fraction* file does this correctly — `format_attributes` gives it
      `instrument` … `instrument_4` and `instrument_type: ALE/GAGE/GCMD/GCMS-Medusa`.
      Before #167 it said `ALE`; now it says `GCMS-Medusa`. Both are wrong.

- [ ] **A3 — Audit every other global attribute for the same class of error.** Work
      through `data/attributes.json` and decide, per attribute, whether a combined file
      should take the most recent value, the union, or something computed. Candidates
      that look wrong today: `doi`, `citation`, `contact`, `comment` (#175: currently
      concatenated without de-duplicating identical instrument comments),
      `calibration_scale` (already validated as identical across instruments, so
      single-valued is correct).

- [ ] **A4 — Pin the decisions in a test.** Once A1–A3 land, the golden manifest will
      capture the values, but add an explicit test asserting the *rule* (e.g. that the
      combined CGO `ch3ccl3` file lists every contributing instrument's data owner) so
      the intent survives a future manifest regeneration.

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
- [ ] `open_data_file` returns a handle from a closed `ZipFile`; works only via
      `ZipExtFile` refcounting
- [x] Leading-slash zip member when `output_subpath=""`. ✅ Fixed 2026-07-30. ZIP output
      now normalizes empty or slash-terminated subpaths without creating a leading slash.
- [ ] Zip `"a"` mode creates duplicate members if a run is not preceded by `delete_archive`
- [ ] `if "time" in var` should be `var == "time"` — [data_selection.py:269](agage_archive/data_selection.py#L269)
- [ ] Instrument partial-matching uses dict insertion order, not longest match —
      [definitions.py:165-170](agage_archive/definitions.py#L165-L170)
- [ ] `choose_scale_defaults_file` breaks ties using `data_file_list` order, which is
      filesystem glob order. Not currently triggered (only one file matches any given
      instrument/site), but it is the same class of non-determinism fixed in Phase 0a —
      [data_selection.py:155-171](agage_archive/data_selection.py#L155-L171)
- [ ] `site_code` casing differs between readers: `read_nc` upper-cases
      ([io.py:261](agage_archive/io.py#L261)), `read_ale_gage`
      ([io.py:634](agage_archive/io.py#L634)) and `read_gcwerks_flask`
      ([io.py:1093](agage_archive/io.py#L1093)) do not — so a lowercase site code silently
      skips `attributes_site.json` lookup for ALE/GAGE

---

## Phase 2 — Performance

All of this is provably output-preserving once Phase 0 is in place, which makes it the
best-value work in the plan.

- [ ] **P1 — Cache config and JSON loads.** `Paths()` walks the tree looking for `.git` and
      re-parses `config.yaml` on every construction: 373 constructions and 343 YAML parses
      per `combine_datasets` call, 1.28 s of 5.27 s. Caching *only* the config YAML parse,
      changing nothing else, measured **2.45 s → 1.87 s (24% faster)** with identical
      output. Extend to `variables.json`, `attributes.json`, `standard_names.json`,
      `variables_not_public.json`.
- [ ] **P2 — Cache `define_instrument_number`.** Re-lists a directory on every call, and is
      called per-dataset and per-variable via `instrument_type_definition`.
- [ ] **P3 — Cache `ale_gage_sites.json`.** Read 243 times in a single `combine_datasets`
      call, once per monthly ALE/GAGE file via `tz_local_to_utc`.
- [ ] **P4 — Vectorise `drop_duplicates`.** [io.py:87-113](agage_archive/io.py#L87-L113)
      does an O(n) `ds.sel(time=timestamp)` inside a loop over duplicated timestamps.
      Replace with a pandas groupby on `(time, instrument_type)` priority. Cover the
      all-NaN branch ([io.py:96](agage_archive/io.py#L96)) with a test *first* — it is
      currently unexecuted.
- [ ] **P5 — Stop calling `format_variables` two or three times per dataset.** It rebuilds
      the whole Dataset and re-reads three JSON files each time (`read_nc`, then
      `combine_datasets`).

**Target:** ≥40% reduction in wall time for a full `agage_test` run, byte-identical output
modulo the three volatile attributes.

---

## Phase 3 — Test coverage

- [ ] **T1 — `run.py` unit tests** (29% → target 80%): the release-schedule `"x"` skip; the
      `top_level_only` conflict raise ([run.py:146](agage_archive/run.py#L146)); the
      single-instrument early return ([run.py:168](agage_archive/run.py#L168)); and the
      error-log path — deliberately break one species and assert it lands in the log with a
      useful message.
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
- [ ] **T5 — Non-monotonic and duplicate-timestamp input fixtures.** The in-memory
      non-monotonic `read_nc` case (B1) is covered; the duplicate-timestamp case for
      `drop_duplicates` (P4) remains outstanding.
- [ ] **T6 — `util.archive_to_csv` end-to-end.** 0% covered and it produces a published
      archive.
- [ ] **T7 — Input configuration consistency checks** (#97). Validate that species names
      agree across release schedules, scale-default files and `standard_names.json`; check
      that configured dates parse successfully and that end dates are not before start
      dates. Cover failures with messages that identify the offending file and value.

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
