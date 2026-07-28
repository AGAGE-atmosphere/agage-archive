# STATUS

Working plan for the code-quality pass on `agage-archive`. Read
[AGENTS.md](AGENTS.md) first — in particular the rule that output files and archive
structure must not change.

**Branch:** `improved-tests`
**Started:** 2026-07-28
**Last updated:** 2026-07-28

Update the checkboxes and the "Last updated" date as items land. Keep the findings
register at the bottom in sync — if an item turns out to be a non-issue, mark it
`WONTFIX` with a one-line reason rather than deleting it.

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

## Phase 0 — Safety net (do this first)

Nothing else in this plan is safe without it. The constraint is "don't change the output",
and there is currently no test that would notice if we did.

- [ ] **Golden archive manifest test.** Run `run_all` on `agage_test` and assert a
      checked-in manifest: sorted list of archive paths, and for each netCDF file its
      variable names, dtypes, `_FillValue`s, units, and global/variable attributes.
      Exclude only `file_created`, `file_created_by`, `processing_code_version`.
- [ ] **Run the golden test against both output modes** — directory output and `.zip`
      output. These take different code paths through `config.py` and only the directory
      path is currently exercised end-to-end.
- [ ] **Assert the error logs are empty** after the golden run. `error_log_individual.txt`
      and `error_log_combined.txt` must not exist. Without this, a swallowed exception
      shows up as a silently smaller archive and a green test suite.
- [ ] **`conftest.py` fixture to isolate `data/agage_test/output` per test.**
      `test_cf_compliance` currently deletes the output tree as a side effect, making the
      suite order-dependent.

**Exit criteria:** a deliberate one-character change to `data/variables.json` turns the
suite red.

---

## Phase 1 — Confirmed bugs

Each gets a failing test first, then the fix. Items 1–3 are the ones that can corrupt or
silently mis-align published data.

- [ ] **B1 — `sortby(inplace=True)` raises.** [io.py:307-308](agage_archive/io.py#L307-L308).
      `Dataset.sortby()` has no `inplace` argument, and the result is discarded anyway, so
      the non-monotonic branch is dead code that crashes when reached. Fix:
      `ds = ds.sortby("time")`. Test: a fixture with shuffled timestamps.
- [ ] **B2 — data/baseline timestamp check silently passes on length mismatch.**
      [run.py:64-66](agage_archive/run.py#L64-L66) and [run.py:177](agage_archive/run.py#L177).
      `(ds_baseline.time != ds.time).any()` aligns with an inner join, so extra or missing
      baseline timestamps compare only over the intersection and the check returns `False`.
      Fix: compare lengths, then `np.array_equal` on `.values`.
- [ ] **B3 — `if ds_baseline:` is Dataset truthiness.** [run.py:59](agage_archive/run.py#L59),
      [run.py:64](agage_archive/run.py#L64). `bool(Dataset())` is `False`, so an empty
      baseline dataset skips every check instead of failing. Fix: `is not None`.
- [ ] **B4 — `data_file_path` returns `None` silently.**
      [config.py:335-340](agage_archive/config.py#L335-L340). Zip + missing member +
      `errors="ignore"` returns `None`; callers fail later with an unrelated
      `AttributeError`. The directory branch with `errors="raise"` returns a path to a
      non-existent file *without* raising — the two branches disagree about what `errors`
      means. Fix alongside B5.
- [ ] **B5 — `errors=` is four undocumented magic strings.** `raise`, `ignore`,
      `ignore_inputs`, `ignore_outputs`; the `Paths` docstring says the default is `raise`
      when it is `ignore`; [config.py:261](agage_archive/config.py#L261) uses substring
      matching (`"ignore" in errors`) which matches all three ignore variants. Pin current
      behaviour with the Phase 3 matrix test *before* simplifying.
- [ ] **B6 — unbound local in `read_release_schedule`.**
      [data_selection.py:97-103](agage_archive/data_selection.py#L97-L103). `pos` is only
      assigned inside the comment-scanning loop, so a schedule file whose first line is not
      a comment raises `UnboundLocalError` at `f.seek(pos)`. Latent today; a trap for new
      networks.
- [ ] **B7 — species case mismatch between validation and lookup.**
      [data_selection.py:122-131](agage_archive/data_selection.py#L122-L131) and
      [data_selection.py:197-200](agage_archive/data_selection.py#L197-L200) validate with
      `format_species(species)` then index with the raw `species`. Works only because all
      current callers pre-format.
- [ ] **B8 — `instrument_type` is accidentally optional.** `data/variables.json` has
      `"optional": ""`, and [formatting.py:261](agage_archive/formatting.py#L261) treats
      anything other than the exact string `"False"` as optional — so a missing
      `instrument_type` is silently dropped rather than raising. Covered by the Phase 3
      schema test.
- [ ] **B9 — flask + baseline fails via the generic handler.**
      [run.py:249-250](agage_archive/run.py#L249-L250) sets `read_baseline_function = None`
      for `GCMS-MEDUSA-FLASK`, then [run.py:123](agage_archive/run.py#L123) calls it
      unconditionally. Fix: explicit skip.
- [ ] **B10 — `warnings.simplefilter` used as a global toggle.**
      [formatting.py:275-280](agage_archive/formatting.py#L275-L280) clobbers the caller's
      warning configuration and does not restore it on exception. Fix:
      `with warnings.catch_warnings():`.
- [ ] **B11 — module-level filesystem walk at import.**
      [io_other_formats.py:11](agage_archive/io_other_formats.py#L11) runs `Paths()` at
      import time and can raise on import; [line 52](agage_archive/io_other_formats.py#L52)
      uses `delim_whitespace=True`, removed in pandas 3.0. 0% coverage — decide whether to
      test it or delete it.

### Minor (batch into one commit)

- [ ] `fnmatch.filter` shadows the builtin and raises `IndexError` rather than
      `FileNotFoundError` on no match — [config.py:384](agage_archive/config.py#L384)
- [ ] `open_data_file` returns a handle from a closed `ZipFile`; works only via
      `ZipExtFile` refcounting
- [ ] Leading-slash zip member when `output_subpath=""` — [io.py:1402](agage_archive/io.py#L1402)
- [ ] Zip `"a"` mode creates duplicate members if a run is not preceded by `delete_archive`
- [ ] `if "time" in var` should be `var == "time"` — [data_selection.py:269](agage_archive/data_selection.py#L269)
- [ ] Instrument partial-matching uses dict insertion order, not longest match —
      [definitions.py:165-170](agage_archive/definitions.py#L165-L170)
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
- [ ] **T2 — Timestamp-mismatch tests** for `run_timestamp_checks` covering B2 and B3.
- [ ] **T3 — `config.py` error-mode matrix.** Parametrise
      `{raise, ignore, ignore_inputs, ignore_outputs}` × `{zip, dir}` ×
      `{file present, absent}` and pin the current behaviour. Prerequisite for B4/B5.
- [ ] **T4 — Schema test over `data/variables.json` and `attributes.json`:** every entry has
      `optional` ∈ {`"True"`, `"False"`}, `remove_flagged` ∈ {`"True"`, `"False"`, `"Zero"`},
      an `encoding.dtype` present in `nc4_types`, and a `resample_method` handled by
      `define_agg_dict`. Catches B8 and every future typo.
- [ ] **T5 — Non-monotonic and duplicate-timestamp input fixtures** for `read_nc` (B1) and
      `drop_duplicates` (P4).
- [ ] **T6 — `util.archive_to_csv` end-to-end.** 0% covered and it produces a published
      archive.

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

- [ ] Remove stray untracked files: `agage_archive/config copy.yaml`,
      `data/agage_test/data-gcms-flask-nc/cf4_air.nc.BACKUP`
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
      - `check-added-large-files` — data belongs in dvc, not git
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
