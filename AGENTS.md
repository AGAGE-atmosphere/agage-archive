# AGENTS.md

Guidance for AI agents (and humans) working in this repository.

`agage-archive` produces the AGAGE public archive: netCDF files describing atmospheric
trace-gas mole fractions, plus baseline flags and monthly baseline products. The package
is also used as a dependency by several downstream archive repositories (see
[template-archive](https://github.com/mrghg/template-archive)), which supply their own
`data/<network>/` directory and call into this code.

## Prime directive: the output format is frozen

**Do not change the format of output files or the structure of the archive.**

The only acceptable reason to change either is a demonstrated bug — a case where the
current output is objectively wrong (mis-stated units, a variable that doesn't match its
metadata, a corrupted timestamp). "Cleaner", "more consistent", "more modern" and "the
new code would be simpler if we did" are **not** sufficient reasons.

If you believe you have found such a bug:

1. Stop and describe it, with the specific input that produces the wrong output.
2. Wait for a human decision. Do not fix it as a side effect of other work.
3. If approved, change it in its own commit, with a `CHANGELOG.md` entry, so downstream
   repositories can see exactly what moved.

Refactoring is expected and encouraged. Refactoring that changes a byte of the output
without an explicit decision is a regression, however tidy the diff looks.

### What "the output format" concretely means

Four things, all of which must be preserved:

| Aspect | Defined by |
| --- | --- |
| Filenames | `output_path()` in [config.py](agage_archive/config.py) — `{network}{-instrument}_{site}_{species}_{extra}{version}.nc` |
| Directory layout | [run.py](agage_archive/run.py) — `{species}/`, `{species}/individual-instruments/`, `{species}/baseline-flags/`, `{species}/monthly-baseline/` |
| Variables, dtypes, `_FillValue`, encoding | `data/variables.json`, applied by `format_variables()` in [formatting.py](agage_archive/formatting.py) |
| Global and variable attributes | `data/attributes.json` + per-network `data/<network>/attributes.json` + `attributes_site.json` + `attributes_site_species_instrument.json`, applied by `format_attributes()` |

`data/variables.json` and the `attributes*.json` files are effectively the archive's
schema. Treat edits to them with the same care as edits to the output itself.

Three attributes are expected to vary between runs and are the only ones excluded from
output comparisons: `file_created`, `file_created_by`, `processing_code_version`.

### The golden manifest is how this is enforced

`tests/test_archive.py` runs the full `agage_test` archive and compares every file against
`tests/reference/archive_manifest.json` — paths, variables, dtypes, encoding, attributes
and a data checksum — and asserts that no errors were written to the error logs.

**If that test fails, you have changed the output.** Do not regenerate the reference to
make it pass. Read the diff, work out which change caused it, and either fix the change or
take it to a human as described above. Regenerating is only correct once someone has
decided the new output is right:

```bash
AGAGE_UPDATE_MANIFEST=1 python -m pytest tests/test_archive.py
```

The archive must also be **reproducible**: two runs must produce identical output. Beware
of anything whose order depends on the filesystem or on Python's hash seed — `set()`
iteration and unsorted `glob` results have both caused non-reproducible archives here.

## Environment

Use the `agage` conda environment:

```bash
conda activate agage
python -m pytest -q                                  # full suite, ~25 s
python -m pytest -q --cov=agage_archive --cov-report=term-missing
```

**Do not install, upgrade, or remove packages or otherwise modify the conda environment.**
Until this repository moves to `uv`, dependency changes are handled manually by the
maintainer. If a command fails because a package is missing or incompatible, stop and
report the exact package name and any known version constraint so the maintainer can
update the `agage` environment. Do not run `conda install`, `pip install`, or equivalent
commands, and do not work around a missing dependency by changing project code.

The package needs `agage_archive/config.yaml`, which is not in version control. Create it
with `python agage_archive/config.py` if it is missing. Tests run against the `agage_test`
network in `data/agage_test/`.

## Working practices

- **Make changes on a dedicated branch and submit them through a pull request.** Before
  editing, confirm that the current branch is not `main`; if it is, create a new,
  descriptively named branch from the latest `main`. Keep unrelated tasks on separate
  branches. When the work is complete, commit it, push the branch, and open an associated
  GitHub pull request with the changes and test results. Do not commit directly to `main`
  or merge the pull request unless explicitly asked.
- **Run the tests before and after every change.** The suite is fast; there is no excuse
  for skipping it. Report failures with the actual output rather than summarising.
- **Add the test first when fixing a bug.** Several bugs in this codebase sat in
  uncovered branches (`io.py:308`, `io.py:96`). A fix without a test is likely to be
  re-broken by the next refactor.
- **Know what belongs in `data/`.** In *this* repository the `agage_test` fixture is
  committed to git, small netCDF and tarball inputs included — that is deliberate, it is
  what makes the test suite self-contained. What must never be committed is processed
  output (`data/agage_test/output/` is gitignored) or real archive data. The downstream
  release repositories are the ones that track their data with [dvc](https://dvc.org);
  see [docs/workflow.md](docs/workflow.md). Keep new fixtures small, and prefer
  generating synthetic inputs in code over adding binary files.
- **Never commit credentials.** `agage-gdrive.json` (a Google Cloud service-account
  private key) and `.dvc/config.local` live in the working tree and are protected only by
  literal filename rules in `.gitignore`. Do not `git add -f` them, do not copy or rename
  them into a path that is not ignored, and do not paste their contents anywhere. The same
  goes for `agage_archive/config.yaml`, which contains local paths and a user name.
- **Update `CHANGELOG.md`** for anything a downstream repository would notice: new or
  renamed keyword arguments, new required files in `data/<network>/`, changed error
  behaviour. Internal refactors that change nothing observable do not need an entry.
- **Keep the public function signatures stable.** Downstream repos call
  `run_all`, `run_individual_instrument`, `run_combined_instruments`, and the `read_*`
  functions directly. Adding keyword arguments with defaults is fine; removing or
  reordering arguments is a breaking change.
- **New networks are configured by data, not code.** Anything instrument- or
  site-specific belongs in `data/<network>/` (`data_read_functions.json`,
  `data_release_schedule/`, `data_combination/`, `data_exclude/`, `scale_defaults*.csv`),
  not in a conditional in `io.py` or `run.py`.

## Code conventions

Match the surrounding code rather than importing a different house style:

- Google-style docstrings with `Args:` / `Returns:` / `Raises:` on every public function.
- `xarray` for datasets, `pandas` for tabular intermediates, `numpy` for arrays.
- Errors are raised as `ValueError`/`FileNotFoundError`/`KeyError` with messages that name
  the species, site and instrument involved — these end up in the error logs and are the
  main debugging aid for a full archive run.
- Comment density is moderate; explain *why*, not *what*.

## Known traps

- `run_individual_site` and `run_combined_site` wrap everything in `except Exception` and
  write to `error_log_individual.txt` / `error_log_combined.txt`. **A failure produces a
  silently missing file, not a crash.** After any run, check those logs — a passing test
  suite plus a truncated archive is the characteristic failure mode here.
- The `errors=` argument threaded through `config.py` takes four magic strings
  (`raise`, `ignore`, `ignore_inputs`, `ignore_outputs`), the docstring is out of date, and
  one check uses substring matching. Read the code, not the docstring, until this is fixed.
- `format_attributes()` and `format_variables()` pass `locals()` into
  `lookup_locals_and_attrs()`, which `eval`s a formatter name. Renaming a local variable in
  those functions can silently change output.
- Reading a config value or a JSON file is not cheap here: a single `combine_datasets()`
  call currently re-parses `config.yaml` 343 times. Prefer caching to re-reading.

## See also

- [STATUS.md](STATUS.md) — current work plan and progress
- [docs/workflow.md](docs/workflow.md) — how a release is actually produced
- [docs/flasks.md](docs/flasks.md), [docs/ale_gage_notes.md](docs/ale_gage_notes.md) — data-specific processing notes
