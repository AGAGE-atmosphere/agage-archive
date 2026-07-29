# AGAGE test archive

This is **not** a real data archive. It is the output of the `agage_test` network, a
synthetic fixture used by the `agage-archive` test suite, and it contains a small,
deliberately chosen subset of AGAGE data. Do not use it for science.

This file exists so that `copy_to_archive` is exercised by a full `run_all`, mirroring
the README that is shipped at the root of the real public archive.

## Structure

```
<species>/                                        combined (recommended) files
<species>/baseline-flags/                         baseline flags for the combined file
<species>/monthly-baseline/                       monthly baseline means
<species>/individual-instruments/                 one file per instrument
<species>/individual-instruments/baseline-flags/
<species>/individual-instruments/monthly-baseline/
```

## What the fixture spans

| Reader | Instruments | Sites |
| --- | --- | --- |
| `read_ale_gage` | ALE, GAGE | CGO |
| `read_nc` | GCMD, GCMS-Medusa, Picarro | CGO, MHD, TAC, THD |
| `read_gcms_magnum` | GCMS-Magnum | MHD |
| `read_gcwerks_flask` | GCMS-Medusa-flask | CBW |

Every live cell in `data_release_schedule/` has input data behind it, so a full run
produces a completely determined archive and writes no error log. If you add a site or
species to a release schedule, add the corresponding input file too — otherwise the run
will fail silently into `error_log_individual.txt`.
