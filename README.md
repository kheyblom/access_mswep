# access_mswep

Download raw MSWEP daily netCDF files from the shared GloH2O Google Drive folder onto
GLADE, with rclone. Structured like `access_gleam`: one script driven by one YAML
config, a thin `utils/` layer, per-worker log files, and a run that is safe to repeat.

```
mswep_download.py            the downloader
mswep_search.ipynb           scratch notebook for inspecting the Drive folder
config/config_download.yaml  what to download and where to put it
config/config_download_nrt.yaml   the V2.8 near real time daily record
config/config_download_tiny.yaml  same, limited to 1979, for a smoke test
utils/                       config loading, logging, rclone wrappers
run_download.sh              start the download on a login node
job_download.pbs             fallback batch job
```

## One time setup

rclone needs an OAuth token for your Google account, and Drive's OAuth flow needs a
browser, which a login node does not have. The trick is to let rclone run its callback
server here and reach it from a browser at home over an SSH port forward, so the token
is written straight into the config and never has to be copied anywhere.

**1. Make your own Drive client ID.** rclone's built-in one is shared by every rclone
user and is throttled hard; a job this size will crawl or start returning 403s on it.
In the [Google Cloud console](https://console.cloud.google.com): new project, enable
the **Google Drive API**, add yourself as a test user on the OAuth consent screen, then
**Credentials -> Create credentials -> OAuth client ID -> Desktop app**.

**2. Forward rclone's callback port to your machine.** Derecho's login nodes sit behind
a round-robin address, so forward to the node you are actually on -- `hostname -f` says
which -- and not to `derecho.hpc.ucar.edu`, or the forward will land somewhere else:

```bash
ssh -N -L 53682:localhost:53682 <user>@128.117.211.170   # 128.117.211.170 is derecho1
```

In VS Code it is simpler to reuse the connection you already have: `Forward a Port`
from the command palette, port `53682`.

**3. Create the remote.**

```bash
rclone config
#  n) New remote
#  name>                      drive
#  Storage>                   drive
#  client_id>                 <from step 1>
#  client_secret>             <from step 1>
#  scope>                     2          (drive.readonly)
#  Edit advanced config?      n
#  Use web browser ... ?      y          <- rclone listens on 127.0.0.1:53682
#  Configure as Shared Drive? n
chmod 600 ~/.config/rclone/rclone.conf
```

rclone will report that it cannot open a browser and print a
`http://127.0.0.1:53682/auth?state=...` URL. Open that on your own machine; the forward
carries it to rclone here. Approving it shows `Success!` in the terminal.

**4. Find the real folder name** and put it in `config/config_download.yaml` as
`rclone.root`. Do not assume `MSWEP_V280`; GloH2O renames this folder between versions.

```bash
rclone lsd drive: --drive-shared-with-me
rclone lsd drive:<folder> --drive-shared-with-me
```

**5. Install the environment:** `uv sync`.

### Two things that will bite later

While the OAuth consent screen's publishing status is **Testing**, Google expires the
refresh token after **7 days** and rclone starts failing with `invalid_grant`. Fix it
with `rclone config reconnect drive:` (same browser forward as above), or set the
publishing status to **In production** to stop it happening. A single download run is
well inside the window; a habit of re-running monthly is not.

If the share lives in a **Shared Drive** rather than in *Shared with me*, drop
`shared_with_me` to `false` in the config and point `rclone.root` at the drive instead;
`--drive-shared-with-me` and Shared Drives are different namespaces and the wrong one
lists nothing without erroring.

## Running

```bash
# what would be downloaded, no transfers
uv run python mswep_download.py --config config/config_download.yaml --dry-run

# a single year into a separate directory, as a smoke test
./run_download.sh config/config_download_tiny.yaml

# the full record
./run_download.sh

# V2.8 near real time daily, 2020-11-27 onwards
./run_download.sh config/config_download_nrt.yaml
```

`run_download.sh` starts the download under `setsid`, in a session of its own, so
it survives logout and anything that kills the starting shell's process group --
`nohup` alone only blocks SIGHUP and is not enough. It runs at `nice -n 19` so it
yields to anything interactive on the shared node.

Once enumeration finishes the run records its host and pid in
`logs/<log_file stem>.pid`, and deletes that file when it ends. The host matters:
Derecho's login nodes share GLADE but not their process tables, so a run started
on `derecho1` is invisible from `derecho4` and looks dead to `ps` there.

```bash
./run_download.sh --status              # host, pid, whether it is alive
tail -n 40 logs/mswep_download.log      # main log: totals, verification, failures
ls logs/                                 # one log per worker, with rclone's own lines
```

`--status` prints the ssh command to check or stop a run that is on another node.
Stopping is always safe: `kill -INT <pid>` sweeps partial files on the way out and
a rerun picks up exactly what is missing. Starting a second run over the same
download directory is refused while the first is alive, since the two would race
on the same partial files.

Note the logs append across runs, so `grep` for `done :-)` will match older runs
too -- use the pid file, not the log, to tell whether something is in progress.

A download is network bound rather than CPU bound: the workers spend nearly all
their time waiting on Google, and Drive's per-user request limit caps the rate well
below anything a login node would notice. If the transfer still needs to be quieter,
set `rclone.bwlimit` in the config (for example `bwlimit: 20M`) rather than cutting
`n_processes`, which would only make the run longer without reducing its peak load.

`job_download.pbs` is a fallback for the case where a login-node run keeps getting
killed. It targets Casper, because Derecho compute nodes have no outbound internet
and rclone cannot reach Google from one.

## Periods and resolutions

A product is a period and a temporal resolution, and V2.8 offers `Past`, `Past_nogauge`
and `NRT` crossed with `3hourly`, `Daily` and `Monthly`. `Past` ends where `NRT` begins:
the near real time record starts at 2020-11-27 and is extended daily, so the two
together cover 1979 to now. A file is named for the period it covers -- `YYYYDOY.nc`
daily, `YYYYDOY.HH.nc` three hourly, `YYYYMM.nc` monthly -- and every form is understood
when the year is read off for filtering and for the year directories.

Downloading a resolution is a matter of listing it in `products`; note that the full NRT
three hourly record is around 450 GB against the daily record's 13 GB.

## How it works

`lsjson` lists the remote once and yields every file with its size, so the total volume
is known before anything transfers. Files are filtered by `year_range`, and their local
paths built as `<download>/<version>/raw/<product>/<file>.nc`.

Drive identifies a file by id rather than by name, so one folder can hold two files
called the same thing, and the NRT folders do: a day gets re-released and the revision
is uploaded alongside the original instead of over it. rclone copies one of them and
ignores the other, so the listing is collapsed the same way, keeping the most recently
modified. Without that the name lands in a shard twice and is verified against whichever
copy was listed last, which makes a complete download report a size mismatch. Each one
dropped is logged, since the choice decides what ends up on disk.

MSWEP daily is tens of thousands of small files. One rclone process per file would mean
tens of thousands of process spawns and enough Google Drive API traffic to trip the
per-user rate limit, so instead the file list is split into `n_processes` shards
balanced by total bytes, and each worker runs one long-lived
`rclone copy --files-from <shard>`. A shard only ever covers one destination directory,
which is what makes `year_subdirectories` safe.

Every run ends with a verification pass comparing each expected file against the size
the remote reported, listing anything missing or short and exiting non-zero. Rerunning
picks up exactly those files: rclone skips whatever already matches, and writes to a
`.partial` file that is only renamed on completion, so an interrupted run never leaves
something that looks finished. Stray partials are swept before and after every run, and
on Ctrl-C.

Keep `n_processes * rclone.tpslimit` at or below about 10, which is Drive's per-user
request ceiling. If the logs fill with `rateLimitExceeded`, lower `tpslimit` rather than
`n_processes`.

## Layout note

`year_subdirectories` is `true`, so the tree is split a year at a time:

```
<download>/v_2_8_0/raw/past/daily/1979/1979032.nc
<download>/v_2_8_0/raw/past/daily/1980/1980001.nc
<download>/v_2_8_0/raw/nrt/daily/2020/2020332.nc
```

Setting it to `false` puts every daily file in one directory instead, which for the
full record is around 15,000 entries — above the 2,000–3,000 that GLADE's parallel
filesystem is happy with, so the split is worth keeping.
