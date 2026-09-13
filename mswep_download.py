"""Download raw MSWEP netCDF files from a shared Google Drive folder with rclone.

The Drive folder lays the data out as ``<root>/<product>/<file>.nc``, where a
product is a period and a temporal resolution such as ``Past/Daily``, and each
daily file is named ``YYYYDOY.nc``. The local tree is rebuilt as
``<download>/<version>/raw/<product>/<file>.nc`` with the version written as
``v_2_8_0`` rather than ``V2.8.0`` and the product lowercased, optionally with a
year directory inserted when ``year_subdirectories`` is set.

Which files are fetched is driven by the config: the list of ``products`` and
``year_range`` (``null`` for the whole record). MSWEP daily is tens of thousands
of small files, so rather than one transfer per file the list is split into
``n_processes`` shards balanced by total bytes, and each worker process runs one
long lived ``rclone copy --files-from`` over its shard, with its own log file.
Spreading the files over few, long lived rclone processes is what keeps the run
inside Google Drive's per user request rate limit.

Downloads are restartable: rclone skips a file whose local copy already matches
the remote size and modification time, it transfers to a ``.partial`` file that
is only renamed into place once complete, and stray partial files are swept
before and after a run. The closing verification pass compares every expected
file against the size the remote reported, so a rerun picks up exactly the files
it lists.
"""

import logging
import argparse
import os
import re
import time
import socket
import multiprocessing
from multiprocessing import Pool

from utils.path_utils import (
    load_config,
)
from utils.log_utils import (
    setup_logging,
)
from utils.rclone_utils import (
    build_flags,
    remote_path,
    lsjson,
    run_copy,
)

# '1979032.nc' -> year 1979, day of year 032
FILENAME_RE = re.compile(r'^(?P<year>\d{4})(?P<doy>\d{3})\.nc$')
# 'V2.8.0' -> '2.8.0', 'V3.16' -> '3.16'; MSWEP versions carry two or three
# parts depending on the release, so the count is not fixed
VERSION_RE = re.compile(r'^[Vv](?P<number>\d+(?:\.\d+)*)$')
# processName keeps the workers apart in the shared console stream
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(processName)s %(name)s: %(message)s'

LOG = logging.getLogger(__name__)

# per worker rclone flags, filled in by init_worker; the settings dict is not
# otherwise visible inside a worker, so the flags are built once and kept here
_WORKER_STATE = {}


def parse_year(filename):
    """Return the year encoded in an MSWEP filename, or None if unparseable.

    Args:
        filename (str): Basename of a remote file, e.g. '1979032.nc'.

    Returns:
        int | None: The four digit year, or None if the filename does not match.
    """
    match = FILENAME_RE.match(filename)
    return int(match.group('year')) if match else None


def format_version(version):
    """Rewrite an MSWEP version for use as a directory name.

    Args:
        version (str): Version as written in the config, e.g. 'V2.8.0' or 'V3.16'.

    Returns:
        str: The version lowercased with the parts underscore separated, so
            'V2.8.0' -> 'v_2_8_0' and 'V3.16' -> 'v_3_16'. However many parts
            the version has are kept, since MSWEP numbers releases both ways.

    Raises:
        ValueError: If the version is not a 'V' followed by dot separated numbers.
    """
    match = VERSION_RE.match(version)
    if match is None:
        raise ValueError(
            f"cannot parse version {version!r}, expected e.g. 'V2.8.0' or 'V3.16'"
        )
    return 'v_' + '_'.join(match.group('number').split('.'))


def format_product(product):
    """Rewrite a remote product path for use as a local directory path.

    Args:
        product (str): Product as written in the config, e.g. 'Past/Daily'.

    Returns:
        str: The lowercased path components, e.g. 'past/daily'.
    """
    return os.path.join(*(part.lower() for part in product.split('/')))


def download_root(settings):
    """Root of the local tree for the configured version.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. '<download>/v_2_8_0/raw'.
    """
    return os.path.join(
        settings['directories']['download'], format_version(settings['version']), 'raw'
    )


def shard_dir(settings):
    """Directory holding the per worker --files-from lists.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. './logs/shards'.
    """
    return os.path.join(settings['directories']['logs'], 'shards')


def pid_file(settings):
    """Path of the file recording which host and pid are running the download.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. '<logs>/mswep_download.pid'.
    """
    stem, _ = os.path.splitext(settings['log_file'])
    return os.path.join(settings['directories']['logs'], f'{stem}.pid')


def write_pid_file(settings):
    """Record the host and pid of this run.

    A download started on one login node is invisible from the others -- GLADE
    is shared but process tables are not -- so the host has to be written down
    alongside the pid or a later session cannot find the run to check or stop it.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: The path written.
    """
    path = pid_file(settings)
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(f'{socket.gethostname()} {os.getpid()}\n')
    return path


def clear_shards(settings):
    """Delete shard lists left over from a previous run.

    A run with a different config writes a different set of shard files, so
    stale ones would sit alongside the current run's and misrepresent it.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        int: Number of shard files removed.
    """
    directory = shard_dir(settings)
    if not os.path.isdir(directory):
        return 0
    removed = 0
    for filename in os.listdir(directory):
        if not filename.endswith('.txt'):
            continue
        try:
            os.remove(os.path.join(directory, filename))
        except OSError as error:
            LOG.error(f'could not remove stale shard list {filename}: {error}')
            continue
        removed += 1
    LOG.info(f'cleared {removed} stale shard lists in {directory}')
    return removed


def in_year_range(year, year_range):
    """Whether a year falls inside the configured range.

    Args:
        year (int): The year encoded in a filename.
        year_range (list | None): Inclusive [first, last], or None for no limit.

    Returns:
        bool: True if the file should be downloaded.
    """
    if year_range is None:
        return True
    first, last = year_range
    return first <= year <= last


def list_files(settings, product):
    """List the files to download for one product.

    Args:
        settings (dict): The loaded configuration.
        product (str): A product path such as 'Past/Daily'.

    Returns:
        list: (name, local_path, size) triples, filtered by year. The name is
            relative to the product directory, which is what rclone's
            --files-from expects.
    """
    rclone_settings = settings['rclone']
    source = remote_path(rclone_settings, product)
    entries = lsjson(source, build_flags(rclone_settings))

    files = []
    for entry in entries:
        # Path is relative to source; for a flat daily directory it is the name
        name = entry['Path']
        if not name.endswith('.nc'):
            continue
        year = parse_year(os.path.basename(name))
        if year is None:
            LOG.warning(
                f'skipping {product}/{name}: cannot parse year from filename'
            )
            continue
        if not in_year_range(year, settings['year_range']):
            continue
        local_dir = os.path.join(download_root(settings), format_product(product))
        # a flat product directory can hold tens of thousands of files, which is
        # hard on a parallel filesystem; this splits it a year at a time
        if settings['year_subdirectories']:
            local_dir = os.path.join(local_dir, str(year))
        local_path = os.path.join(local_dir, name)
        # Size is kept so shards can be balanced and downloads verified later
        files.append((name, local_path, entry['Size']))
    return files


def format_size(n_bytes):
    """Human readable file size.

    Args:
        n_bytes (int): Size in bytes.

    Returns:
        str: The size in MB, or GB once it passes 1024 MB.
    """
    mb = n_bytes / 1024**2
    return f'{mb:.1f} MB' if mb < 1024 else f'{mb / 1024:.2f} GB'


def build_shards(settings, product, files, n_shards):
    """Split one product's files into balanced --files-from lists.

    rclone resolves a --files-from name relative to the copy source and writes
    it at the matching place under the destination, so a shard can only ever
    cover one destination directory. Files are therefore grouped by their local
    directory first, which is a no-op unless ``year_subdirectories`` is set, and
    each group is split by longest processing time first: the largest file goes
    to whichever shard has the fewest bytes so far, so the workers finish
    together even when file sizes are uneven.

    Args:
        settings (dict): The loaded configuration.
        product (str): The product these files belong to.
        files (list): The (name, local_path, size) triples for that product.
        n_shards (int): Target number of shards across the whole product; each
            group gets at least one, so a product split into more groups than
            this yields one shard per group.

    Returns:
        list: (source, dest, shard_path, n_files, shard_bytes) tuples, one per
            non empty shard.
    """
    # group by destination directory, since a shard cannot span two of them
    groups = {}
    for name, local_path, size in files:
        groups.setdefault(os.path.dirname(local_path), []).append((name, size))

    source = remote_path(settings['rclone'], product)
    os.makedirs(shard_dir(settings), exist_ok=True)
    per_group = max(1, n_shards // len(groups))

    shard_jobs = []
    for dest in sorted(groups):
        group = groups[dest]
        shards = [[] for _ in range(per_group)]
        shard_bytes = [0] * per_group
        for name, size in sorted(group, key=lambda item: item[1], reverse=True):
            lightest = shard_bytes.index(min(shard_bytes))
            shards[lightest].append(name)
            shard_bytes[lightest] += size

        # naming the shard after the directory it feeds keeps two groups apart
        # and makes it obvious what a shard was for, e.g. 'past_daily_1979'
        stem = os.path.relpath(dest, download_root(settings)).replace(os.sep, '_')
        for index, names in enumerate(shards, start=1):
            if not names:
                continue
            shard_path = os.path.join(
                shard_dir(settings), f'{stem}_shard_{index}.txt'
            )
            with open(shard_path, 'w', encoding='utf-8') as shard_file:
                shard_file.write('\n'.join(names) + '\n')
            shard_jobs.append(
                (source, dest, shard_path, len(names), shard_bytes[index - 1])
            )
    return shard_jobs


def worker_log_file(settings, worker=None):
    """Path of the per-process log file, for the current worker unless one is named.

    Args:
        settings (dict): The loaded configuration.
        worker (str, optional): Worker name to build a path for. Defaults to the
            name of the calling process.

    Returns:
        str: e.g. '<logs>/mswep_download_forkserverpoolworker-1.log'.
    """
    stem, extension = os.path.splitext(settings['log_file'])
    worker = worker or multiprocessing.current_process().name.lower()
    return os.path.join(settings['directories']['logs'], f'{stem}_{worker}{extension}')


def init_worker(settings):
    """Give each worker process its own log file.

    Runs once per worker at pool startup. Unlike a connection based transfer
    there is no session to open here; each rclone process authenticates itself
    from the shared rclone config.

    Args:
        settings (dict): The loaded configuration.
    """
    setup_logging(worker_log_file(settings), fmt=LOG_FORMAT)
    _WORKER_STATE['flags'] = build_flags(settings['rclone'])
    LOG.info('worker ready')


def download_shard(job):
    """Download one shard, letting rclone skip files that are already complete.

    Args:
        job (tuple): (index, total, source, dest, shard_path, n_files, size);
            index and total are only used to tag the log lines.

    Returns:
        bool: True if rclone reported success, False if the transfer failed.
    """
    index, total, source, dest, shard_path, n_files, size = job
    tag = f'[{index}/{total}]'

    os.makedirs(dest, exist_ok=True)
    LOG.info(
        f'{tag} starting {n_files} files ({format_size(size)}) '
        f'from {source} -> {dest}'
    )
    start = time.monotonic()
    try:
        # rclone owns the retries, the skip check and the .partial staging, so
        # there is nothing to unwind here beyond reporting the failure
        returncode, stats = run_copy(
            source, dest, shard_path, _WORKER_STATE['flags'], LOG
        )
    except Exception as error:
        # keep going with the other shards; failures are reported at the end
        LOG.error(f'{tag} failed to run rclone for {shard_path}: {error}')
        return False
    elapsed = time.monotonic() - start
    if returncode != 0:
        LOG.error(f'{tag} rclone exited {returncode} for {shard_path}')
        return False

    # report what rclone actually moved rather than the shard's nominal size;
    # on a rerun almost everything is a skip and the two differ completely
    transferred = stats.get('transfers', 0)
    moved_bytes = stats.get('bytes', 0)
    skipped = n_files - transferred
    rate = moved_bytes / 1024**2 / elapsed if elapsed else 0
    LOG.info(
        f'{tag} finished {n_files} files: {transferred} transferred '
        f'({format_size(moved_bytes)}), {skipped} already present, '
        f'in {elapsed:.1f} s ({rate:.1f} MB/s)'
    )
    if stats.get('errors'):
        # rclone can exit 0 having retried past errors; surface them anyway
        LOG.warning(f'{tag} rclone reported {stats["errors"]} errors for {shard_path}')
    return True


def cleanup_partial_files(download_dir):
    """Remove leftover partial files from a run that was killed mid-transfer.

    Note this removes every *.partial and *.part under the directory, so two
    runs must not share a download directory.

    Args:
        download_dir (str): Root of the local download tree.

    Returns:
        int: Number of files removed.
    """
    removed = 0
    for root, _, filenames in os.walk(download_dir):
        for filename in filenames:
            if not filename.endswith(('.partial', '.part')):
                continue
            path = os.path.join(root, filename)
            try:
                os.remove(path)
            except OSError as error:
                LOG.error(f'could not remove partial download {path}: {error}')
                continue
            LOG.warning(f'removed partial download {path}')
            removed += 1
    LOG.info(f'cleaned up {removed} partial downloads in {download_dir}')
    return removed


def verify_downloads(jobs):
    """Check every expected file is on disk with the remote size, and log the results.

    Args:
        jobs (list): The (product, name, local_path, size) tuples that were
            enumerated for download.

    Returns:
        bool: True if every file is present and complete.
    """
    LOG.info(f'verifying {len(jobs)} downloaded files')
    missing = []
    incomplete = []
    verified_bytes = 0
    for product, name, local_path, size in jobs:
        if not os.path.exists(local_path):
            missing.append((f'{product}/{name}', local_path))
            continue
        local_size = os.path.getsize(local_path)
        if local_size != size:
            incomplete.append((f'{product}/{name}', local_path, local_size, size))
            continue
        verified_bytes += size

    # list the problem files first, then the summary line
    for remote, local_path in missing:
        LOG.error(f'missing: {local_path} (remote {remote})')
    for remote, local_path, local_size, size in incomplete:
        LOG.error(
            f'size mismatch: {local_path} is {format_size(local_size)}, '
            f'remote {remote} is {format_size(size)}'
        )

    n_ok = len(jobs) - len(missing) - len(incomplete)
    LOG.info(
        f'verified {n_ok}/{len(jobs)} files ({format_size(verified_bytes)}), '
        f'{len(missing)} missing, {len(incomplete)} incomplete'
    )
    return not missing and not incomplete


def main(settings, dry_run=False):

    os.makedirs(download_root(settings), exist_ok=True)
    os.makedirs(settings['directories']['logs'], exist_ok=True)
    log_file = os.path.join(settings['directories']['logs'], settings['log_file'])
    setup_logging(log_file, fmt=LOG_FORMAT)

    years = settings['year_range'] or 'all'
    LOG.info(
        f'downloading MSWEP {settings["version"]} data '
        f'({settings["products"]}, years: {years}) from '
        f'{remote_path(settings["rclone"])}'
    )
    LOG.info(f'running as pid {os.getpid()} on {socket.gethostname()}')

    # build the full file list up front, one listing call per product, so the
    # total volume is known before any transfer starts
    jobs = []
    per_product = {}
    for product in settings['products']:
        product_files = list_files(settings, product)
        LOG.info(f'found {len(product_files)} {product} files')
        per_product[product] = product_files
        jobs.extend(
            (product, name, local_path, size)
            for name, local_path, size in product_files
        )

    total_bytes = sum(size for _, _, _, size in jobs)
    LOG.info(f'found {len(jobs)} files in total ({format_size(total_bytes)})')

    if dry_run:
        for product, product_files in per_product.items():
            product_bytes = sum(size for _, _, size in product_files)
            LOG.info(
                f'{product}: {len(product_files)} files '
                f'({format_size(product_bytes)})'
            )
            for name, local_path, size in product_files[:3]:
                LOG.info(f'  {name} ({format_size(size)}) -> {local_path}')
            if len(product_files) > 3:
                LOG.info(f'  ... and {len(product_files) - 3} more')
        LOG.info('dry run, nothing downloaded')
        return

    if not jobs:
        LOG.warning('no files matched the configuration, nothing to download')
        return

    # clear debris from any previous run before the workers start writing
    cleanup_partial_files(download_root(settings))
    clear_shards(settings)
    LOG.info(f'wrote {write_pid_file(settings)}')

    n_processes = min(settings['n_processes'], len(jobs)) or 1
    shard_jobs = []
    for product, product_files in per_product.items():
        if product_files:
            shard_jobs.extend(
                build_shards(settings, product, product_files, n_processes)
            )
    # number the shards so each log line says which shard of how many it is
    shard_jobs = [
        (index, len(shard_jobs), *job) for index, job in enumerate(shard_jobs, start=1)
    ]

    LOG.info(
        f'starting {n_processes} worker processes over {len(shard_jobs)} shards, '
        f'each logging to {worker_log_file(settings, worker="<worker>")}'
    )
    try:
        # chunksize=1 hands out one shard at a time, so a worker that draws a
        # heavy shard does not hold up a whole pre-assigned block
        with Pool(processes=n_processes, initializer=init_worker, initargs=(settings,)) as pool:
            results = pool.map(download_shard, shard_jobs, chunksize=1)
    except BaseException as error:
        # covers ctrl-c too; the pool has terminated and joined its workers by
        # the time this runs, so nothing is still writing to a partial file
        LOG.error(f'download interrupted ({type(error).__name__}), cleaning up before exiting')
        cleanup_partial_files(download_root(settings))
        raise
    finally:
        # a stale pid file would point at a process that is gone, or worse at a
        # pid the system has since reused
        if os.path.exists(pid_file(settings)):
            os.remove(pid_file(settings))
    cleanup_partial_files(download_root(settings))

    for (index, _, _, _, shard_path, _, _), ok in zip(shard_jobs, results):
        if not ok:
            LOG.error(f'[{index}/{len(shard_jobs)}] did not download: {shard_path}')
    LOG.info(f'downloaded {results.count(True)} shards, {results.count(False)} failed')

    # final check against the remote sizes, in the main log; a rerun picks up
    # exactly the files reported here
    if not verify_downloads(jobs):
        LOG.error('download incomplete, rerun to retry the files listed above')
        raise SystemExit(1)

    LOG.info('all files downloaded and verified')
    LOG.info('done :-)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='download raw mswep netcdf files from google drive via rclone.'
    )
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to YAML configuration file.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List what would be downloaded and exit.',
    )
    args = parser.parse_args()
    settings = load_config(args.config)
    main(settings, dry_run=args.dry_run)
