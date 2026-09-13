"""Thin subprocess wrappers around the rclone command line client.

Every rclone invocation in this project goes through here, so the flags built
from the config (`build_flags`) are shared by listing and copying and cannot
drift apart. Nothing in this module knows about MSWEP; it only knows how to
name a remote, list it, and copy from it.
"""

import json
import posixpath
import subprocess

# rclone writes one json object per line under --use-json-log; these are its
# level names mapped onto the logging module's methods
LOG_LEVELS = {
    'debug': 'debug',
    'info': 'info',
    'notice': 'info',
    'warning': 'warning',
    'error': 'error',
    'critical': 'critical',
}


def build_flags(rclone_settings):
    """Assemble the rclone flags that every invocation shares.

    Args:
        rclone_settings (dict): The 'rclone' section of the configuration.

    Returns:
        list: Flags ready to splice into an rclone argument list.
    """
    flags = [
        '--transfers', str(rclone_settings['transfers']),
        '--checkers', str(rclone_settings['checkers']),
        '--tpslimit', str(rclone_settings['tpslimit']),
        '--retries', str(rclone_settings['retries']),
        '--low-level-retries', str(rclone_settings['low_level_retries']),
    ]
    # a login node's network is shared; capping bandwidth keeps a long download
    # from crowding out everyone else's work
    if rclone_settings['bwlimit']:
        flags.extend(['--bwlimit', str(rclone_settings['bwlimit'])])
    # the folder is in 'Shared with me' rather than My Drive, which rclone
    # treats as a separate namespace that has to be asked for explicitly
    if rclone_settings['shared_with_me']:
        flags.append('--drive-shared-with-me')
    flags.extend(rclone_settings['extra_flags'])
    return flags


def remote_path(rclone_settings, *parts):
    """Build a 'remote:path' string for the configured remote.

    Args:
        rclone_settings (dict): The 'rclone' section of the configuration.
        *parts (str): Path components below the configured root.

    Returns:
        str: e.g. 'gdrive:MSWEP_V280/Past/Daily'.
    """
    # posixpath keeps remote paths from ever picking up a local separator
    path = posixpath.join(rclone_settings['root'], *parts)
    return f'{rclone_settings["remote"]}:{path}'


def lsjson(remote, flags):
    """List every file under a remote path.

    Args:
        remote (str): A 'remote:path' string, as built by remote_path.
        flags (list): Common flags from build_flags.

    Returns:
        list: One dict per file, with at least 'Path', 'Name' and 'Size'.

    Raises:
        RuntimeError: If rclone exits non-zero.
    """
    command = [
        'rclone', 'lsjson',
        '--recursive',
        '--files-only',
        # one listing call per directory instead of one per file; the
        # difference is thousands of api calls on a directory this size
        '--fast-list',
        '--no-modtime',
        remote,
        *flags,
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f'rclone lsjson failed on {remote} (exit {result.returncode}): '
            f'{result.stderr.strip()}'
        )
    return json.loads(result.stdout)


def run_copy(source, dest, files_from, flags, log):
    """Copy a listed set of files from a remote directory to a local one.

    rclone's own progress lines are streamed into the caller's logger as they
    are produced, rather than buffered, because a single call can run for hours.

    Args:
        source (str): A 'remote:path' string to copy from.
        dest (str): Local directory to copy into.
        files_from (str): Path to a file listing the names to copy, one per
            line, relative to source.
        flags (list): Common flags from build_flags.
        log (logging.Logger): Logger to re-emit rclone's output through.

    Returns:
        tuple: rclone's exit code (0 on success) and the last stats dict it
            reported, which carries 'bytes', 'transfers', 'checks' and 'errors'.
    """
    command = [
        'rclone', 'copy',
        source,
        dest,
        '--files-from', files_from,
        '--use-json-log',
        '--log-level', 'INFO',
        '--stats', '60s',
        '--stats-log-level', 'NOTICE',
        *flags,
    ]
    log.info(f'running: {" ".join(command)}')
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        # rclone logs to stderr; merging keeps the ordering intact
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    stats = {}
    for line in process.stdout:
        record = log_rclone_line(line, log)
        # rclone reports cumulative stats periodically; the last one it emits
        # before exiting is the summary for the whole shard
        if record and record.get('stats'):
            stats = record['stats']
    process.wait()
    return process.returncode, stats


def log_rclone_line(line, log):
    """Re-emit one line of rclone output through a logger.

    Args:
        line (str): A single line of rclone's --use-json-log output.
        log (logging.Logger): Logger to write to.

    Returns:
        dict | None: The parsed record, or None if the line was not json.
    """
    line = line.strip()
    if not line:
        return None
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        # not every line rclone emits is json, e.g. a go panic or a warning
        # from before logging is set up; keep it rather than dropping it
        log.info(f'rclone: {line}')
        return None
    level = LOG_LEVELS.get(record.get('level', 'info'), 'info')
    message = record.get('msg', line)
    if record.get('object'):
        message = f'{record["object"]}: {message}'
    # the periodic stats message is a multi-line block; flattening it keeps one
    # log record on one line, which matters when eight workers share a stream
    message = ' | '.join(part.strip() for part in message.split('\n') if part.strip())
    getattr(log, level)(f'rclone: {message}')
    return record
