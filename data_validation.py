from prefect import task, flow, get_run_logger
import time as ttime
from tiled.client import from_uri


@task(retries=2, retry_delay_seconds=10)
def get_client(uid, api_key=None):
    logger = get_run_logger()
    tiled_client = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = tiled_client["smi"]["raw"][uid]
    logger.info(f"Validating uid {uid}")
    return run


@task
def read_stream(run, stream):
    stream_data = run[stream].read()
    return stream_data


@task
def read_all_streams(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    if dry_run:
        logger.info("Dry run: not creating Tiled client or checking streams")
    else:
        start_time = ttime.monotonic()
        run = get_client(uid, api_key=api_key)
        for stream in run:
            logger.info(f"{stream}:")
            stream_start_time = ttime.monotonic()
            stream_data = read_stream(run, stream)
            stream_elapsed_time = ttime.monotonic() - stream_start_time
            logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
            logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
        elapsed_time = ttime.monotonic() - start_time
        logger.info(f"{elapsed_time = }")

@flow
def data_validation(uid, api_key=None, dry_run=False):
    read_all_streams(uid, api_key=api_key, dry_run=dry_run)
