import os
from prefect import task, flow, get_run_logger
import time as ttime
from tiled.client import from_uri
from dotenv import load_dotenv


def get_api_key_from_env(api_key=None):
    logger = get_run_logger()
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    tiled_client = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = tiled_client["smi/raw"][uid]
    return run


@task
def read_stream(run, stream):
    stream_data = run[stream].read()
    return stream_data


# this is a task to enable being run by a ConcurrentTaskRunner
@task
def read_all_streams(uid, api_key=None):
    logger = get_run_logger()
    start_time = ttime.monotonic()
    run = get_run(uid, api_key=api_key)
    logger.info(f"Validating uid {uid}")
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
def data_validation(uid, api_key=None):
    read_all_streams(uid, api_key=api_key)
