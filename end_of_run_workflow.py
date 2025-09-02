import getpass
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from data_validation import read_all_streams
from linker import get_symlink_pairs
from export import export_amptek


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    logger = get_run_logger()
    uid = stop_doc["run_start"]
    logger.info(f"effective user: {getpass.getuser()}")

    # Launch validation and linker concurrently.
    det_map = {"900KW": "WAXS", "1M": "SAXS", "2M": "SAXS2M"}
    logger.info("Running linker task")
    get_symlink_pairs(uid, det_map=det_map)
    logger.info("Running validation task")
    read_all_streams(uid, beamline_acronym="smi")
    logger.info("Running amptek export task")
    export_amptek(uid)

    log_completion()
