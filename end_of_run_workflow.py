import getpass
import os
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from data_validation import data_validation
from linker import get_symlink_pairs
from export import export_amptek
from dotenv import load_dotenv


def get_api_key_from_env(api_key=None):
    logger = get_run_logger()
    if not api_key:
        try:
            with open("/srv/container.secret", "r") as secrets:
                load_dotenv(stream=secrets)
        except Exception:
            logger.exception("Exception while getting Tiled API key")
        finally:
            api_key = os.environ["TILED_API_KEY"]
    return api_key


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(task_runner=ConcurrentTaskRunner())
def end_of_run_workflow(stop_doc, dry_run=False):
    logger = get_run_logger()
    uid = stop_doc["run_start"]
    api_key = get_api_key_from_env(api_key=None)
    logger.info(f"effective user: {getpass.getuser()}")

    # Launch validation and linker concurrently.
    det_map = {"900KW": "WAXS", "1M": "SAXS", "2M": "SAXS2M"}
    linker_task = get_symlink_pairs.submit(uid, det_map=det_map, api_key=api_key, dry_run=dry_run)
    logger.info("Launched linker task")
    validation_task = data_validation.submit(uid, api_key=api_key, dry_run=dry_run)
    logger.info("Launched validation task")
    export_task = None
    if not dry_run:
        export_task = export_amptek.submit(uid)
        logger.info("Launched amptek export task")

    # Wait for completion.
    logger.info("Waiting for tasks to complete")
    validation_task.result()
    linker_task.result()
    if export_task:
        export_task.result()
    log_completion()
