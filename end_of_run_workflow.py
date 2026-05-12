import getpass
import os
import traceback

from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from dotenv import load_dotenv
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext
from prefect.settings import PREFECT_UI_URL

from data_validation import read_all_streams, get_run, get_api_key_from_env
from linker import get_symlink_pairs
from export import export_amptek, has_amptek_keys

CATALOG_NAME = "smi"


def slack(func):
    """
    Send a message to mon-prefect and mon-prefect-im slack channels if the flow-run failed.
    Send a message to mon-prefect-smi slack channel with the flow-run status.
    Send a message to mon-bluesky slack channel if the bluesky-run failed.

    NOTE: the name of this inner function is the same as the real end_of_workflow() function because
    when the decorator is used, Prefect sees the name of this inner function as the name of
    the flow. To keep the naming of workflows consistent, the name of this inner function had to match the expected name.
    """

    def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")

        # Load slack credentials that are saved in Prefect.
        mon_prefect = SlackWebhook.load("mon-prefect")
        mon_bluesky = SlackWebhook.load("mon-bluesky")
        mon_prefect_smi = SlackWebhook.load("mon-prefect-smi")
        mon_prefect_cs = SlackWebhook.load("mon-prefect-cs")

        # Get the uid.
        uid = stop_doc["run_start"]

        # Get Tiled API key, if not set already
        if not api_key:
            api_key = get_api_key_from_env()

        # Get the scan_id.
        run = get_run(uid, api_key=api_key)
        scan_id = run.start["scan_id"]

        # Send a message to mon-bluesky if bluesky-run failed.
        if stop_doc.get("exit_status") == "fail":
            mon_bluesky.notify(
                f":bangbang: {CATALOG_NAME} bluesky-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```reason: {stop_doc.get('reason', 'none')}```"
            )

        try:
            result = func(stop_doc, api_key=api_key, dry_run=dry_run)

            # Send a message to mon-prefect-smi if flow-run is successful.
            message = f":white_check_mark: {CATALOG_NAME} flow-run successful. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}```"
            mon_prefect_smi.notify(message)
            return result
        except Exception as e:
            tb = traceback.format_exception_only(e)

            # Send a message to mon-prefect-smi, mon-prefect if flow-run failed.
            message = f":bangbang: {CATALOG_NAME} flow-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            mon_prefect.notify(message)
            mon_prefect_smi.notify(message)
            flow_run = FlowRunContext.get().flow_run
            # Add link to flow-run for the message to mon-prefect-cs.
            program_message = (
                f":bangbang: {CATALOG_NAME} flow-run failed. <https://{PREFECT_UI_URL.value()}/flow-runs/"
                + f"flow-run/{flow_run.id}|the flow run link> (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            )
            mon_prefect_cs.notify(program_message)
            raise

    return end_of_run_workflow


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(task_runner=ConcurrentTaskRunner())
@slack
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    logger = get_run_logger()
    uid = stop_doc["run_start"]
    if not api_key:
        api_key = get_api_key_from_env(api_key=None)
    logger.info(f"effective user: {getpass.getuser()}")

    # Launch validation and linker concurrently.
    det_map = {"900KW": "WAXS", "1M": "SAXS", "2M": "SAXS2M"}
    linker_task = get_symlink_pairs.submit(uid, det_map=det_map, api_key=api_key, dry_run=dry_run)
    logger.info("Launched linker task")
    validation_task = read_all_streams.submit(uid, api_key=api_key)
    logger.info("Launched validation task")
    export_task = None
    if not dry_run and has_amptek_keys(uid, api_key=api_key):
        export_task = export_amptek.submit(uid)
        logger.info("Launched amptek export task")
    elif dry_run:
        logger.info("Dry run: skipping amptek export")
    else:
        logger.info("Skipping export, amptek keys not present")
    # Wait for completion.
    logger.info("Waiting for tasks to complete")
    validation_task.result()
    linker_task.result()
    if export_task:
        export_task.result()
    log_completion()
