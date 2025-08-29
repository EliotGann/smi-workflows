from prefect import flow, task, get_run_logger
from pathlib import Path
from tiled.client import from_profile

import event_model
import tqdm
import shutil

tiled_client = from_profile("nsls2")["smi"]
tiled_client_raw = tiled_client["raw"]


def do_symlinking(
    links: list[tuple[str, Path, Path]],
    overwrite_dest=False,
) -> tuple[list[tuple[str, Path, Path]], list[tuple[str, Path, Path]]]:
    """
    Create the symlinks, making target directories as needed.

    Paramaters
    ----------
    links : list of (uid, src, dest) tuples
        The uid, source file and destination files

    overwrite_dest : bool, optional
        If an existing destitation should be unlinked and replaced.

    Returns
    -------
    linked, failed : list of (uid, src, dest) tuples
        The linked (or failed) values.
    """

    logger = get_run_logger()
    failed = []
    linked = []

    for uid, src, dest, analysis in tqdm.tqdm(links, leave=False):
        if not src.exists():
            logger.error(f"{src} does not exist. uid: {uid} dest: {dest} analysis: {analysis}")
            failed.append((uid, src, dest, analysis))
            continue

        try:
            dest.parent.mkdir(exist_ok=True, parents=True)

            if not analysis.exists():
                # copy the default analysis notebooks to the analysis directory
                default_analysis_path_s = Path('/nsls2/data/smi/shared/default_nb/saxs.ipynb')
                default_analysis_path_w = Path('/nsls2/data/smi/shared/default_nb/waxs.ipynb')
                
                analysis.mkdir(exist_ok=True, parents=True)

                shutil.copyfile(default_analysis_path_s, analysis / 'saxs.ipynb')
                shutil.copyfile(default_analysis_path_w, analysis / 'waxs.ipynb')
                



            if overwrite_dest and dest.exists():
                dest.unlink()
            dest.symlink_to(src)
            logger.info(f"symlink: {src} to {dest}")

        except Exception as e:
            tqdm.tqdm.write(f"FAILED: {dest}")
            logger.error("Exception while making symlink: {src} to {dest}")
            failed.append((uid, src, dest, analysis))
        else:
            tqdm.tqdm.write(f"Linked: {dest}")
            linked.append((uid, src, dest, analysis))
    if failed:
        logger.error(f"Tasks failed: {failed}")
    logger.info("Linked items:")
    for item in linked:
        logger.info(item)
    return linked, failed


@task
def get_symlink_pairs(ref, *, det_map, root_map=None):
    """
    Parameters
    ----------
    ref : Union[int, str]
        Scan_id or uid of the start document
    det_map : dict[str, str]
        A dictionaly mapping the detector name (1M, 900KW)
        to the type of measurement (SAXS, WAXS)
    root_map : dict[str, str], optional
        A mapping of root in the resource document -> a new path
        as in databroker

    Returns
    -------
    list[tuple[str, Path, Path]]
         A tuple of the start uid, the source path and the destination path.
    """
    logger = get_run_logger()
    ########################
    if root_map is None:
        root_map = {}

    links = []
    target_template: str
    output_path: str
    resource_info = {}
    datum_info = {}
    target_keys = set()
    ########################

    # hrf = db[ref]
    hrf = tiled_client_raw[ref]
    for name, doc in hrf.documents():
        if name == "start":
            start_uid = doc["uid"]
            #target_template = (f"{{det_name}}/{doc['username']}_{doc['sample_name']}_"
            #                   f"id{doc['scan_id']}_{{N:06d}}_{{det_type}}.tif")
            target_template = (f"{{det_name}}/{doc['sample_name']}_"
                               f"id{doc['scan_id']}_{{N:06d}}_{{det_type}}.tif")

            target_path = Path(
                (f"/nsls2/data/smi/proposals/{doc['cycle']}/{doc['data_session']}/"
                f"projects/{doc['project_name']}/user_data")
            )
            analysis_path = Path(
                (f"/nsls2/data/smi/proposals/{doc['cycle']}/{doc['data_session']}/"
                f"projects/{doc['project_name']}/analysis")
            )

        elif name == "resource":

            if doc["spec"] != "AD_TIFF":
                continue
            doc_root = doc["root"]
            resource_info[doc["uid"]] = {
                "path": Path(root_map.get(doc_root, doc_root)) / doc["resource_path"],  # noqa: 501
                "kwargs": doc["resource_kwargs"],
            }
        elif "datum" in name:
            if name == "datum":
                doc = event_model.pack_datum_page(doc)

            for datum_uid, point_number in zip(
                doc["datum_id"], doc["datum_kwargs"]["point_number"]
            ):
                datum_info[datum_uid] = (
                    resource_info[doc["resource"]],
                    point_number,
                )

        elif name == "descriptor":
            for k, v in doc["data_keys"].items():
                if "external" in v:
                    target_keys.add(k)
        elif "event" in name:
            # continue building the target_template here adding
            # the event level things (motor positions)
            if name == "event":
                doc = event_model.pack_event_page(doc)
            single_doc_data = {key:doc['data'][key][0] for key in doc['data']}
            for key in target_keys:

                det, _, _ = key.partition("_")
                det_name = det.removeprefix("pil")
                det_type = det_map.get(det_name, det_name)

                if key not in doc["data"]:
                    continue

                for datum_id in doc["data"][key]:
                    # pulling out the image column
                    resource_vals, point_number = datum_info[datum_id]
                    orig_template = resource_vals["kwargs"]["template"]
                    fpp = resource_vals["kwargs"]["frame_per_point"]
                    base_fname = resource_vals["kwargs"]["filename"]

                    for fr in range(fpp):
                        source_path = Path(
                            orig_template
                            % (
                                str(resource_vals["path"]) + "/",
                                base_fname,
                                point_number * fpp + fr,
                            )
                        )

                        
                        dest_path = target_path / target_template.format(
                            det_name=det_name,
                            N=point_number * fpp + fr,
                            det_type=det_type,
                            **single_doc_data
                        ).format(**single_doc_data)
                        
                        links.append(
                            (start_uid, source_path, dest_path, analysis_path)
                        )

        elif name == "stop":
            break

    linked, failed = do_symlinking(links, overwrite_dest=True)

    if len(failed) > 0:
        logger.info(f"Failed generating links {failed}")
        return
    elif len(linked) > 0:
        logger.info(f"Links successfully generated {linked}")
        return

