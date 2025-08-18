from prefect import task, get_run_logger
import os, event_model
import numpy as np
import pandas as pd
from pathlib import Path
from tiled.client import from_profile
import time as ttime

tiled_client = from_profile("nsls2")["smi"]
tiled_client_raw = tiled_client["raw"]

@task
def export_amptek(ref):
    """
    Parameters
    ----------
    ref : Union[int, str]
        Scan_id or uid of the start document
    
    Returns
    -------
    """
    logger = get_run_logger()
    ########################

    target_template: str

    ########################

    run = tiled_client_raw[ref]


    if "amptek_energy_channels" in run.primary.data and "amptek_mca_spectrum" in run.primary.data :
        cycle = run.metadata["start"]["cycle"]
        project = run.metadata["start"]["project_name"]
        datasession = run.metadata["start"]["data_session"]
        #username = run.metadata["start"]['username']
        sample_name = run.metadata["start"]['sample_name']
        scan_id = run.metadata["start"]['scan_id']
        newdirpath = Path("/nsls2/data/smi/proposals/%s/%s/projects/%s/user_data/Amptek2/"% (cycle, datasession, project))
        newdirpath.mkdir(exist_ok=True, parents=True)
        target_template = (f"{sample_name}_id{scan_id}_FY.csv")
        common_column="amptek_energy_channels"
        columns=["amptek_mca_spectrum"]
        xr = run.primary.read()
        if columns is None:
            columns = list(xr.keys())
        all_columns = [common_column] + columns
        xr = xr[all_columns]
        logger.info(f"Start exporting of spectra to {newdirpath}")
        print(f"Start exporting of spectra to {newdirpath}")


        for name, doc in run.documents():
            if "event" in name:
                # continue building the target_template here adding
                # the event level things (motor positions)
                if name == "event":
                    doc = event_model.pack_event_page(doc)
                single_doc_data = {key:doc['data'][key][0] for key in doc['data']}
                file = newdirpath / target_template.format(**single_doc_data).format(**single_doc_data)
                xr = doc['data']
                if not common_column in xr:
                    continue
                data = xr.get(common_column)[0]
                for j in range(len(columns)):
                    data = np.vstack([data, xr.get(columns[j])[0]])
                data = data.T
                df = pd.DataFrame(data=data, columns=all_columns)
                logger.info(f"Exporting data with shape {data.shape} to {file}...")
                df.to_csv(file, index=False)
                logger.info(f"Exported to {file}")




