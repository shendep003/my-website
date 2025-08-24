import datetime
import logging
import os
import sys
from pis.utils import logger
from pis.etl.bom_analyser.extract_and_load import ExtractAndLoad

def run(run_for):
    """
    Driver Function

    * Take the current time
    * Initialize logger
    * Do the ETL process as per the function calls
    """
    start_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    os.environ["APP"] = "bom_analyser"
    logger.get_logger(start_time)
    logging.info("Running extraction process")
    process_type = run_for.split("-")[0]
    try:
        if process_type == "extract":
            extract_obj = ExtractAndLoad()
            query_type = run_for.split("-")[1]
            organization_name = run_for.split("-")[2]
            extract_obj.run(query_type, organization_name)
    except Exception as e:
        logging.error(f"Exception::{e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    valid_options = ["extract-Cat_Master-AND", "extract-Cat_Master-ANF",
                     "extract-Cat_Master-ANL", "extract-Cat_Master-ANS",
                     "extract-Cat_Master-ANP", "extract-Product_Structure-AND", 
                    "extract-Product_Structure-ANF", "extract-Product_Structure-ANL",
                    "extract-Product_Structure-ANS", "extract-Product_Structure-ANP"]
    if len(sys.argv) == 1:
        print("Please provide Command Line Arguments")
        print(f"Valid arguments::{valid_options}")
        sys.exit(1)
    run_for = sys.argv[1]
    if run_for not in valid_options:
        print(f"Invalid Options, Choose from {valid_options}")
        sys.exit(1)
    run(run_for)