import os
from google_drive_downloader import GoogleDriveDownloader as gdd

prefix = "/data/ontimeperformance"
files = [
  { "name": f"{prefix}_flights_small.csv",   "file_id": "1LR5ULeE30oKo6DidtkHQNjCXJuZ-IIJO" },
#  { "name": f"{prefix}_flights_medium.csv",  "file_id": "1VBn-BdOX7EGQrkMg7MkcoOEPDHYBNdL_" },
#  { "name": f"{prefix}_flights_large.csv", "file_id": "1-KIBLcswJyzrbc4NsMrtVotyB1k2SdGX" },
  { "name": f"{prefix}_airlines.csv", "file_id": "1MIwg67fN43cK-frskA-2aYtxhw4rV9RY" },
  { "name": f"{prefix}_airports.csv", "file_id": "1cltP3m7Qgrp0Bei2LUlaYXwtsTz2wyCt" },
  { "name": f"{prefix}_aircrafts.csv", "file_id": "1JeDwF_zeJgE1Ebnnzkz2fRy1ZE2C7A6i" }
]

large_files =  { "name": f"{prefix}_flights_large.zip", "file_id": "1A7o60Qu62TherJRPyfMXAtCHIarS78oA" }

def remove_existing():
    for file in files:
        if os.path.exists("/data/{}".format(file["name"])):
            os.remove("/data/{}".format(file["name"]))

def download_csv():
    for file in files:
        gdd.download_file_from_google_drive(file_id=file["file_id"], dest_path="/data/{}".format(file["name"]))

def download_large_csv():
    gdd.download_file_from_google_drive(file_id=large_files["file_id"],
                                        dest_path="/data/{}".format(large_files["name"]),
                                        unzip=True)
