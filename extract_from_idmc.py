import requests
import json
import time
import configparser
import os
import re
import socket

version = 20250613


CLIENT_ID = "idmc_api"
NONCE = "1234"

def login(pod, username, password):
    url = f"https://{pod}.informaticacloud.com/identity-service/api/v1/Login"
    payload = json.dumps({
        "username": username,
        "password": password
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, data=payload)
    response.raise_for_status()
    data = response.json()

    session_id = data.get("sessionId")
    org_id = data.get("currentOrgId")

    if not session_id or not org_id:
        raise Exception(f"Login failed, missing sessionId or currentOrgId: {data}")
    print(f"Logged in. Session ID: {session_id}, Org ID: {org_id}")
    return session_id, org_id


def generate_jwt_token(pod, session_id):
    url = f"https://{pod}.informaticacloud.com/identity-service/api/v1/jwt/Token?client_id={CLIENT_ID}&nonce={NONCE}"
    headers = {
        'cookie': f'USER_SESSION={session_id}',
        'IDS-SESSION-ID': session_id
    }
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    jwt_token = data.get("jwt_token")
    if not jwt_token:
        raise Exception(f"Failed to get JWT token: {data}")
    print("Generated JWT token")
    return jwt_token


def start_export_job(pod, org_id, jwt_token, knowledge_query, export_filename):
    url = f"https://idmc-api.{pod}.informaticacloud.com/data360/search/export/v1/assets?knowledgeQuery={knowledge_query}&segments=all&fileName={export_filename}&summaryViews=all"
    payload = json.dumps({
        "from": 0,
        "size": 10000
    })
    headers = {
        'X-INFA-ORG-ID': org_id,
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {jwt_token}'
    }

    response = requests.post(url, headers=headers, data=payload)
    response.raise_for_status()
    data = response.json()

    job_id = data.get("jobId")
    tracking_uri = data.get("trackingURI")
    output_uri = data.get("outputURI")

    if not (job_id and tracking_uri and output_uri):
        raise Exception(f"Export job start failed, incomplete response: {data}")

    print(f"Started export job. Job ID: {job_id}")
    return job_id, output_uri


def check_job_status(pod, org_id, jwt_token, job_id, poll_interval=10):
    url = f"https://idmc-api.{pod}.informaticacloud.com/data360/observable/v1/jobs/{job_id}"
    headers = {
        'X-INFA-ORG-ID': org_id,
        'Authorization': f'Bearer {jwt_token}'
    }

    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        status = data.get("status")
        print(f"Job status: {status}")

        if status == "COMPLETED":
            print("Job completed")
            break
        elif status in ("FAILED", "CANCELED", "ERROR"):
            raise Exception(f"Job ended with status: {status}")
        else:
            time.sleep(poll_interval)


def download_export_file(pod, org_id, jwt_token, output_uri, export_filename_base):
    url = f"https://idmc-api.{pod}.informaticacloud.com{output_uri}"
    headers = {
        'X-INFA-ORG-ID': org_id,
        'Authorization': f'Bearer {jwt_token}'
    }

    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()

    content_disposition = response.headers.get('Content-Disposition')
    filename = None

    if content_disposition:
        # Example: Content-Disposition: attachment; filename="Export_CDGC_All_Segments_1749653847.xlsx"
        match = re.search(r'filename="([^"]+)"', content_disposition)
        if match:
            filename = match.group(1)
    
    if not filename:
        # fallback: use export_filename_base + .bin extension
        filename = export_filename_base + ".bin"

    with open(filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(filename)  # print the full filename to stdout
    return filename

def infaLog(annotation=""):
    try:
        ## This is simply a "phone home" call.
        ## Just to note which Informatica Org is using this script
        ## If it's unable to reach this URL, it will ignore.
        this_headers = {"Content-Type": "application/json", "X-Auth-Key": "b74a58ca9f170e49f65b7c56df0f452b0861c8c870864599b2fbc656ff758f5d"}
        logs=[{"timestamp": time.time(), "function": f"[{os.path.basename(__file__)}][main]", "execution_time": "N/A", "annotation": annotation, "machine": socket.gethostname()}]
        response=requests.post("https://infa-lic-worker.tim-qin-yujue.workers.dev", data=json.dumps({"logs": logs}), headers=this_headers)
    except:
        pass


def main():
    print(f"Running extract_idmc.py Version {version}")
    config_path = "config.ini"
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config file 'config.ini' not found.")
    config = configparser.ConfigParser()
    config.read(config_path)

    if 'idmc' not in config:
        raise Exception("Missing [idmc] section in config.ini")

    idmc_cfg = config['idmc']
    username = idmc_cfg.get('username')
    password = idmc_cfg.get('password')
    pod = idmc_cfg.get('pod')
    query = idmc_cfg.get('query')
    export_filename = idmc_cfg.get('export_filename')
    infaLog(f"Org: {username}, Pod: {pod}, Version: {version}, Query: {query}, Export: {export_filename}")

    if not all([username, password, pod, query, export_filename]):
        raise Exception("Config [idmc] must include username, password, pod, query, and export_filename")

    session_id, org_id = login(pod, username, password)
    jwt_token = generate_jwt_token(pod, session_id)
    job_id, output_uri = start_export_job(pod, org_id, jwt_token, query, export_filename)

    check_job_status(pod, org_id, jwt_token, job_id)

    downloaded_file = download_export_file(pod, org_id, jwt_token, output_uri, export_filename)
    # The file name is printed by download_export_file and also returned here if needed


if __name__ == "__main__":
    main()
