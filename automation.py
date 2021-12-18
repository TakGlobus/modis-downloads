import os
import time
import logging
import argparse

import globus_sdk
from globus_sdk import AccessTokenAuthorizer 
from funcx.sdk.client import FuncXClient

logging.basicConfig()
app_logger = logging.getLogger("app.logger")
app_logger.setLevel(logging.DEBUG)

def get_args(verbose=True):
    p = argparse.ArgumentParser()
    ### Base parse arguments
    p.add_argument('-m', '--machine',    dest='machine',    type=str, default='midway',    help='Name of machine for execution')
    p.add_argument('-p', '--filepath',   dest='filepath',   type=str, default='./',        help='path to wget bash script')
    p.add_argument('-f', '--filename',   dest='filename',   type=str, default='wget.bash', help='wget bash script name')
    p.add_argument('-y', '--year',       dest='year',       type=int, default=2012,        help='year of downloading')
    p.add_argument('-b', '--BASEDIR',    dest='BASEDIR',    type=str, default='./',        help='name of directory to save downloaded files')
    p.add_argument('-C', '--CMD',       nargs='+', dest='CMD' ,       default=['nohup', 'bash'], help='list of bash command e.g. -C nohup bash = [nohup, bash]') # list
    p.add_argument('-s', '--start_list',nargs='+', dest='start_list', default=[1,  100,200,300],   help='list of start day list e.g. [1] when bash wget.bash 1 50') # list
    p.add_argument('-e', '--end_list',  nargs='+', dest='end_list',   default=[101,201,301,365],   help='list of end   day list e.g. [50] ') # list
    ### Transfer parse arguments
    p.add_argument("--transfer",        action='store_true', help="add this flag when execute transfer")
    p.add_argument('--sourcepathdir',   type=str, default='/home/tkurihana', help='path to downloaded file directory e.g. ~/data/MOD021KM')
    p.add_argument('--destpathdir',     type=str, default='/home/tkurihana', help='path to transfer data e.g. /eagle/Clouds/C6.1/MOD021KM')

    FLAGS = p.parse_args()
    if verbose:
        for f in FLAGS.__dict__:
            print("\t", f, (25 - len(f)) * " ", FLAGS.__dict__[f])
        print("\n")
    return FLAGS

def status_polling(filepath:str, filename:str):
    """ function to polling wget's running status:
        filename : (str) filename of wget script
    """
    import os
    import re
    import subprocess
    
    INDICES=["UID","PID","PPID","C","STIME","TTY","TIME","CMD"]

    def exec_ps(CMD, SHELL=True):
        proc = subprocess.Popen(CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=SHELL)
        out, err = proc.communicate()
        return out.decode("utf-8")

    def string_extractor(string:str):
        ps_list = string.split()
        ps_cmd=ps_list[:len(INDICES)-1] + [" ".join(ps_list[len(INDICES)-1:])]
        cmd_dict = {}
        for i, (index, value) in enumerate(zip(INDICES,ps_cmd)):
            cmd_dict[index] = value
        return cmd_dict

    SCMD=f"ps -aef | grep {filename}"
    results_cmd1 = exec_ps(CMD=SCMD, SHELL=True)
    strings = results_cmd1.split("\n")

    PIDs = []
    for i in strings:
        # Split item of ps commands
        try:
            _cmd_dict = string_extractor(string=i)
        except Exception:
            pass
        try:
            _CMD = _cmd_dict['CMD']
        except KeyError:
            _CMD = None
            pass
        # Find filename in command column
        if  _CMD:     
            CMD_string = re.findall(f"bash {os.path.join(filepath, filename) }", _CMD)
        else:
            CMD_string = None
        if CMD_string:
            PIDs.append(_cmd_dict['PID'])
    if len(PIDs) == 0:
        rstring = "Terminated or Hanged"
    else:
        rstring = "Running"
    return rstring

##################################################################

def status_scraper(BASEDIR, year, UNIT='TB'):
    import os
    import calendar
    from tqdm import tqdm

    _MULTIPLY={
        "MB": 2, "GB": 3, "TB": 4,
    }

    def convert_bytes(x, UNIT='TB'):
        alpha = 1024.00**_MULTIPLY[UNIT]
        return x/alpha

    ndays  = 366 if calendar.isleap(year) else 365
    nfiles = 288 # number of files might vary due to e.g. instrument errors 

    disk=0.00
    df  =0.00
    with tqdm(total = ndays * nfiles) as pbar:
        for i in range(1,ndays+1,1):
            day = str(i).zfill(3) 
            DIR = os.path.join(*[BASEDIR, str(year), day])
            try:
                f = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
                values = sum([ convert_bytes(os.stat(os.path.join(DIR,name)).st_size, UNIT=UNIT) 
                            for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))] )
            except FileNotFoundError:
                f = 0
                values = 0.00
                pass
            disk += values
            df   += f
            pbar.update(f)
            pbar.set_postfix({f'Disk ({UNIT})' : round(disk,2)})
            pbar.set_description("Processing %d" % year)
    com_percent = df / (ndays * nfiles) * 100.0
    #rstring = f"Processsing {year} : Task completion = {com_percent} %"
    return com_percent

##################################################################


def app_launcher(filepath, filename, CMD, ARGS):
    """
        Args:
            filepath : (str) path to modis downloading script
            filename : (str) downloading script
            CMD      : (list) command e.g. [nohup,bash] 
            ARGS     : (list) arguments of downloading script e.g. [--dir=./ ,  ]
    """
    import os
    from subprocess import run, PIPE, TimeoutExpired
    flag = 0
    try:
        run(CMD+[os.path.join(*[filepath, filename])]+ARGS, )
    except TimeoutExpired:
        flag = -1
    return flag

def polling_launcher(fxc, func_uuid):
    """ 
        Args:
            fxc (FuncXClient) client object 
    """
    # Argparse PARAM
    FLAGS=get_args(False)
    res = fxc.run(FLAGS.filepath, FLAGS.filename, function_id=func_uuid, endpoint_id=endpoints[FLAGS.machine])
    while fxc.get_task(res)['pending'] == True:
        time.sleep(5)    
    gr = fxc.get_result(res)
    return gr

def scraper_launcher(fxc, func_uuid):
    """ 
        Args:
            fxc (FuncXClient) client object 
    """
    # Argparse PARAM
    FLAGS=get_args(False)
    res = fxc.run(FLAGS.BASEDIR, FLAGS.year, function_id=func_uuid, endpoint_id=endpoints[FLAGS.machine])
    while fxc.get_task(res)['pending'] == True:
        time.sleep(5)    
    gr = fxc.get_result(res)
    return gr

##################################################################

def auth_parser(CLIENT_ID:str):
    """client ID : https://globus-sdk-python.readthedocs.io/en/stable/tutorial.html#step-1-get-a-client
    """

    client = globus_sdk.NativeAppAuthClient(CLIENT_ID)
    client.oauth2_start_flow()

    authorize_url = client.oauth2_get_authorize_url()
    print("Please go to this URL and login: {0}".format(authorize_url))

    auth_code = input("Please enter the code you get after login here: ").strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    globus_auth_data = token_response.by_resource_server["auth.globus.org"]
    globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]

    # most specifically, you want these tokens as strings
    AUTH_TOKEN = globus_auth_data["access_token"]
    TRANSFER_TOKEN = globus_transfer_data["access_token"]
    return {"AUTH_TOKEN": AUTH_TOKEN,  "TRANSFER_TOKEN":TRANSFER_TOKEN}


def exec_trasfer(TRANSFER_TOKEN:str, 
                source_endpoint_id:str,destination_endpoint_id:str,
                label:str,
                sourcepathdir:str, destpathdir:str,
                recursive:bool,
                filename=None,
                ):
    tc = globus_sdk.TransferClient(
        authorizer=AccessTokenAuthorizer(TRANSFER_TOKEN)
    )

    tdata = globus_sdk.TransferData(tc, 
                                    source_endpoint_id,
                                    destination_endpoint_id,
                                    label=label,
                                    sync_level="checksum")
    if recursive:
        try:
            tdata.add_item(sourcepathdir, destpathdir,
                    recursive=True)
        except Exception as e: 
            logging.exception(e)
    else:
        if os.path.exists(os.path.join(sourcepathdir, filename)):
            try:
                tdata.add_item(os.path.join(sourcepathdir, filename),
                        os.path.join(destpathdir, filename))
            except Exception as e: 
                logging.exception(e)
        else:
            raise FileNotFoundError

    if tdata:
        transfer_result = tc.submit_transfer(tdata)
        print("task_id =", transfer_result["task_id"])
        return transfer_result["task_id"]
    return -1

def get_response(TRANSFER_TOKEN:str, TASK_ID:str):
    tc = globus_sdk.TransferClient(
        authorizer=AccessTokenAuthorizer(TRANSFER_TOKEN)
    )
    r = tc.get_task(task_id=TASK_ID) # return dict
    app_logger.info(r['status'])
    return r['status']


if __name__ == "__main__":
    ### Config:
    # funcx endpoints
    endpoints={
        "machine name" : "endpoint name"
    }

    # globus endpoints
    globus_endpoints={
        "machine name" : "endpoint name"
    }

    # Argparse PARAM
    FLAGS=get_args()
    launcher_sleep_time=600 # seconds
    scrape_time=20 # seconds    

    # Type globus auth
    if FLAGS.transfer:
        TOKENS = auth_parser()

    # Call client object
    fxc = FuncXClient()

    # Execute download
    func_uuid=fxc.register_function(app_launcher)
    app_logger.info(f" Download function UUID : {func_uuid}")
    for idx, (istart, iend) in enumerate(zip(FLAGS.start_list, FLAGS.end_list)):
        app_logger.info(f"Launch worker-{idx+1}")
        res = fxc.run(FLAGS.filepath, FLAGS.filename, FLAGS.CMD, [str(istart), str(iend)], 
                        function_id=func_uuid, 
                        endpoint_id=endpoints[FLAGS.machine])
        while fxc.get_task(res)['pending'] == True:
            time.sleep(5)
        
    # Execute by funcx
    pollar_uuid =fxc.register_function(status_polling)
    scraper_uuid=fxc.register_function(status_scraper)
    app_logger.info(f" Polling Terminated/Hanged function UUID : {pollar_uuid}")
    app_logger.info(f" Scrape completion ratio function UUID : {scraper_uuid}")

   
    while True:
        # polling script running status
        pr = polling_launcher(fxc, pollar_uuid)
        sr = scraper_launcher(fxc, scraper_uuid)
        time.sleep(launcher_sleep_time)
        
        # stop operation
        nowtime =  os.popen("date").read().rstrip('\n')
        app_logger.info(f"{nowtime}  Status : {pr}  Complete {round(float(sr),2)} [%]")
        if pr == "Terminated or Hanged":
            break

    if FLAGS.transfer:
        # transfer operation
        task_id = exec_trasfer(TRANSFER_TOKEN = TOKENS["TRANSFER_TOKEN"], 
                    source_endpoint_id = globus_endpoints[FLAGS.machine],
                    destination_endpoint_id = globus_endpoints['theta'],
                    label='transfer to eagle theta MODIS',
                    sourcepathdir=FLAGS.sourcepathdir, 
                    destpathdir=FLAGS.destpathdir,
                    recursive=True,
                    )

        while res == 'ACTIVE':
            res = get_response(TRANSFER_TOKEN=TOKENS["TRANSFER_TOKEN"], 
                        TASK_ID=task_id)

            time.sleep(scrape_time)


