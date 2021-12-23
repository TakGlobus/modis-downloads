# modis-downloads

- automation.py:   
    automated download script. The script can run on remote machines. In this case, add `--transfer` option.  
    you may need to obtain Globus endpoint id, Funcx endpoint id, and LAADS token to run wget script.

- modis-wget.bash:  
    bash script to execute wget command for downloading modis data from NASA LAADS server. 

- requirement.txt:  
    package information. Run `pip install -r requirements.txt`
    
## Prerequisite

- `pip install globus-sdk`
- `pip install funcx`
- `pip install funcx-endpoint`

### Caution
You may need to install `funcx` and `funcx-endpoint` libraries on all machines using the downloading process i.e., if you run the automate.py between two remote machines, you need to download and start their endpoint.


## Example
1.  Edit and check `modis-wget.bash` if ;  
- `$product` e.g. MOD021KM, MOD06_L2  
- `$year` e.g 2011  
- `$token` (you need to register and then get from LAADS website)
- `$savedir` (which directory you will save the downloading data)
are correctly set.  

2. Add environement variable `GLOBUS_CLIENT_ID`,       
e.g `export GLOBUS_CLIENT_ID='xxxxx'`  

3. You may need to edit script for adding funcx and globus endpoints
```
   # funcx endpoints
    endpoints={
        "theta" : "xxxx"
    }

    # globus endpoints
    globus_endpoints={
        'midway' : 'yyyy',
        'theta'  : 'zzzz'
    }
```
  
4. Execite automation script. Example command is as follows
```
$ nohup python automation.py -m 'theta' -p '~/modis-downloads' -f 'modis-wget.bash' -y 2011 -b '/home/tkurihana/scratch-midway2/data' \
                            -C nohup bash -s 1 100 200 300 -e 99 199 299 365 \
                            --transfer \
                            --sourcepathdir /home/tkurihana/scratch-midway2/data/MOD021KM/2011 \
                            --destpathdir /eagle/Clouds/C6.1/L1/MOD021KM/2011
```
