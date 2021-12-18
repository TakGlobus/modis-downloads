# modis-downloads

- automation.py:   
    automated download script. The script can run on remote machines. In this case, add `--transfer` option.  
    you may need to obtain Globus endpoint id, Funcx endpoint id, and LAADS token to run wget script.

- modis-wget.bash:  
    bash script to execute wget command for downloading modis data from NASA LAADS server. 

- requirement.txt:  
    package information. Run `pip install -r requirements.txt`
