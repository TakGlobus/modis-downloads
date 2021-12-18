#!/bin/bash
from=`printf "%03d" $1`
to=`printf "%03d" $2`
file=OUT_${from}_${to}.log
echo 'Writing log to' $file

## change before run
product='MOD06_L2'
year='2011'
token=" Get token from https://ladsweb.modaps.eosdis.nasa.gov/"
savedir='.'

for ((i=$1;i<=$2;i++)); do
 index=`printf "%03d" $i`
 echo $index
 echo 'Processing' 61/${product}/${year}/${index} >> $file
 date >> $file
 # -c means "continue" [do not get if already there and same size]
 # -nv means "not verbose"
 time wget --timeout=0 -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/${product}/${year}/${index}/" --header "Authorization: Bearer ${token}" -P  ${savedir}
done