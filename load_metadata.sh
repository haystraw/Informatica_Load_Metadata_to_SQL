#!/bin/sh

#########################################################
## Example crontab to automaticallly run every Monday
## 0 13 * * 1 <path>/load_metadata.sh >> <path>/load_metadata.log 2>&1
#########################################################
echo "Starting Script: $(date)"

cd /apps/ec2-user/MISC/compass/Load_Metadata_to_SQL

# Get export_filename from config.ini (assumes [idmc] section)
export_filename=$(sed -n '/^\[idmc\]/,/^\[/{s/^export_filename *= *//p}' config.ini | tr -d '\r')

# Delete .zip and .xlsx if they exist, ignoring errors
rm -f "${export_filename}.zip" "${export_filename}.xlsx"

# Capture the filename output from the python export script
/usr/bin/python3 -u extract_from_idmc.py

/usr/bin/python3 -u load_excel.py



