#!/bin/sh
# This script needs to recide in pdda_raw/10_state_cancer_profile directory
# This script gets all the filenames from pdda_raw/10_state_cancer_profile and generates one file inc_state_cancer_profile.csv
# Create a header for the file with the added column Democode which stands for the name of each file
# Store this header in a file inc_state_cancer_profile.csv which will be the final file
# In each file, delete first 7 and all line after the occurance of "Notes:" till the end of the file
# In each file add a column towards the end and value as filename for all the rows
# Keep appending the file inc_state_cancer_profile.csv

# ¶ = /xb6, § = /xa7

set -e
if [ $# -lt 2 ]; then
  echo "USAGE: $0 listOfLinks dataDir"
  exit 1
fi

filenames=$(awk '$2~/^Inc/{print $2}' $1)

echo "County,State,FIPS,Annual_Incidence_Rate,Lower_CI,Upper_CI,Average_Cases_per_Year,Rate_Period,Interval_Range,Democode" > inc_state_cancer_profile.csv
for f in ${filenames}
do
  echo $f
  awk -F ',' 'BEGIN{OFS=","}NR>7{sub(".*/", "", FILENAME); print $0,FILENAME}' $2/$f |perl -pe 's/[\xb6\xa7]//g' >> inc_state_cancer_profile.csv
  LANG=C sed -i '' '/Notes:/,$d' inc_state_cancer_profile.csv
done
