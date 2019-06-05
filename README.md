# myworkspace

The Repository contains the completed "Big Data Technical Assignment"


--------------------------------------------------------------
# Below steps need to be followed for running this assignment
--------------------------------------------------------------
1) Clone the repository

2) open terminal and change dir to : ./assignment/docker-compose-config/es-and-kibana and run the command : docker-compose up -d

3) go to directory : /assignment/code. This directory contains below files 
	1) process_crimedata_and_load_to_es.ipynb : python Jupyter notebook containinng the code.
	2) process_crimedata_and_load_to_es.py : python file which can be executed with the command "python process_crimedata_and_load_to_es.py"
	3) process_crimedata_and_load_to_es.html :  Executed pyhton notebook with all output cells


	I have used python version 3.7 setup within a conda env for developing code for this assignment 
	Below is the list of all the different packages that are used in the python code
		=> from elasticsearch import Elasticsearch
		=> from elasticsearch import helpers
		=> from pathlib import Path
		=> import tarfile
		=> import shutil
		=> import os
		=> import pandas as pd
		=> import numpy as np

	I have placed the conda env spec file in location : /assignment/conda-env/spec-file.txt and you can use the
	command : "conda create --name myenv --file spec-file.txt" to create a identical python environment.

----------------------------------------------------
# Below are high level design details of the process. 
# The python notebook contains detailed information
# about each step of the process. In case jupyter notebook
# env is not available I will suggest to go thorough the html 
# version of the notebook in path: 
#        /assignment/code/process_crimedata_and_load_to_es.html
----------------------------------------------------

1) The source dir : "./assignment/problem-statement-artifacts/data" is considered to be the source data dir 
	for the application and when the process is triggered it will pull all the "*.tar.gz" files
	from this dir and copy the files to indir dir: "./assignment/data/indir". These files are then unzipped
	into the indir. The source dir is supposed to contain the tar files for each month seperately
	(I had to follow this convention as otherwise single zip file of all 3 month data was exceeding
	the git upload limit of 100 MB for a single file).

2) After the files are copied into indir the process picks all the files ending in "-street.csv" and places all 
	these files into the procdir dir : "./assignment/data/procdir/street" and similarly the process picks up 
	all the "-outcomes.csv" files from indir and copies these files into the procdir : 
	"./assignment/data/procdir/outcome". While coping these files to procdir (both street and outcome files)
	the process adds line numbers in all the files. This line number is used during the step of removing duplicates
	on "Crime ID" to retain the record with highest line number in case the data file of same month has more than
	one entry for the same "Crime ID".

3) The data from the procdir of street and outcome are loaded into 2 seperate pandas dataframes. We convert the Month from
	string representation of (2019-01) to Integer repre3sentation (201901). After We perform the operation
	of remove duplicate on both the dataframes. For remove duplicate we sort in ascending order both these dataframes on
	columns : "Crime ID, District name, Month, line number(added in step 2)" and retain only the last record. This ensures 
	that the record with latest outcome present in most recent month is picked up. The line number in the sort operation
	ensures that in case of multiple entries in the same month file the record which is present later in the file is picked
	for reporting the last outcome status. We drop all for which CrimeId is not available from both the dataframes.

4) We then perform left outer join of both the dataframes of street and outcome and calculate the lastOutcome column value based 
	on the logic mentioned in the assignment document.

5) The final dataframe created in step 4 is then used to upload the data to the elastic search. We also store a copy of this final
	dataset into outdir: /assignment/data/outdir into a csv file.
