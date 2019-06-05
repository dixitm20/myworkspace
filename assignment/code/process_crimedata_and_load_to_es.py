#!/usr/bin/env python
# coding: utf-8

# # Copy The Tar Data Files For Each Month(2019-01 To 2019-03) To The Input Data Dir & Untar Those Files.

# In[1]:


from pathlib import Path
import tarfile
import shutil
import os

# Set The Path From Where Process Will Pick The Input Data Files
# It Must Be An Existing Path
srcDataDir = Path('../problem-statement-artifacts/data/').resolve()

# Set The Directory Paths
inputDataDir = Path('../data/indir/').resolve()
procDataDir = Path('../data/procdir/').resolve()
outDataDir = Path('../data/outdir/').resolve()



# If The Directory Already Exist
# Clear The inputdir,procdir And outdir Dir (In Real Life Scenario We May Want To Archive These Dirs ).
if inputDataDir.is_dir():
    shutil.rmtree(str(inputDataDir))
if procDataDir.is_dir():
    shutil.rmtree(str(procDataDir))
if outDataDir.is_dir():
    shutil.rmtree(str(outDataDir))

    
# Create The indir,procdir And outdir Directory If They Do Not Already Exists
Path('../data/indir').mkdir(parents=True, exist_ok=True)
Path('../data/procdir').mkdir(parents=True, exist_ok=True)
Path('../data/outdir').mkdir(parents=True, exist_ok=True)


# Print the Info
print('Source Data Artifacts Dir (This Is Our Source System From Where We Will  Pull The Raw Input Files): \n\t{}'.format(srcDataDir))

print('\nInput Data Dir (Contains Raw Input Files): \n\t{}'.format(inputDataDir))

print('\nData Proc Dir (Raw Files Are Oraganized Into Folders Which Will Be Used  For Processing These Files): \n\t{}'.format(procDataDir))

print('\nData Out Dir (Contains Final Formated Output Data Files Which Will be  Loaded Into Elastic Search): \n\t{}'.format(procDataDir))


# Copy The Tar Files To Input Dir
for file in [file for file in srcDataDir.glob('**/*.gz') if file.is_file()]:
    shutil.copy(str(file), str(inputDataDir))
    
# Untar the Files In Input Dir And Delete The Tar Files
for tfile in [file for file in inputDataDir.glob('**/*.gz') if file.is_file()]:
    tarfile.open(str(tfile)).extractall(str(inputDataDir))
    os.remove(str(tfile))


# # Check The Files In The indir

# In[2]:


for path,dirs,files in os.walk(str(inputDataDir)):
    print('\n{}'.format(path))
    for f in files:
        print('\t{}'.format(f))


# # Copy The Files Into The procdir And Group Files Of Different Months Into 2 Folders. One Folder street (contains all street files from all months) & Other Folder outcome(all outcome files from all months are placed here)
# 
# 
# # As We Copy The Files We Also Keep A Running Total Of Count Of Number Of Records In Each Category (street & outcome) Of Files. We Will Use This Count Later To Reconcile The Record Count Once The Data Is Loaded Into The Pandas Dataframe
# 
# 
# # We also add a column name Rownum containing the line number to each file. We will be using this Rownum to remove Duplicate Records on "Crime ID" which are present within a file.

# In[3]:


# Create The Folders street & outcome In The Procdir
Path(str(procDataDir) + '/street/').mkdir(parents=True, exist_ok=True)
Path(str(procDataDir) + '/outcome/').mkdir(parents=True, exist_ok=True)

# Set The Path Info
procDirStreet=Path(str(procDataDir) + '/street/').resolve()
procDirOutcome=Path(str(procDataDir) + '/outcome/').resolve()

#Print The procdir Subdirtory Details
print('Path For All street Files : \n\t{}'.format(procDirStreet))
print('\nPath For All outcome Files : \n\t{}'.format(procDirOutcome))

# Capture Total Record In street & outcomes Files For Reconciliation
totalStreetRecCount=0
totalOutcomesRecCount=0

# Copy All The street Data Files From inputDataDir To procDirStreet After adding Rownum to each line
for file in [file for file in inputDataDir.glob('**/*-street.csv') if file.is_file()]:
    rownum=0
    with open(str(file), 'r') as recs:
        data = recs.readlines()
    
    with open(str(procDirStreet) + '/' +os.path.basename(file), 'w') as recs:
        for (rownum, line) in enumerate(data):
            if rownum == 0:
                recs.write('{},{}'.format('Rownum', line))
            else:
                recs.write('{},{}'.format(rownum, line))

    # Get number of lines in the present file
    curr_file_num_lines = rownum
    totalStreetRecCount += curr_file_num_lines

    
# Copy All The outcomes Data Files From inputDataDir To procDirOutcome
for file in [file for file in inputDataDir.glob('**/*-outcomes.csv') if file.is_file()]:
    rownum=0
    with open(str(file), 'r') as recs:
        data = recs.readlines()
    
    with open(str(procDirOutcome) + '/' +os.path.basename(file), 'w') as recs:
        for (rownum, line) in enumerate(data):
            if rownum == 0:
                recs.write('{},{}'.format('Rownum', line))
            else:
                recs.write('{},{}'.format(rownum, line))

    # Get number of lines in the present file
    curr_file_num_lines = rownum
    totalOutcomesRecCount += curr_file_num_lines
    
    
print('\n\nTotal street Record Count From All Files: {}'.format(totalStreetRecCount))
print('\nTotal outcomes Record Count From All Files: {}'.format(totalOutcomesRecCount))


# # Check The Files In The procdir

# In[4]:


for path,dirs,files in os.walk(str(procDataDir)):
    print('\n{}'.format(path))
    for f in files:
        print('\t{}'.format(f))


# # Load The File Data Into Pandas Dataframes

# In[5]:


import pandas as pd
import numpy as np

# List All the street And outcomes
streetfileList=[str(file) for file in procDirStreet.glob('**/*-street.csv') if file.is_file()]
outcomefileList=[str(file) for file in procDirOutcome.glob('**/*-outcomes.csv') if file.is_file()]
 
# Map function which will read the csv file and also add one more column named "District name" to 
# the output dataframe
def read_csv_with_districtname_from_file(inputfile):
    outdf=pd.read_csv(inputfile)
    filename=os.path.basename(inputfile)
    districtname=filename[8:].replace('-street.csv','').replace('-outcomes.csv','')
    outdf['District name']=districtname
    return outdf
   
    
# Load The Data Into The Pandas Dataframes
dfStreet = pd.concat(map(read_csv_with_districtname_from_file, streetfileList))
dfOutcomes = pd.concat(map(read_csv_with_districtname_from_file, outcomefileList))


# # Keep Columns Which Are Required

# In[6]:


print('street data column list before dropping extra cols: \n{}'.format(dfStreet.columns))
print('\noutcome data column list before dropping extra cols: \n{}'.format(dfOutcomes.columns))

dfStreet=dfStreet[['Rownum', 'Crime ID', 'Month', 'Longitude', 'Latitude', 'Crime type', 'Last outcome category', 'District name']]
dfOutcomes=dfOutcomes[['Rownum', 'Crime ID', 'Month', 'Longitude', 'Latitude', 'Outcome type', 'District name']]

print('\n\nstreet data column list after dropping extra cols: \n{}'.format(dfStreet.columns))
print('\noutcome data column list after dropping extra cols: \n{}'.format(dfOutcomes.columns))


# # Reconcile the Record Counts In street & outcomes Files With The Counts Loaded In The dataframes

# In[7]:


if dfStreet.shape[0] == totalStreetRecCount:
    print('SUCCESS: Record Count Match Between dfStreet dataframe: {} & Total Street Records: {}'.format(dfStreet.shape[0],totalStreetRecCount))
else:
    print('\nFATAL: Record Count Mismatch Between dfStreet dataframe: {} & Total Street Records: {}'.format(dfStreet.shape[0],totalStreetRecCount))
    
if dfOutcomes.shape[0] == totalOutcomesRecCount:
    print('\nSUCCESS: Record Count Match Between dfStreet dataframe: {} & Total Street Records: {}'.format(dfOutcomes.shape[0],totalOutcomesRecCount))
else:
    print('\nFATAL: Record Count Mismatch Between dfStreet dataframe: {} & Total Street Records: {}'.format(dfOutcomes.shape[0],totalOutcomesRecCount)) 
    


# # Apply Schema To The DataFrames
# 
# # Convert Month From string value: 2019-01 to integer: 201901

# In[8]:


commonColunsInStreetAndOutcomes=[]

print('Columns In street Dataframe: {}'.format(dfStreet.columns.shape[0]))
print('-----------------------------')
for col in dfStreet.columns:
    if col in ['Latitude','Longitude']:
        dfStreet[col]=dfStreet[col].astype(float)
    elif col in ['Month','Rownum']:
        if 'int' not in str(dfStreet[col].dtype):
            dfStreet[col]=dfStreet[col].str.replace('-','').astype(int)
    else:
        dfStreet[col]=dfStreet[col].astype(str)
    print('Column Name: {}, Data Type: {}'.format(col,dfStreet[col].dtype))

    
print('\nColumns In outcome Dataframe: {}'.format(dfOutcomes.columns.shape[0]))
print('-----------------------------')
for col in dfOutcomes.columns:
    if col in ['Latitude','Longitude']:
        dfOutcomes[col]=dfOutcomes[col].astype(float)
    elif col in ['Month','Rownum']:
        if 'int' not in str(dfOutcomes[col].dtype):
            dfOutcomes[col]=dfOutcomes[col].str.replace('-','').astype(int)
    else:
        dfOutcomes[col]=dfOutcomes[col].astype(str)
    print('Column Name: {}, Data Type: {}'.format(col,dfOutcomes[col].dtype))
    if col in dfStreet.columns:
        commonColunsInStreetAndOutcomes.append(col)
        

print('\nCommon Columns In street & outcome Dataframe: {}'.format(len(commonColunsInStreetAndOutcomes)))
print('-----------------------------')
for col in commonColunsInStreetAndOutcomes:
    print(col)


# # Peek Into The Data Loaded in Dataframes

# In[9]:


print('Shape Of The street Dataframe: {}'.format(dfStreet.shape))
dfStreet.head()


# In[10]:


print('Shape Of The street Dataframe: {}'.format(dfOutcomes.shape))
dfOutcomes.head()


# # Clean the street & outcomes Dataframe by following the below stops:
# 
# # 1) Removing duplicates on 'Crime ID','District name'. We retain the record with the maximum value of Month and In case we have more than one entry for the same 'Crime ID','District name' In the same month then we retain the record which has highest line number in a given file.
# 
# # 2) Delete the records from both the datasets where 'Crime ID' is not available
# 
# # 3) Keep only those columns which are needed in the final output dataset

# In[11]:


rmd_dfStreet=dfStreet.sort_values(['Crime ID','District name','Month','Rownum']).drop_duplicates(['Crime ID'],'last')

rmd_clean_dfStreet=rmd_dfStreet[rmd_dfStreet['Crime ID'] != 'nan']

rmd_clean_dfStreet=rmd_clean_dfStreet[['Crime ID', 'District name', 'Latitude'                                       , 'Longitude', 'Crime type', 'Last outcome category']]

print('Shape Of street dataset Before remove duplicate & nan drop on Crime Id: {}'      .format(dfStreet.shape))
print('Shape Of street dataset After remove duplicate & nan drop on Crime Id: {}'      .format(rmd_clean_dfStreet.shape))

rmd_clean_dfStreet.head()


# In[12]:


rmd_dfOutcomes=dfOutcomes.sort_values(['Crime ID','District name','Month','Rownum']).drop_duplicates(['Crime ID'],'last')

rmd_clean_dfOutcomes=rmd_dfOutcomes[rmd_dfOutcomes['Crime ID'] != 'nan']

rmd_clean_dfOutcomes=rmd_clean_dfOutcomes[['Crime ID','Outcome type']]

rmd_clean_dfOutcomes['Join_Flag']=1

print('Shape Of outcomes dataset Before remove duplicate & nan drop on Crime Id: {}'      .format(dfOutcomes.shape))
print('Shape Of outcomes dataset After remove duplicate & nan drop  on Crime Id: {}'      .format(rmd_clean_dfOutcomes.shape))

rmd_clean_dfOutcomes.head()


# # Join The street & outcomes clean datasets to produce the outut dataset

# In[13]:


dfStreetXoutcomes = pd.merge(rmd_clean_dfStreet, rmd_clean_dfOutcomes,                    how='left', left_on=['Crime ID'], right_on = ['Crime ID'])


# # Print stats about the matched and unmatched records

# In[14]:


print('Total Count Of Distinct Crime ID present in the street dataset: {}'      .format(rmd_clean_dfStreet.shape[0]))

print('Crime ID in street which found match in outcomes: {}'      .format(dfStreetXoutcomes[dfStreetXoutcomes['Join_Flag'] ==1].shape[0]))

print('Crime ID in street which did not found match in outcomes: {}'      .format(dfStreetXoutcomes[dfStreetXoutcomes['Join_Flag'] !=1].shape[0]))

print('\nShape of the join dataset: {}'.format(dfStreetXoutcomes.shape))
print('\nColumns of the join dataset: {}'.format(dfStreetXoutcomes.columns))
dfStreetXoutcomes.head()


# # Derive lastOutcome column based on the join_flag

# In[15]:


dfStreetXoutcomes['lastOutcome']=np.where(dfStreetXoutcomes['Join_Flag']==1,                                 dfStreetXoutcomes['Outcome type'], dfStreetXoutcomes['Last outcome category'])


# # Peek at the output to verify the lastOutcome column derivation

# In[16]:


dfStreetXoutcomes.head()


# # Rename the columns and as per the final output dataset and keep only the required columns

# In[17]:


dfStreetXoutcomes=dfStreetXoutcomes[['Crime ID','District name','Latitude','Longitude','Crime type','lastOutcome']]

dfStreetXoutcomes = dfStreetXoutcomes.astype(object).replace(np.nan, 'none')
dfStreetXoutcomes = dfStreetXoutcomes.astype(object).replace('nan', 'none')

dfFinalOut4ES=dfStreetXoutcomes.rename(index=str, columns={'Crime ID': 'crimeId','District name': 'districtName'                ,'Latitude': 'latitude','Longitude': 'longitude','Crime type': 'crimeType'})


# # Print Stats about the Final dataset

# In[22]:


print('Shape Of the Final Dataset (Ready For Load Into Elastic Search: {}'      .format(dfFinalOut4ES.shape))

print('\nColumns Of the Final Dataset (Ready For Load Into Elastic Search: \n{}'      .format(dfFinalOut4ES.columns))

dfFinalOut4ES.head()


# # Write The final output dataset to file

# In[19]:


dfFinalOut4ES.to_csv(str(outDataDir)+'/out-dataset-for-elasticSearch-upload.csv')


# # Load The Data To Elastic Search

# In[70]:


from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch(http_compress=True)


# In[71]:


def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": 'crimedetails',
                "_type": "_doc",
                "_id" : f"{document['crimeId']}",
                "_source": document[['districtName','latitude','longitude','crimeType','lastOutcome']].to_dict(),
            }
    


# In[ ]:


# Convert the float types to string to avoid number format exception 
# while loading the data to elastic search
dfFinalOut4ES['latitude']=dfFinalOut4ES['latitude'].astype(str)
dfFinalOut4ES['longitude']=dfFinalOut4ES['longitude'].astype(str)


helpers.bulk(es, doc_generator(dfFinalOut4ES))

