# Updating the AGAGE public archive

This repository uses two types of version control:
- git is used to track versions of the code
- [dvc](https://dvc.org) is used to track versions of the processed data

Make sure that you have git installed on your system before you start.

The dvc and dvc-gcloud (Google Cloud plugin) are listed in the requirements file, and can be installed from conda, using the conda-forge channel.

## Initial setup

It will probably be easiest to create a new conda environment, ```conda conda create --name agage_env``` then install ```pip``` using ```conda install pip```.
Alternatively, if avoiding conda, install a virtual environment using ```python -m venv agage_env``` and activate this using ```source agage_env/bin/activate```.

Make sure that you have installed the required dependencies (see ```requirements.txt```), which can be done using ```pip install -r requirements.txt```.

Allow the package to be callable using ```pip install --no-build-isolation --no-deps -e . ``` and run ```python agage_archive/config.py```, to set the desired input and output paths.
If using conda, you can use ```conda develop .``` but note that this functionality is now depricated.

### DVC initial setup (*optional: only if you want to sync processed files with other users*)

**Note: This step should now be done from a separate repository that calls ```agage-archive``` as a dependency. Keeping documentation here for now.**

Firstly, check that DVC is working by running from the terminal:

```dvc status```

It should say something like:

```
data/agage/agage-public-archive.zip.dvc:                              
        changed outs:
                not in cache:       data/agage/agage-public-archive.zip
```

The first time you use DVC, you will need to provide some credentials for the remote file hosting. We are using Google Drive for this. We are using a Google Drive service account.  **These credentials must not be shared outside of the AGAGE team. Please ensure that the authentication file is NOT committed to the repository or shared online.**

You will need to contact Matt Rigby for the authentication json file. Put this file somewhere on your local machine. You can put it in the repository folder if you like, where it will be ignored by the version control (assuming that you don't change the name).

Run the following command from a terminal in the repository directory, replacing ```/path/to``` with the path to the location of the json file:

```dvc remote modify myremote --local gdrive_service_account_json_file_path /path/to/agage-gdrive.json```

Note that the ```--local``` flag is extremely important here. It adds a file ```.dvc/config.local``` containing the location of the authentication file. This local config file is excluded from the repository.

**If the ```agage-gdrive.json``` file is accidentally pushed to the repository or shared outside of the AGAGE team, please contact Matt Rigby immediately.**

You will find out whether the authentication has worked the first time you do ```dvc pull```.


#### Note to admin

Authentication settings are through the [Google Drive API](https://console.developers.google.com/). See instructions on authentication and service accounts on the [DVC gdrive docs](https://dvc.org/doc/user-guide/data-management/remote-storage/google-drive).

Need to set:

```dvc remote modify myremote gdrive_use_service_account true```

Which should then be in .dvc/config

## Usage

Firstly, make sure that the git repository is up to date (make sure you know how to use git, but assuming you do, just do ```git pull``` to get the latest version).

If you want to update the archive:

1. This library must be called as a dependency from a downstream archive with all of the relevant data specification files. This ensures that the code and data versioning can be independent. A template of a data specification repository is provided at https://github.com/AGAGE-atmosphere/agage-archive-template. 
2. Download the latest data from GCWerks, and place it at the locations specified in your ```config.yaml``` file.
3. Modify the data specification files as required, in ```data/agage``` (see section Data Specification Files)`:
```
data_release_schedule/
data_combination/
data_exclude/
scale_defaults.csv
```
4. For each instrument, specify which read function should be used in ```data/NETWORK/data_read_functions.json```. Current options are ```read_nc``` (main GCWerks netcdf files), ```read_gcwerks_flask``` (GCWerks flask measurement output format), ```read_ale_gage``` (only for ALE/GAGE data in the GA Tech 1994 format), ```read_gcms_magnum``` (only for reading MHD GCMS ``Magnum'' files). These functions are in ```agage_archive.io```. 
5. Re-run ```run.py```, or any other processing functions that you'd like to run. 
6. Thoroughly check the processed data files by examining the netcdf files stored in the newly created ```data/agage/agage-public-archive.zip``` (the functions in ```io.py``` can automatically extract files from the zip archive, given the appropriate site, instrument, etc.), and using the ```notebooks/visualise.ipynb``` data viewer.
7. (*Optional*) Once you're happy, run: 
```
dvc add data/agage/agage-public-archive.zip
git commit -am "COMMENT DESCRIBING THE CHANGE YOU'VE MADE"
dvc push
git push
```

## Copying relevant input files

You will need to copy over the GCWerks output folders ```data-nc/md```, ```data-nc/gcms``` and ```data-nc/optical``` from whereever you pull the data to your local ```data/archive``` folder (and zip the two folders if not zipped already).

## Data Specification Files

There are four main sets of csv files that tell the code which data to include in the archive and how to combine or process each set of files. These files must be in the ```data/<network>``` folder (e.g., ```data/agage``` for this repository, but if you are using this code to process data from a different network, rename accordingly).

**Important note on date formats**: If you edit the csv files in Excel will try to convert date strings into its own date format. Please don't let it do so. Keep all date cells as strings. 

Data specification files are in the following sub-folders:

### ```data_release_schedule```

These files tells us which species to include in the archive and until which date(s). Individual instrument files will be released until the specified end date. Combined files will contain data for a particular instrument until the date in the release schedule, or until the date specified in the relevant ```data_combination``` file, whichever comes first.

**Each species that should be released from a site needs to be listed in this file. If it's not in here, it won't be included in the archive.**

Each csv file is for a different instrument. A separate file will be needed for each instrument name that appears in the GCWerks output filenames (e.g., GCMD, Picarro-1, Picarro-2, etc.).

Within each file, there is a cell for a "General release date". This date applies to every compound at every site, unless a date is specified in the table.

The table specifies the species- and site-specific release date. An "x" in this table means that a particular species will not be released at a site. A date in this column overrides the general release date, and this species/site will only be released until the specified date.

### ```data_combination```

These csv files specify when to switch from one instrument to another at each site and for each species. If a species does not appear in this table, no combined file is produced. 

There is one file per site (denoted by the three-letter site code). Rows are the standardised species names (see the ```scale_defaults.csv``` file for the definitive list). The column headers have the format ```<instrument> start``` and ```<instrument> end``` (e.g., ```GCMD start```, ```GCMD end```). Use these columns to specify when a particular instrument starts and ends:
- An "x" means that no data from this instrument should be used
- An empty cell means that all data should be used (subject to the end date in the release schedule)
- A date in the start column specifies the starting date for a particular instrument. Similarly, end date.

**Interleaving data**: Sometimes, we may want to interleave one instrument and another. E.g., there is a period where instrument 1's precision is poor, so we should use instrument 2, but then bring instrument 1 back again later. The code isn't directly set up to do this. However, this behaviour can be achieved by setting overlapping start and end dates for two instruments, and then using a ```data_exclude``` file to remove certain periods from one or more instrument (see below).

### ```data_exclude```

These files specify periods to remove data from the archive. This can occur when data has been left in the input files, but should have been flagged, or when we want to interleave two instruments.

There is one csv file per site. If a site is not present, no data is excluded. One row is required per period to exclude for each species and instrument. Any data between the start and end dates is excluded (UTC, inclusive). Note that this is the time as it appears in the archive files, which may have been adjusted from the GCWerks time, for any lag due to the sampling period.

Use the "Notes" column to explain why this particular block of data is being omitted from the archive.

If you only want to exclude data from the combined data files (e.g., if you want to interleave two datasets, but both are "good" data, which should appear in the individual instrument files), place a "Y" in the "Combined_only" column (which can be added for a site, if it is not present).

### ```scale_defaults.csv```

Use this file to specify a calibration scale for each species. The ```scale_defaults.csv``` file is used for the combined files and each, instrument by default. However, if data for a particular instrument should be released in the individual instrument files on a different scale, this can be specified in ```scale_defaults-<instrument>```. Note that a family of instruments can be used in the filename, and it is not necessary to specify a file for each individual instrument (e.g., "Picarro" will be used by any instrument called "Picarro-1", "Picarro-2", etc.).

Any required scale conversion will be carried out using the [OpenGHG calscales](https://github.com/openghg/openghg_calscales) library. If a conversion factor is missing in there, raise an issue, and suggest a conversion factor/function.

## Archive structure

For details on the file structure of the archive, see the README that gets included in the archive (stored under ```data/NETWORK/README.md```). A changelog will also be included in your archive, if you use ```data/NETWORK/CHANGELOG.md```. 

The archive is organised by species. At the top level are the combined data records, or, if a species is only measured on one instrument at a site, the individual instrument file. In a subfolder called "individual-instruments" are the files for each instrument. Other subfolders include the baseline flags or baseline monthly means.