# VAN apartment data scrape

## TL;DR

the following will run the whole process. Details below

```bash
source activate van
cd ~/code/van_apartment_canvass
python apartmentcanvass.py --fvancredentials /path/to/van_credentials.yaml --fdbcredentials /path/to/db_credentials.yaml
source deactivate van
```

The credentials files are obviously secret and permission to access them should be controlled on the OS level (i.e. only the permissioned user can read them).


## Details
;
--------------------------------------------------------------------------------
The general idea here is that either

1. a text file of addresses (one address per line), or
2. a data base query returning a list of addresses

will be loaded by a python script (`apartmentcanvass.py`) which will then parse all VAN data for those addresses.

I have set up a `conda` environment called `van` for use in this project. To activate it, simply type

```bash
source activate van
```

At any point you can check out the options for the python script by calling (from the command line):

```bash
python apartmentcanvass.py --help
```

The defaults provided are almost always what you want to use, but you *must* provide the path to a credentials file for the van website (`--fvancredentials`), and if you are interacting with the MUBS db (the default behavior) you *must* provide a path to the credentials file for that database (`--fdbcredentials`).