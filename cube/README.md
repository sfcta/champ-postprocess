# NET to CSV
`net_convert.s` converts Cube .NET files to CSV (or DBF but just use CSVs as DBFs are larger and more unwieldy) files. Run it by first setting the requisite environmental variables (see the file header comments), and then run `runtpp net_convert.s` on one of the servers.

There's also some Cube scripts in Y:\champ\util\Validation . `net_convert.s` was written with reference to these:
- NETtoCSV_simple.s: straight conversion
- NETtoCSV_TNC.s: conversion plus calculation of additional convenience fields (DA, SR2, SR3, ...., AUTOVOL, PAXVOL, TOTVAL, PCEVOL)

Currently (Jan 2024), the scripts are set to take the loaded networks as input with the following restrictions:
```
# no ` ` or `-` allowed in the path due to Cube restrictions
```

To set environmental variables:
```
$env:CUBENET = "\path\to\LOAD{XX}_FINAL"  # setting env vars on powershell
set CUBENET=\path\to\LOAD{XX}_FINAL  # setting env vars on cmd
```