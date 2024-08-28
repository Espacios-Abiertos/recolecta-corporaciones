# The Global Minimum Tax and its Potential Effects in Puerto Rico

This repository contains the scripts and results of the analyses for Espacios Abiertos' report on the potential impact of the Global Tax in Puerto Rico. These programs conduct tasks such as:
- Collect and filter company information from the Department of State
- Determine companies eligible for Global Tax and its phases (QDMTT, IIR, UTPR)
- Summarize and present the results into tables such as those included in the paper

## Repository structure

There are several folders that organize how data flows around the repository:
- `inputs/`
  - Placeholder for files generated outside this repository that are ingested into scripts
  - Includes miscellaneous datasets such as country metadata and exported Orbis batch results

- `outputs/`
  - Intermediate and final results of the analyses contained within this repository
  - Collection of Excel, CSV, and Parquet files

- `scripts/`
  - Programs that run the analyses and output results in dataset and presentation formats
  - Generally, they are Python scripts which orchestrate a sequence of DuckDB SQL queries

## Prerequisites

If you only need to go through the exported results, you can open the datasets in `outputs/` with your preferred tool. Excel should do for `.xlsx` and `.csv` files, while `.parquet` files can be skimmed using [Tad](https://www.tadviewer.com/). For viewing database files, we recommend [DBeaver](https://dbeaver.io/).

For running scripts, you'll need a Python 3.10.7 and, optionally, the DuckDB CLI and the [Harlequin](https://harlequin.sh/) SQL IDE to go over results manually.

## Installation

Create a new Python virtual environment and install the required packages:

```bash
python -m venv .venv # Create env
source .venv/bin/activate # Mac: Activate env
pip install -r requirements.txt # Get dependencies
```

## Scripts

A subset of the scripts do as follows:

`recolecta_buscador.py`
Collects basic corporation information available from the "Corporations Search" tool of the Department of State. Saves the results into a SQLite database.

`recolecta_corporation_info.py`
Collects detailed corporation information from the Department of State. Saves data into one JSON file per corporation.

`exporta_foraneas_activas.py`
Filters the list of corporations into those that are foreign and have an active status.

`sql/attach_db_readonly.sql`
Attaches the created database of corporation data sourced from the Department of State into a running DuckDB instance.

`merge_orbis_results.py`
Joins all eight exported Orbis batches into one dataset. This includes both the automated pairing results and the matched company results.

`get_orbis_qualified_companies.py`
Filters for companies that may be elligible for Global Tax using the criteria of 750M euros in revenue.

`describe_orbis_corporate_network.py`
Exports the results of the Orbis corporate structure batch into a Parquet file for easier analysis.

`get_orbis_parents_with_filial_countries.py`
Processes the Orbis corporate structure for our companies of interest to determine UTPR on a per-company basis.

`presentation_companies_table.py`
Creates an Excel table of parent companies and their local branches in a presentation-friendly format.

Other scripts:

`get_pr_companies_for_orbis_search.py`
Creates a list of local Puerto Rican companies to search for in Orbis.