# gentroutils

[![checks](https://github.com/opentargets/gentroutils/actions/workflows/pr.yaml/badge.svg?branch=dev)](https://github.com/opentargets/gentroutils/actions/workflows/pr.yaml)
![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)
[![release](https://github.com/opentargets/gentroutils/actions/workflows/release.yaml/badge.svg)](https://github.com/opentargets/gentroutils/actions/workflows/release.yaml)

Set of Command Line Interface tools to process Open Targets Genetics GWAS data.

## Installation

```{bash}
pip install gentroutils
```

## Available commands

To see all available commands after installation run

```{bash}
gentroutils --help
```

## Usage

To run a single step run

```{bash}
uv run gentroutils -s gwas_catalog_release  # After cloning the repository
gentroutils -s gwas_catalog_release -c otter_config.yaml # When installed by pip
```

The `gentroutils` repository uses the [otter](https://github.com/opentargets/otter) framework to build the set of tasks to run. The current implementation of tasks can be found in the `config.yaml` file in the root of the repository. To run gentroutils installed via `pip` you need to define the otter config that looks like the `config.yaml` file.

<details>
<summary>Example config</summary>

For the top level fields refer to the [otter documentation](https://opentargets.github.io/otter/otter.config.html)

> [!NOTE]
> All `destination_template` must point to the Google Cloud Storage (GCS) bucket objects.
> All `source_template` must point to the FTP server paths.
> In case this is not enforced, the user may experience silent failures.

```yaml
---
work_path: ./work
log_level: DEBUG
scratchpad:
steps:
  gwas_catalog_release:
    - name: crawl release metadata
      stats_uri: "https://www.ebi.ac.uk/gwas/api/search/stats"
      destination_template: "gs://gwas_catalog_inputs/gentroutils/{release_date}/stats.json"
      promote: "true"
    - name: fetch associations
      stats_uri: "https://www.ebi.ac.uk/gwas/api/search/stats"
      source_template: "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/{release_date}/gwas-catalog-associations_ontology-annotated.tsv"
      destination_template: "gs://gwas_catalog_inputs/gentroutils/{release_date}/gwas_catalog_associations_ontology_annotated.tsv"
      promote: true
    - name: fetch studies
      stats_uri: "https://www.ebi.ac.uk/gwas/api/search/stats"
      source_template: "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/{release_date}/gwas-catalog-download-studies-v1.0.3.1.txt"
      destination_template: "gs://gwas_catalog_inputs/gentroutils/{release_date}/gwas_catalog_download_studies.tsv"
      promote: true
    - name: fetch ancestries
      stats_uri: "https://www.ebi.ac.uk/gwas/api/search/stats"
      source_template: "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/{release_date}/gwas-catalog-download-ancestries-v1.0.3.1.txt"
      destination_template: "gs://gwas_catalog_inputs/gentroutils/{release_date}/gwas_catalog_download_ancestries.tsv"
      promote: true
    - name: curation study
      requires:
        - fetch studies
      previous_curation: gs://gwas_catalog_inputs/curation/latest/curated/GWAS_Catalog_study_curation.tsv
      studies: gs://gwas_catalog_inputs/gentroutils/latest/gwas_catalog_download_studies.tsv
      destination_template: gs://gwas_catalog_inputs/gentroutils/curation/{release_date}/GWAS_Catalog_study_curation.tsv
      promote: true
```

The config above defines the steps that are run in parallel by the `otter` framework.

</details>

### Available tasks

The list of tasks (defined in the `config.yaml` file) that can be run are:

#### Crawl release metadata

```yaml
- name: crawl release metadata
      stats_uri: "https://www.ebi.ac.uk/gwas/api/search/stats"
      destination_template: "gs://gwas_catalog_inputs/gentroutils/{release_date}/stats.json"
      promote: "true"
```

This task fetches the latest GWAS Catalog release metadata from the `https://www.ebi.ac.uk/gwas/api/search/stats` endpoint and saves it to the specified destination.

> [!NOTE]
> **Task parameters**
>
> - The `stats_uri` is used to fetch the latest release date and other metadata.
> - The `destination_template` is where the metadata will be saved, and it uses the `{release_date}` placeholder to specify the release date dynamically. By default it searches for the release directly in the stats_uri json output.
> - The `promote` field is set to `true`, which means the output will be promoted to the latest release. Meaning that the file will be saved under `gs://gwas_catalog_inputs/gentroutils/latest/stats.json` after the task is completed. If the `promote` field is set to `false`, the file will not be promoted and will be saved under the specified path with the release date.

---

### Fetch associations

```yaml
- name: fetch associations
      stats_uri: "https://www.ebi.ac.uk/gwas/api/search/stats"
      source_template: "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/{release_date}/gwas-catalog-associations_ontology-annotated.tsv"
      destination_template: "gs://gwas_catalog_inputs/gentroutils/{release_date}/gwas_catalog_associations_ontology_annotated.tsv"
      promote: true
```

This task fetches the GWAS Catalog associations file from the specified FTP server and saves it to the specified destination.

> [!NOTE]
> **Task parameters**
>
> - The `stats_uri` is used to fetch the latest release date and other metadata.
> - The `source_template` is the URL of the GWAS Catalog associations file, which uses the `{release_date}` placeholder to specify the release date dynamically. The release date is fetched from the `stats_uri` endpoint.
> - The `destination_template` is where the associations file will be saved, and it also uses the `{release_date}` placeholder. The release date is fetched from the `stats_uri` endpoint.
> - The `promote` field is set to `true`, which means the output will be promoted to the latest release. Meaning that the file will be saved under `gs://gwas_catalog_inputs/gentroutils/latest/gwas_catalog_associations_ontology_annotated.tsv` after the task is completed. If the `promote` field is set to `false`, the file will not be promoted and will be saved under the specified path with the release date.

---

### Fetch studies

```yaml
- name: fetch studies
      stats_uri: "https://www.ebi.ac.uk/gwas/api/search/stats"
      source_template: "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/{release_date}/gwas-catalog-download-studies-v1.0.3.1.txt"
      destination_template: "gs://gwas_catalog_inputs/gentroutils/{release_date}/gwas_catalog_download_studies.tsv"
      promote: true
```

This task fetches the GWAS Catalog studies file from the specified FTP server and saves it to the specified destination.

> [!NOTE] 
> **Task parameters**
>
> - The `stats_uri` is used to fetch the latest release date and other metadata.
> - The `source_template` is the URL of the GWAS Catalog studies file, which uses the `{release_date}` placeholder to specify the release date dynamically. The release date is fetched from the `stats_uri` endpoint.
> - The `destination_template` is where the studies file will be saved, and it also uses the `{release_date}` placeholder. The release date is fetched from the `stats_uri` endpoint.
> - The `promote` field is set to `true`, which means the output will be promoted to the latest release. Meaning that the file will be saved under `gs://gwas_catalog_inputs/gentroutils/latest/gwas_catalog_download_studies.tsv` after the task is completed. If the `promote` field is set to `false`, the file will not be promoted and will be saved under the specified path with the release date.

---

### Fetch ancestries

```yaml
- name: fetch ancestries
      stats_uri: "https://www.ebi.ac.uk/gwas/api/search/stats"
      source_template: "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/{release_date}/gwas-catalog-download-ancestries-v1.0.3.1.txt"
      destination_template: "gs://gwas_catalog_inputs/gentroutils/{release_date}/gwas_catalog_download_ancestries.tsv"
      promote: true
```

This task fetches the GWAS Catalog ancestries file from the specified FTP server and saves it to the specified destination.

> [!NOTE] 
> **Task parameters**
>
> - The `stats_uri` is used to fetch the latest release date and other metadata.
> - The `source_template` is the URL of the GWAS Catalog ancestries file, which uses the `{release_date}` placeholder to specify the release date dynamically. The release date is fetched from the `stats_uri` endpoint.
> - The `destination_template` is where the ancestries file will be saved, and it also uses the `{release_date}` placeholder. The release date is fetched from the `stats_uri` endpoint.
> - The `promote` field is set to `true`, which means the output will be promoted to the latest release. Meaning that the file will be saved under `gs://gwas_catalog_inputs/gentroutils/latest/gwas_catalog_download_ancestries.tsv` after the task is completed. If the `promote` field is set to `false`, the file will not be promoted and will be saved under the specified path with the release date.

---

### Curation

```yaml
- name: curation study
      requires:
        - fetch studies
      previous_curation: gs://gwas_catalog_inputs/curation/latest/curated/GWAS_Catalog_study_curation.tsv
      studies: gs://gwas_catalog_inputs/gentroutils/latest/gwas_catalog_download_studies.tsv
      destination_template: gs://gwas_catalog_inputs/curation/{release_date}/raw/gwas_catalog_study_curation.tsv
      promote: true
```

This task is used to build the GWAS Catalog curation file that is later used as a template for manual curation. It requires the `fetch studies` task to be completed before it can run. This is due to the fact that the curation file is build based on the list of studies fetched from `download studies` file.

> [!NOTE]
> **Task parameters**
>
> - The `requires` field specifies that this task depends on the `fetch studies` task, meaning it will only run after the studies have been fetched.
> - The `previous_curation` field is used to specify the path to the previous curation file. This is used to build the new curation file based on the previous one.
> - The `studies` field is the path to the studies file that was fetched in the `fetch studies` task. This file is used to build the curation file.
> - The `destination_template` is where the curation file will be saved, and it uses the `{release_date}` placeholder to specify the release date dynamically. The release date is fetched from the `stats_uri` endpoint.
> - The `promote` field is set to `true`, which means the output will be promoted to the latest release. Meaning that the file will be saved under `gs://gwas_catalog_inputs/curation/latest/raw/gwas_catalog_study_curation.tsv` after the task is completed. If the `promote` field is set to `false`, the file will not be promoted and will be saved under the specified path with the release date.

---

## Curation process

The base of the curation process for GWAS Catalog data is defined in the [docs/gwas_catalog_curation.md](docs/gwas_catalog_curation.md). The original solution uses R script to prepare the data for curation and then manually curates the data. The solution proposed in the `curation` task autommates the preparation of the data for curation and provides a template for manual curation. The manual curation process is still required, but the data preparation is automated.

The automated process includes:

1. Reading `download studies` file with the list of studies that are currently comming from the latest GWAS Catalog release.
2. Reading `previous curation` file that contains the list of the curated studies from the previous release.
3. Comparing the two datasets with following logic:
   - In case the study is present in the `previous curation` and `download studies`, the study is marked as `curated`
   * In case the study is present in the `download studies` but not in the `previous curation`, the study is marked as `new`
   * In case the study is present in the `previous curation` but not in the `download studies`, the study is marked as `removed`
4. The output of the curation process is a file that contains the list of studies with their status (curated, new, removed) and the fields that are required for manual curation. The output file is saved to the `destination_template` path specified in the task configuration. The file is saved under `gs://gwas_catalog_inputs/curation/{release_date}/raw/gwas_catalog_study_curation.tsv` path.
5. The output file is then promoted to the latest release path `gs://gwas_catalog_inputs/curation/latest/raw/gwas_catalog_study_curation.tsv` so that it can be used for manual curation.
6. The manual curation process is then performed on the `gs://gwas_catalog_inputs/curation/latest/raw/gwas_catalog_study_curation.tsv` file. The manual curation process is not automated and requires manual intervention. The output from the manual curation process should be saved then to the `gs://gwas_catalog_inputs/curation/latest/curated/GWAS_Catalog_study_curation.tsv` and `gs://gwas_catalog_inputs/curation/{release_date}/curated/GWAS_Catalog_study_curation.tsv` file. This file is then used for the [Open Targets Staging Dags](https://github.com/opentargets/orchestration).

---

## Contribute

To be able to contribute to the project you need to set it up. This project
runs on:

- [x] python 3.13
- [x] uv (dependency manager)

To set up the project run

```{bash}
make dev
```

The command will install above dependencies (initial requirements are curl and bash) if not present and
install all python dependencies listed in `pyproject.toml`. Finally the command will install `pre-commit` hooks
required to be run before the commit is created.

The project has additional `dev` dependencies that include the list of packages used for testing purposes.
All of the `dev` dependencies are automatically installed by `uv`.

To see all available dev commands

Run following command to see all available dev commands

```{bash}
make help
```

### Manual testing of CLI module

To check CLI execution manually you need to run

```{bash}
uv run gentroutils
```

---

This software was developed as part of the Open Targets project. For more
information please see: http://www.opentargets.org
