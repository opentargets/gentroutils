# gentroutils

[![Tests](https://github.com/opentargets/gentroutils/actions/workflows/test.yaml/badge.svg?event=push)](https://github.com/opentargets/gentroutils/actions/workflows/test.yaml)
![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)

Set of Command Line Interface tools to process Open Targets Genetics GWAS data.

## Installation

```
pip install gentroutils
```

## Available commands

To see all available commands after installation run

```{bash}
gentroutils --help
```

### Updating gwas catalog metadata

To update gwas catalog metadata run folliwing command

```bash
gentroutils  -vvv -q gs://ot_orchestration/tests/gentroutils/log.txt  update-gwas-curation-metadata \
-f ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations_ontology-annotated.tsv gs://ot_orchestration/tests/gentroutils/gwas-catalog-associations_ontology-annotated.tsv \
-f ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-studies-v1.0.3.1.txt gs://ot_orchestration/tests/gentroutils/gwas-catalog-download-studies-v1.0.3.1.txt \
-f ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-ancestries-v1.0.3.1.txt gs://ot_orchestration/tests/gentroutils/gwas-catalog-download-ancestries-v1.0.3.1.txt \
-g https://www.ebi.ac.uk/gwas/api/search/stats
```

The command `update-gwas-curation-metadata` fetches the data from the ftp server and transfers them to the gcp without intermediate temporary files. The download(s) and upload(s) are made asyncronously.

The logs from the command are saved under the `-q` log file, if specified `gcp` log file, then the file will be uploaded after the command has run.

To test the command run it with `-d` == `--dry-run`, this will just mark the input and output destinations.
To allow for full logs to be transmitted to the log file, use `-vvv` to increase the verbosity of the logs

> [!NOTE]
> Change the path to the output `gcp` files to make sure they are saved under requested path

> [!WARNING]
> Please read before running the command!:
>
> * The above command has some default values set for the input and output files, make sure you test them in `--dry-run` so the existing files will not get overwritten!
> * Make sure to run `gcloud auth application-default login` to allow to use Google Cloud Python SDK before running the command

## Contribute

To be able to contribute to the project you need to set it up. This project
runs on:

- [x] python 3.10.8
- [x] rye (package manager)
- [x] uv (dependency manager)

To set up the project run

```{bash}
make dev
```

The command will install above dependencies (initial requirements are curl and bash) if not present and
install all python dependencies listed in `pyproject.toml`. Finally the command will install `pre-commit` hooks
requred to be run before the commit is created.

The project has additional `dev` dependencies that include the list of packages used for testing purposes.
All of the `dev` depnendencies are automatically installed by `rye`.

To see all available dev commands

Run following command to see all available dev commands

```{bash}
make help
```

### Manual testing of CLI module

To check CLI execution manually you need to run

```{bash}
rye run gentroutils
```
