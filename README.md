# gentroutils

Set of Command Line Interface tools to process Open Targets Genetics GWAS data.

## Installation

TBA

## Available commands

To see all available commands after installation run

```{bash}
gentroutils --help
```

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
