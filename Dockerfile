# Description: Dockerfile for the gentroutils package
#
# To run locally, you must have a credentials file for GCP. Assuming you do,
# you can run the following command:
#
# docker run -v /path/to/credentials.json:/app/credentials.json -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json gentroutuls -s gwas_catalog_release

FROM python:3.13.1-alpine3.21
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ADD . /app

WORKDIR /app
RUN uv sync --frozen

ENTRYPOINT ["uv", "run", "gentroutils"]
