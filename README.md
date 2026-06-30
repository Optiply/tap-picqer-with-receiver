# tap-picqer

`tap-picqer` is a Singer tap for Picqer, with Webhook Support via extension dependency.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```bash
pipx install tap-picqer
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities is available by running:

```bash
tap-picqer --about
```

### Environment Variables

This tap will automatically import any environment variables within the working directory's
`.env` if `--config=ENV` is provided.

### Authentication

Set the `api_key` and `org` config values. Optionally configure `firestore_extension` for Firestore incremental sync.

## Usage

Run `tap-picqer` standalone or in a pipeline via [Meltano](https://meltano.com/).

```bash
tap-picqer --version
tap-picqer --help
tap-picqer --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Setup

```bash
pipx install poetry
poetry install
```

### Tests

```bash
poetry run pytest
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more details.
