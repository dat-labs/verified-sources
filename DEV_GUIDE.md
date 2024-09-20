# Development guide

## Introduction

This is a detailed guide to help you develop your own connector for `verified-sources` and ensure that all your tests pass.

### Generate stub files for your verified-sources actor

Run the following command in the `verified-sources` directory

```bash
dat-cli init
```

Follow the onscreen instructions and you will be met with these lines in your terminal output.

```text 
...
verified_sources/{your-source-connector}/specs.yml written.
verified_sources/{your-source-connector}/specs.py written.
verified_sources/{your-source-connector}/source.py written.
verified_sources/{your-source-connector}/tests/conftest.py written.
verified_sources/{your-source-connector}/tests/test_{your-source-connector}.py written.
verified_sources/{your-source-connector}/catalog.yml written.
verified_sources/{your-source-connector}/catalog.py written.
verified_sources/{your-source-connector}/streams.py written.pywritten.
```

Your `verified-sources` directory should look something like this:
```text
verified_sources/
├── {your-source-connector}
│   ├── catalog.py
│   ├── catalog.yml
│   ├── source.py
│   ├── specs.py
│   ├── specs.yml
│   ├── streams.py
│   └── tests
│       ├── conftest.py
│       └── test_{your-source-connector}.py
└── verified_source_0/
    ├── ...
    └── ...
```

We have a detailed description for what each file does under our [docs](http://path/to/docs). Here we are providing a short description of them.

### `specs.yml`

Here, we can add any additional parameters (as keys) that we want our source to have under `connection_specification`.

E.g. If you are developing a Google Drive source actor, you might want to ask for the `client_id`, `client_secret` and `refresh_token` as part of `connection_specification`. You can add it like so:
Example:
```yml
description: Specification of an actor (source/embeddingsgenerator/destination)
type: object
...
file truncated for brevity
...
  connection_specification:
    description: ConnectorDefinition specific blob. Must be a valid JSON string.
    type: object
    additionalProperties: true
    properties:
      client_id:
        type: string
        description: "client id for the project"
        title: "Client ID"
        order: 0
      client_secret:
        type: string
        description: "client secret for the project"
        title: "Client Secret"
        order: 1
      refresh_token:
        type: string
        description: "refresh token for the project"
        title: "Refresh Token"
        order: 2
```

### `specs.py`

This file contains the `specs.yml` file as a [Pydantic](https://docs.pydantic.dev/latest/concepts/models/) model. You can generate it using [`datamodel-codegen`](https://docs.pydantic.dev/latest/integrations/datamodel_code_generator/) or edit manually like so:
Example:
```python
'''
file truncated for brevity
'''
class ConnectionSpecificationModel(ConnectionSpecification):
    client_id: str = Field(
        ..., description='client id for the project', title='Client ID'
    )
    client_secret: str = Field(
        ..., description='client secret for the project', title='Client Secret'
    )
    refresh_token: str = Field(
        ..., description='refresh token for the project', title='Refresh Token'
    )
'''
file truncated for brevity
'''
```

### `source.py`

There are two methods in the file that need to be implemented. Stub code has already been generated.

#### `check_connection(...):`
Implement your connection logic here.

Returns `(connections_status, message)`


#### `streams(...):`
Return a list of valid streams

### `catalog.yml`
Contains the structure of valid streams of this actor.
```yml
type: object
properties:
  document_streams:
    type: array
    items:
      - type: object
        allOf:
        - "$ref": "https://raw.githubusercontent.com/dat-labs/dat-core/main/dat_core/specs/DatDocumentStream.yml"
        required:
          - dir_uris
        properties:
          name:
            type: string
            const: pdf
          namespace:
            description: "namespace the data is associated with"
            type: string
            title: "Namespace"
            order: 0
          dir_uris:
            type: array
            title: "Directory URIs"
            order: 1
            items:
              type: string
```

### `catalog.py`
This file contains the `specs.yml` file as a [Pydantic](https://docs.pydantic.dev/latest/concepts/models/) model. You can generate it using [`datamodel-codegen`](https://docs.pydantic.dev/latest/integrations/datamodel_code_generator/) or edit manually.

### `streams.py`
#### `__init__(...):`
Initializer


#### `read_records(...):`
Logic to read_records


## Running tests
```bash
pytest verified_sources/{your-source-connector}/tests/test_{your-source-connector}.py 
```