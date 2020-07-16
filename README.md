# osf-pigeon

A utility for archiving OSF data to archive.org  


## Purpose

This utility takes a publicly available OSF registration guid as a parameter and using that info alone it will transfer
that registration, file data, metadata et all to Internet Archive.

# Install

Simply install the package using python's package manager pip with bash:
 
 `pip3 install osf_pigeon`. 
 
To use for local development just remember to install the developer requirements using 

`pip3 install -r dev.txt`

# Use

Simply import the module and enter a guid with credentials:

```python
from osf_pigeon import pigeon

pigeon(
    'guid0',
    datacite_username='test_datacite_username',
    datacite_password='test_datacite_password',
    datacite_prefix='test_datacite_prefix',
    ia_access_key='test_datacite_password',
    ia_secret_key='test_datacite_password',
)

```

That's it!

Assuming the registration is fully public and the DOI has been minted properly at datacite. 

## Running for development

#### Tests

Running tests are easy enough just:

```
pip3 install -r dev.txt
python3 -m pytest . 
```

#### Linting

Optional, but recommended: To set up pre-commit hooks (will run
formatters and linters on staged files):

```
pip install pre-commit

pre-commit install --allow-missing-config
```