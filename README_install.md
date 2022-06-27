# Install instructions

After cloning the repository.

## Install python virtual environment

- Open command/terminal window
- Change location to your local (path)/GitHub/neo4cdisc folder.
- Execute: *python -m venv ./venv*
- Execute: .\venv\Scripts\activate

N.B! Always make sure you are in the activated virtual environment

1. pip install -r requirements.txt
2. (You should be prompted to sign in to github via browser, if the repository is private)
3. Note. Might need to upgrade pip and redo the install, use message from installer:
  (path_to_your_local_install)/neo4cdisc/venv/scripts/python.exe -m pip install --upgrade pip


N.B! Keep an eye on the progress bar for when Pycharm updates indices

# Pycharm

### Configure python interpreter to the one you just created
- Configure python interpreter for running scripts
    - File->Settings : Project: neo4cdisc ->Project Interpreter
    - Click 'gear' to add a new interpreter
    - Add the one you've just created (/Github/tab2neo/venv/Scripts/python.exe)

- Configure python interpreter for tests
    - File->Settings : Tools->Python Integrated Tools
    - Testing->Default test runner: pytest

### Add environment variables (to python and debug)
- Click "Add configuration"
- Copy the below adding your details
```
AZ_CONTAINER=us6fnddev001
AZ_CLIENT_SECRET=
AZ_FILESYSTEM=dev-clinicalspace
AZ_TENANT=
AZ_CLIENT=
NEO4J_HOST=neo4j://10.40.225.78:27687/
NEO4J_USER=neo4j
NEO4J_PASSWORD=
NEO4J_RDF_HOST=http://10.40.225.78:27474/rdf/
GIT_TOKEN=
GIT_TOKEN_RW=
CLD_API_HOST=http://10.40.225.78:8000/items
```

### Set working directory
(path)/Github/neo4cdisc
