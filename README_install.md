# Install instructions

After cloning the repository.

## Install python virtual environment

- Open command/terminal window
- Change location to your local ```(path)/GitHub/neo4cdisc``` folder.
- Execute: ```python -m venv ./venv```
- Execute: ```.\venv\Scripts\activate```
- Execute: ```pip install -r requirements.txt```
- (You should be prompted to sign in to github via browser, if the repository is private)
- Note. Might need to upgrade pip and redo the install, use message from installer:
  (path_to_your_local_install)/neo4cdisc/venv/scripts/python.exe -m pip install --upgrade pip

&ensp;&thinsp;&ensp;&thinsp;&ensp;&thinsp;Note. Always make sure you are in the activated virtual environment when executing pip commands.



# Pycharm
Note. Keep an eye on the Pycharm progress bar so that Pycharm has completed updating indices etc. before trying to run any program.

### Configure python interpreter to the one you just created
- Configure python interpreter for running scripts
    - File->Settings : Project: neo4cdisc ->Project Interpreter
    - Click 'gear' to add a new interpreter
    - Add the one you've just created (/Github/tab2neo/venv/Scripts/python.exe)

- Configure python interpreter for tests
    - File->Settings : Tools->Python Integrated Tools
    - Testing->Default test runner: pytest

### Set working directory and add environment variables
If pytests are to be run, repeat the steps below but change **Templates->Python** to **Templates->Python Tests->Pytest**

- Click _Edit configurations_
- Click **Templates->Python**
- Click folder icon next to **Working directory**
- Select the root directory of your neo4cdisc repository (should come up automatically)
- Click folder icon next to **Environment variables**
- Copy the below parameters, adding your specific details (NEO4J parameters are the only required parameters for executing current examples)
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

