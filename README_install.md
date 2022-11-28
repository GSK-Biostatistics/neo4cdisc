# Installation Instructions

## Prerequisite
- Clone the repository.

## Setting up Python Virtual Environment

Python virtual environment can be setup by following any of the below paths.

##### Path 1
- Open command/terminal window
- Change location to your local ```<add path>/neo4cdisc``` folder.
- Execute: ```python -m venv ./venv```
- Execute: ```.\venv\Scripts\activate```
- Execute: ```pip install --upgrade pip```
- Execute: ```pip install -r requirements.txt```
- (You should be prompted to sign in to github via browser, if the repository is private)

Note: Virtual Environment should always be activated before executing any pip commands.

##### Path 2
Open installation.bat file if using Windows machine or installation.sh if using Unix/Linux machine in file explorer and run it by double clicking on it to execute the steps in Path 1.

## Setting up Pycharm (Only if using Pycharm IDE)
Note: Let the Pycharm complete updating the indices etc. before running the program. Progress can be seen at the bottom of the screen in Pycharm progress bar.

#### Configure python interpreter
- Configure python interpreter for running scripts
    - File->Settings : Project: neo4cdisc ->Project Interpreter
    - Click 'gear' icon on top right to add a new interpreter
    - Add the one you've just created (/neo4cdisc/venv/Scripts/python.exe)

- Configure python interpreter for tests
    - File->Settings : Tools->Python Integrated Tools
    - Testing->Default test runner: pytest

#### Set working directory and add environment variables

- Click **Run->Edit configuration** in the top menu bar
- Click **Templates->Python**
- Click folder icon next to **Working directory**
- Select the root directory of your neo4cdisc repository (should come up automatically)
- Click folder icon next to **Environment variables**
- Copy the below parameters, adding your specific details (NEO4J parameters are the only required parameters for executing current examples)
```
NEO4J_HOST=neo4j://10.40.225.78:27687/
NEO4J_USER=neo4j
NEO4J_PASSWORD=
NEO4J_RDF_HOST=http://10.40.225.78:27474/rdf/
```

To run pytests, repeat the above steps and replace **Templates->Python** by **Templates->Python Tests->Pytest** in step 2.

