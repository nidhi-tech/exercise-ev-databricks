# Project structure
```
exercise-ev-databricks/
├─ .github/workflows/
│  ├─ databricks-ci.yaml
├─ data/
│  ├─ 1678731740.csv
│  ├─ 1679387766.csv
├─ src/
|  ├─ source
|  ├─ sink
|  ├─ transform
|  ├─ DatabricksHelper
├─ tests/
│  ├─ test_source
   ├─ test_transofrm
├─ .gitignore
├─ requirements.txt
├─ README.md
```
## Problem Statement 
Given data is from the software of Charge Point Simulator for Electric Vehicles (EVs). We use this data to process and calculate final charge time and final charge dispense for every completed transaction.

## databricks-ci.yaml(CI/CD)
This file starts the cluster and run our integration test whenever a pull request is made on the repo.
Databricks Host and Token variables in the databricks-ci.yaml file are configured in Github actions secrets. 

Configure Cluster Id in the yaml file(current cluster created in databricks environment) [Cluster URL AND ID](https://docs.databricks.com/en/workspace/workspace-details.html).

Refer [using secrets in github actions](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions)
## Useful links
[Personal Token access for Databricks REST API](https://docs.databricks.com/en/dev-tools/auth.html)

[Databricks CLI](https://docs.databricks.com/en/archive/dev-tools/cli/index.html)



