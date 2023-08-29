# Project structure
├──.github/workflows
│   └── databricks-ci.yaml
├── README.md
├── data
│   └── 1678731740.csv
│   └── 1679387766.csv
├── notebooks
│   └── Batch Processing - Bronze.py
└── tests
    |__ run_unit_tests.py
    
## CI/CD
we add a GitHub Actions workflow that will run our integration test whenever a pull request is made on the repo.
