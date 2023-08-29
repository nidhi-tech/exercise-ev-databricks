# Project structure
```
exercise-ev-databricks/
├─ .github/workflows/
│  ├─ databricks-ci.yaml
├─ data/
│  ├─ 1678731740.csv
│  ├─ 1679387766.csv
├─ notebooks/
│  ├─ Batch Processing - Bronze.py
├─ tests/
│  ├─ run_unit_tests.py
├─ .gitignore
├─ README.md
```  
## CI/CD
we add a GitHub Actions workflow that will run our integration test whenever a pull request is made on the repo.
