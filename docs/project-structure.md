# Project structure

```text
├── README.md: main project documentation file
├── dl.cfg: AWS credentials, and input and output paths
├── docs: contains files related to the project documentation
├── etl.py: reads data from S3, processes that data using Spark, and writes them back to S3
├── extract.py: functions to read data from S3
├── load.py: functions to write data into S3
├── requirements
│   ├── requirements.txt: project requirements (Python libraries)
│   └── requirements_dev.txt: additional requirements used for development
├── test_data_sanity_checks.ipynb: checks for the data extraction and transformation
├── test_final_parquet.ipynb: checks for the final parquet files
├── transform.py: functions to process data using Spark
└── utils.py: auxiliary functions
```
