# Running

## Locally

```bash
conda create -yn etl-env-data-lake python=3.7 --file requirements/requirements.txt
conda activate etl-env-data-lake
python etl.py
conda deactivate
```

### Jupyter notebook

This will allow to run the functions interactively in the jupyter notebook.

```bash
conda install -yn base nb_conda_kernels
conda install -yn etl-env-data-lake --file requirements/requirements_dev.txt
conda activate base
jupyter notebook
```

In jupyter, choose the kernel `etl-env-data-lake`.
