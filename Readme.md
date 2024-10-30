# IP CIDR Pyspark Data Extractor

This project is designed to process IP CIDR data using PySpark, enabling efficient data extraction and manipulation.

## Environment Setup

To run this project, you need to set up a conda environment with Python 3.9.13 and the necessary packages. Follow the steps below to create the environment:

### Prerequisites

- Ensure you have [Anaconda](https://www.anaconda.com/products/distribution#download-section) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) installed on your system.

### Create the Conda Environment

1. Create a file named `environment.yml` with the following content:

    ```yaml
    name: ip_cidr_pyspark_env
    channels:
      - defaults
    dependencies:
      - python=3.9.13
      - pip
      - pip:
          - pyspark==3.3.1
          - pytest==8.3.3
    ```

2. Open your terminal or command prompt and navigate to the directory where the `environment.yml` file is located.

3. Run the following command to create the conda environment:

    ```bash
    conda env create -f environment.yml
    ```

### Activate the Environment

Once the environment is created, activate it using the following command:

```bash
conda activate ip_cidr_pyspark_env
