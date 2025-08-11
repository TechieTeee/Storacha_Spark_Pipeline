# Storacha Spark Pipeline

## The Case for Decentralized Data Pipelines

In a data-driven world, we often rely on centralized data pipelines. This approach creates single points of failure, increases security risks, and limits data sovereignty. Decentralized data pipelines, powered by technologies like IPFS and Storacha, offer a more resilient, secure, and censorship-resistant alternative.

This project demonstrates how to build a simple, yet powerful, big data pipeline. It uses Apache Spark to process data and Storacha to store it in a decentralized approach. With decentralized technologies, we can create data workflows that are more transparent, user-centric, and fault-tolerant.

## What This Is

This project provides a blueprint for a decentralized data pipeline. It connects three powerful tools to create a simple, yet effective, workflow:

1.  **Starts with a local data file.**
2.  **Processes the data with Apache Spark.**
3.  **Uploads the results to the decentralized Storacha network.**

Use this project as a starting point to build your own custom data pipelines.

## Key Features

*   **Store Data on a Decentralized Network**: This pipeline uses Storacha to store your data, which provides high availability and censorship resistance.
*   **Process Large Datasets**: Apache Spark gives you the power to process large amounts of data in a distributed environment.
*   **Secure Your Secrets**: The `.env` file keeps your API tokens and other sensitive information out of the main codebase.
*   **Extend the Code**: The modular design of the code makes it easy to add new features or adapt it to your specific needs.
*   **Handle Errors Gracefully**: The pipeline includes error handling and a fallback mechanism to ensure your data gets uploaded, even if there are hiccups.

## Prerequisites

Before you begin, ensure you have the following installed:

*   Python 3.6+
*   Apache Spark
*   Java 8+
*   The Storacha CLI

## Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/username/repository.git
    cd Storacha_Spark_Pipeline
    ```

2.  **Install the Python dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

3.  **Set up your environment variables:**

    Create a `.env` file in the root of the project and add the following:

    ```
    STORACHA_API_TOKEN="your-storacha-api-token"
    STORACHA_API_URL="https://api.storacha.network"
    SPARK_APP_NAME="StorachaBigDataPipeline"
    ```

    Replace `"your-storacha-api-token"` with your actual Storacha API token.

## Usage

To run the pipeline, use the following command:

```bash
python spark_storacha_pipeline.py /path/to/your/input_file.txt
```

Replace `/path/to/your/input_file.txt` with the actual path to your input file.

### Command-line Arguments

*   `input_file`: (Required) The path to the input file.
*   `--keep-files`: (Optional) If specified, the script will not delete the generated files (the input file and the `spark_output` directory) after the pipeline completes.

## Contributing

Contributions are welcome! If you have any ideas for improvements or new features, please open an issue or submit a pull request.

### Code Style

This project uses [Black](https://github.com/psf/black) for code formatting and [Flake8](https://flake8.pycqa.org/en/latest/) for linting. Before submitting a pull request, please ensure that your code is formatted and linted:

```bash
black .
flake8 .
```

### Docker

To build the Docker image:

```bash
make docker-build
```

To run the pipeline using Docker:

```bash
make docker-run
```

**Note on Secrets:** When running with Docker, it's best to pass your environment variables using the `--env-file` flag or individual `-e` flags rather than copying the `.env` file into the container.

### Security

To make your pipeline more secure, it's good practice to scan the code for potential security vulnerabilities with a tool like [Bandit](https://bandit.readthedocs.io/en/latest/):

```bash
bandit -r .
```
