import os
import subprocess
import logging
import argparse
import shutil
from pyspark.sql import SparkSession
import w3storage
from w3storage import API
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration --- #
# For production environments, consider using a more secure secret management tool.
STORACHA_API_TOKEN: Optional[str] = os.getenv("STORACHA_API_TOKEN")
STORACHA_API_URL: Optional[str] = os.getenv("STORACHA_API_URL")
SPARK_APP_NAME: Optional[str] = os.getenv("SPARK_APP_NAME")
OUTPUT_DIR: str = "spark_output"


def create_spark_session(app_name: str) -> SparkSession:
    """Creates and returns a SparkSession object.

    Args:
        app_name: The name of the Spark application.

    Returns:
        A SparkSession object.
    """
    try:
        # For production workloads, consider tuning the Spark configuration for better performance.
        # Example:
        # spark = SparkSession.builder.appName(app_name)
        #     .config("spark.executor.memory", "2g")
        #     .config("spark.executor.cores", "2")
        #     .config("spark.driver.memory", "1g")
        #     .getOrCreate()
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logging.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Failed to create SparkSession: {e}")
        raise

def validate_input_file(file_path: str) -> bool:
    """Validates the input file.

    Args:
        file_path: The path to the input file.

    Returns:
        True if the file is valid, False otherwise.
    """
    if not os.path.exists(file_path):
        logging.error(f"Input file not found: {file_path}")
        return False
    if os.path.getsize(file_path) == 0:
        logging.warning(f"Input file is empty: {file_path}")
    return True

def process_with_spark(spark: SparkSession, input_path: str, output_path: str) -> None:
    """Use Spark to process data (word count example).

    Args:
        spark: The SparkSession object.
        input_path: The path to the input data.
        output_path: The path to save the output.
    """
    try:
        logging.info(f"Starting Spark processing for input: {input_path}")
        text_rdd = spark.sparkContext.textFile(input_path)
        words = text_rdd.flatMap(lambda line: line.split(" "))
        word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
        word_counts.saveAsTextFile(output_path)
        logging.info(f"Spark processing complete. Output saved to {output_path}")
    except Exception as e:
        logging.error(f"Spark processing failed: {e}")
        raise

def upload_to_storacha(file_path: str, api_token: str, api_url: str) -> Optional[str]:
    """Upload file to Storacha using the Python client with a fallback to CLI.

    Args:
        file_path: The path to the file or directory to upload.
        api_token: The Storacha API token.
        api_url: The Storacha API URL.

    Returns:
        The CID of the uploaded file, or None if the upload failed.
    """
    try:
        logging.info(f"Attempting to upload {file_path} to Storacha via Python client.")
        custom_api = API(api_url)
        client = w3storage.make_w3(custom_api, api_token)

        if os.path.isdir(file_path):
            cid = client.post_upload(os.listdir(file_path))
        else:
            with open(file_path, "rb") as f:
                cid = client.post_upload((os.path.basename(file_path), f))

        logging.info(f"Successfully uploaded to Storacha with CID: {cid}")
        logging.info(f"Access via IPFS gateway: https://ipfs.io/ipfs/{cid}")
        return cid
    except Exception as e:
        logging.warning(f"Python client upload failed: {e}. Falling back to CLI.")
        return upload_with_cli(file_path)

def upload_with_cli(file_path: str) -> Optional[str]:
    """Fallback upload using Storacha CLI.

    Args:
        file_path: The path to the file or directory to upload.

    Returns:
        The CID of the uploaded file, or None if the upload failed.
    """
    try:
        logging.info(f"Attempting to upload {file_path} via Storacha CLI.")
        result = subprocess.run(
            ["storacha", "up", file_path],
            capture_output=True,
            text=True,
            check=True
        )
        output = result.stdout
        cid_line = next((line for line in output.splitlines() if "CID" in line), None)

        if cid_line:
            cid = cid_line.split(":")[-1].strip()
            logging.info(f"CLI upload successful. CID: {cid}")
            return cid
        else:
            raise ValueError("CID not found in CLI output.")
    except FileNotFoundError:
        logging.error("The 'storacha' command was not found. Is the CLI installed and in your PATH?")
    except subprocess.CalledProcessError as e:
        logging.error(f"CLI upload failed with exit code {e.returncode}.")
        logging.error(f"Stderr: {e.stderr}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during CLI upload: {e}")
    return None

def cleanup(paths: list[str]) -> None:
    """Removes files and directories.

    Args:
        paths: A list of paths to remove.
    """
    for path in paths:
        try:
            if os.path.isfile(path):
                os.remove(path)
                logging.info(f"Removed file: {path}")
            elif os.path.isdir(path):
                shutil.rmtree(path)
                logging.info(f"Removed directory: {path}")
        except OSError as e:
            logging.error(f"Error removing {path}: {e}")

def main(input_file: str, keep_files: bool) -> None:
    """Main pipeline execution.

    Args:
        input_file: The path to the input file.
        keep_files: Whether to keep the generated files.
    """
    if not all([STORACHA_API_TOKEN, STORACHA_API_URL, SPARK_APP_NAME]):
        logging.error("Missing one or more required environment variables.")
        return

    if not validate_input_file(input_file):
        return

    spark = None
    try:
        spark = create_spark_session(SPARK_APP_NAME)
        process_with_spark(spark, input_file, OUTPUT_DIR)
        upload_to_storacha(OUTPUT_DIR, STORACHA_API_TOKEN, STORACHA_API_URL)
    except Exception as e:
        logging.critical(f"Pipeline execution failed: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("SparkSession stopped.")
        if not keep_files:
            cleanup([input_file, OUTPUT_DIR])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Storacha Spark Pipeline")
    parser.add_argument("input_file", help="The path to the input file.")
    parser.add_argument("--keep-files", action="store_true", help="Keep generated files after execution.")
    args = parser.parse_args()

    main(args.input_file, args.keep_files)