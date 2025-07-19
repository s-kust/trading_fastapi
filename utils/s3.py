import io
from typing import List, Optional, Set

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.logging import execute_and_log, get_app_logger

load_dotenv()
s3_client = boto3.client(service_name="s3")


def _check_s3_call_inputs(
    caller_func: str,
    folder: str,
    filename: Optional[str],
    df: Optional[pd.DataFrame] = None,
) -> None:
    if df is not None and df.empty:
        raise ValueError(f"{caller_func}: input DataFrame is empty")
    if folder[-1] != "/":
        raise ValueError(f"{caller_func}: {folder=}, last symbol must be /")
    if folder[0] == "/":
        raise ValueError(f"{caller_func}: {folder=}, first symbol can't be /")
    if filename is not None:
        if caller_func in ["write_df_to_s3_csv", "read_df_from_s3_csv"]:
            if not filename.endswith(".csv"):
                raise ValueError(f"{caller_func}: {filename=} - must end with .csv")


def remove_csv_from_s3(
    filename: str,
    bucket: str = S3_BUCKET,
    folder: str = S3_FOLDER_DAILY_DATA,
) -> str:
    """
    Remove file from S3 bucket.
    If removal fails because file DOES NOT EXIST, it's ok.
    """
    _check_s3_call_inputs(
        caller_func="remove_csv_from_s3",
        folder=folder,
        filename=filename,
    )
    folder_filename = folder + filename
    try:
        s3_client.head_object(Bucket=bucket, Key=folder_filename)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return f"File {folder_filename} DOES NOT EXIST in the S3 bucket {bucket}"
        elif e.response["Error"]["Code"] == "403":
            raise RuntimeError(
                f"Unauthorized access to S3, maybe invalid {bucket=}"
            ) from e
        else:
            # Something else has gone wrong.
            raise
    else:
        response = s3_client.delete_object(Bucket=bucket, Key=folder_filename)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 204:
            raise RuntimeError(
                f"remove_csv_from_s3 {folder_filename=}, {status=}, should be 204, full {response=}"
            )
        else:
            return f"{folder_filename} removed from S3 bucket {bucket} - OK"


def remove_csv_for_tickers(tickers: Set[str]) -> None:
    """
    For every input ticker, remove CSV from S3 bucket
    and print message
    """
    total_count = len(tickers)
    counter = 0
    for ticker in tickers:
        counter = counter + 1
        msg = remove_csv_from_s3(filename=f"{ticker}.csv")
        print(f"Removing CSV for {ticker=} - {msg} - {counter} of {total_count}")


def write_df_to_s3_csv(
    df: pd.DataFrame,
    filename: str,
    bucket: str = S3_BUCKET,
    folder: str = S3_FOLDER_DAILY_DATA,
) -> str:

    if not "/" in filename:
        _check_s3_call_inputs(
            caller_func="write_df_to_s3_csv",
            df=df,
            folder=folder,
            filename=filename,
        )
        folder_filename = folder + filename
    else:
        if not filename.endswith(".csv"):
            raise ValueError(f"write_df_to_s3_csv: {filename=} - must end with .csv")
        folder_filename = filename

    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=True)
        response = s3_client.put_object(
            Bucket=bucket, Key=folder_filename, Body=csv_buffer.getvalue()
        )
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            return f"Writing to S3 {S3_BUCKET}/{folder_filename} - OK"
        else:
            return (
                f"Writing to S3 {S3_BUCKET}/{folder_filename} FAILED, status - {status}"
            )


def read_df_from_s3_csv(
    filename: str,
    bucket: str = S3_BUCKET,
    folder: str = S3_FOLDER_DAILY_DATA,
) -> Optional[pd.DataFrame]:

    if not "/" in filename:
        _check_s3_call_inputs(
            caller_func="read_df_from_s3_csv",
            folder=folder,
            filename=filename,
        )
        folder_filename = folder + filename
    else:
        if not filename.endswith(".csv"):
            raise ValueError(f"read_df_from_s3_csv: {filename=} - must end with .csv")
        folder_filename = filename

    try:
        response = s3_client.get_object(Bucket=bucket, Key=folder_filename)
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchKey":
            return None
        else:
            raise

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        logger = get_app_logger()
        res = pd.read_csv(response.get("Body"), index_col=0)
        logger.info("Before pd.to_datetime")
        logger.info(f"{type(res.index)=}")
        res.index = pd.to_datetime(res.index, utc=True)
        logger.info("After pd.to_datetime before normalize")
        logger.info(f"{type(res.index)=}")
        res.index = res.index.normalize()
        logger.info("After normalize before index.date")
        logger.info(f"{type(res.index)=}")
        # res.index = res.index.date  # type: ignore
        # logger.info("After index.date before res.sort_index()")
        # logger.info(f"{type(res.index)=}")
        res = res.sort_index()
        logger.info("After res.sort_index()")
        logger.info(f"{type(res.index)=}")
        return res
    else:
        raise RuntimeError(
            f"read_df_from_s3_csv: S3 response {status=} != 200, full {response=}"
        )


def read_daily_ohlc_from_s3(ticker: str) -> Optional[pd.DataFrame]:
    filename = f"{ticker.upper()}.csv"
    return read_df_from_s3_csv(
        filename=filename,
        bucket=S3_BUCKET,
        folder=S3_FOLDER_DAILY_DATA,
    )


def get_list_of_files_in_s3_folder(
    s3_bucker: str = S3_BUCKET, s3_folder: str = S3_FOLDER_DAILY_DATA
) -> List[str]:
    _check_s3_call_inputs(
        caller_func="get_list_of_files_in_s3_folder", folder=s3_folder, filename=None
    )
    res = list()
    kwargs = {"Bucket": s3_bucker, "Prefix": s3_folder}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            res.append(obj["Key"])

        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return res
