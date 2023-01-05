# Visual Analytics Final Project

Welcome to our project!

## About the data

To download the dataset from AWS, the AWS Command Line Interface (CLI) needs to be downloaded. If you want to recreate the data download process, you can download it [here](https://aws.amazon.com/cli/?nc1=h_ls). 

The dataset is provided in chunks, split by category (internet speed measurements from fixed or mobile devices), year and quarter. The files are provided by AWS in a file folder structure based on these splits and you can navigate through the file structure in AWS with the `ls` command.

With the cp command, you can download files to the current directory. We managed to download all dataset chunks at once with the following command:

```
aws s3 cp --recursive s3://ookla-open-data/parquet . --no-sign-request
```
