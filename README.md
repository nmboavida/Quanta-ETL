## Deustche Boerse Data Pipeline
For this project I developed a data pipeline to ingest intra-day asset price and transaction data (minute-by-minute basis) from Eurex Exchange and XETRA German Electronic Exchange, both being under Deutsche Börse AG.

Eurex Exchange data consists of price and transaction data of:

- Common Stocks
- Exchange Traded Funds (ETFs)
- Exchange Traded Notes (ETNs)
- Exchange Traded Commodities (ETCs)

XETRA German Electronic Exchange data consists of:
- Option chains for:
    - Indices (Equity and Volatility)
    - Individual stocks
- Futures
    - Fixed Income Futures
    - Index Dividend Futures
    - Volatility Index Futures
    - Single Stock Dividend Futures
    - Single Stock Futures

The available data starts from the 17th of June 2017. Note that observations in the dataset are on the basis of transactions,
 therefore if there were no trades for a given asset at a given point in time (say at time 08:01) there will be no observation for that particular asset at and that time and 
  therefore the price needs to be inferred from the previous observation.

### Requirements
The following project requires:
 1. Having [Docker](https://www.docker.com/) installed.
 2. Having an account with Amazon Web Services

This project spawns an EMR Cluster and loads data to an S3 bucket, therefore there are costs associated to running this project.

### Methodology
The project sets up a docker container configured to run airflow locally. Airflow is then used to orchestrated the job.
The airflow dag loads two files to an s3 bucket specified by the user. The files are the following:

- `etl.py` is the spark script that will run on the EMR Cluster
- `eurex_product_specification.csv` is a table with additional information on the derivatives traded with EUREX and we will join some of this data to the EUREX transaction dataset. The dataset can be found publicly [here](https://www.eurexchange.com/resource/blob/297610/a6b69425928ae76c10adde4cae8bd5db/data/product_specification.xlsx).

After the files being loaded to the s3 bucket an airflow operator is tasked to launch an EMR Cluster on AWS with the following m5.xlarge nodes:

- 1 Master Node
- 4 Core/Slave Nodes

Once the cluster is launched another operator is tasked to add the steps to the EMR Cluster. Steps are essentially used to submit work to the Spark Framework installed in the EMR Cluster.
You can find documentation [here](https://docs.amazonaws.cn/en_us/emr/latest/ReleaseGuide/emr-spark-submit-step.html) to learn more about steps in EMR Clusters.
In this project we add one step to submit our spark job to the cluster.

The spark job will then run once the cluster is ready and all the steps were added. Note that it may take around 5-10 minutes for the cluster to start and to be ready to perform our spark job.

Once the cluster is ready and the spark job is running you can monitor the job in Amazon's EMR Console. 
Once the job is finished the output can be found in the s3 bucket, consisting of two main partitioned parquet files with the trading data from XETRA and EUREX,
 as well as two parquet files with the result of two data quality checks executed on the EUREX dataset. These checks record which derivatives are missing ISINs and which are missing an Underlying Asset. This information can be subsequently acquired from the `eurex_product_specification.csv`

Note: To run this project you must specify an s3 bucket to load the files mentioned above as as well as the output of the spark job.


### Raw Data

The raw data consists of two partitioned datasets in csv format.
The datasets can be found in the following public s3 buckets:

- [Xetra](https://s3.console.aws.amazon.com/s3/buckets/deutsche-boerse-xetra-pds?region=eu-central-1&tab=objects): `s3a://deutsche-boerse-xetra-pds/`
- [Eurex](https://s3.console.aws.amazon.com/s3/buckets/deutsche-boerse-eurex-pds?region=eu-central-1&tab=objects): `s3a://deutsche-boerse-eurex-pds/`

One can find sample data in the folder `/sample-data` in this repository.
Below is an example of the data represented in a dictionary.

XETRA:
```json
{
    "ISIN": "AT0000A0E9W5",
    "Mnemonic": "SANT",
    "SecurityDesc": "S+T AG (Z.REG.MK.Z.)O.N.",
    "SecurityType":"Common stock",
    "Currency": "EUR",
    "SecurityID": 2504159,
    "Date": "2020-11-24",
    "Time": "08:00",
    "StartPrice": 18.94,
    "MaxPrice": 18.94,
    "MinPrice": 18.87,
    "EndPrice": 18.87,
    "TradedVolume": 1183,
    "NumberOfTrades": 3,
}
```

EUREX:

```json
{
    "isin":  "DE000P0YX7V7",
    "market_segment": "OESX",
    "underlying_symbol":  "SX5E",
    "underlying_isin": "EU0009658145",
    "currency":  "EUR",
    "security_type": "OPT",
    "maturity_string":  20201218,
    "strike_price": 2700,
    "put_or_call":  "Put",
    "mleg": "OESX SI 20201218 CS EU P 2700 0",
    "contract_generation_number":  1,
    "security_id": 27235,
    "trading_date": "2020-11-24",
    "trading_time": "08:00",
    "start_price":  1.6,
    "max_price": 1.6,
    "min_price":  1.6,
    "end_price": 1.6,
    "number_of_contracts": 15,
    "number_of_trades": 1,
}
```

### Data Model

The spark job will output two parquet files partitioned by the column `trading_date`.

Schema for  XETRA asset price and transaction data:

| Column               | Type                        | Nullable |
| -------------------- | --------------------------- | -------- |
| isin                 | StringType                  | True     |
| mnemonic             | StringType                  | True     |
| security_description | StringType                  | True     |
| security_type        | StringType                  | True     |
| currency             | StringType                  | True     |
| security_id          | StringType                  | True     |
| trading_date         | DateType                    | True     |
| trading_time         | StringType                  | True     |
| start_price          | DoubleType                  | True     |
| max_price            | DoubleType                  | True     |
| min_price            | DoubleType                  | True     |
| end_price            | DoubleType                  | True     |
| traded_volume        | LongType                    | True     |
| number_of_trades     | LongType                    | True     |
| trading_ts           | TimestampType               | True     |

Schema for  EUREX derivative price and transaction data:

| Column                     | Type                        | Nullable |
| -------------------------- | --------------------------- | -------- |
| isin                       | StringType                  | True     |
| market_segment             | StringType                  | True     |
| underlying_symbol          | StringType                  | True     |
| underlying_isin            | StringType                  | True     |
| currency                   | StringType                  | True     |
| security_type              | StringType                  | True     |
| maturity_string            | StringType                  | True     |
| strike_price               | DoubleType                  | True     |
| put_or_call                | StringType                  | True     |
| mleg                       | StringType                  | True     |
| contract_generation_number | IntegerType                 | True     |
| security_id                | StringType                  | True     |
| trading_date               | DateType                    | True     |
| trading_time               | StringType                  | True     |
| start_price                | DoubleType                  | True     |
| max_price                  | DoubleType                  | True     |
| min_price                  | DoubleType                  | True     |
| end_price                  | DoubleType                  | True     |
| number_of_contracts        | LongType                    | True     |
| number_of_trades           | LongType                    | True     |
| trading_ts                 | TimestampType               | True     |
| maturity_date              | DateType                    | True     |
| maturity_days              | LongType                    | True     |
| maturity_months            | DoubleType                  | True     |
| product_name               | StringType                  | True     |
| product_type               | StringType                  | True     |
| underlying_name            | StringType                  | True     |
| underlying_category        | StringType                  | True     |

Considerations on the data model chosen:

- Despite Xetra and Eurex table recording different asset types both datasets are related to each other as the underyling assets of the instruments traded with Eurex can be found in the Xetra table (provided the underlying asset is traded within that exchange).
- Therefore to analyse relationships between transactions and price changes of derivatives and it's underlying assets (provided they are traded in Xetra) we can join both tables using `eurex.underlying_isin == xetra.isin` and `eurex.trading_ts == xetra.trading_ts`. One can also generate a hash using these two columns for each dataset to facilitate join.
- This data model makes it easy to calculate minute-by-minute asset price returns. To calculate the returns one can shift `df.end_price` by one observation, grouped by isin. Then we calculate returns as `returns = (df.end_price / df.end_price_prev) - 1`. One can also calculate the volatility of returns, however this operation can be more computationally intensive as the volatitlity for each observation natually relies on a rolling window of previous observations.
- This data model can be aggregated to view transactions on a less granular timeframe. The column `trading_ts` will facilitate the aggregation but further columns can be added to the dataset to increase the swiftness of the process.
- One can also decide to use `trading_ts` to aggregate transactions and prices of various assets to create indices
- As a final remark, the decision to make the data model more or less parsimonious will also depend on the frequency of the queries we want to run on the data. If these queries run on a scheduled basis, we can create intermediate columns downstream to facilitate the querying. Conversely, if the queries are running throughout the day multiple times it can be an wise to add these intermediate columns in the ETL process here described.


### Run

1. Run docker-compose:
    ```
    docker-compose up -d
    ```
   <sub><sup>
   Note: Dockerfile and docker-compose have been pulled from puckel's [docker-airflow](https://github.com/puckel/docker-airflow)
   , a repository containing a Dockerfile of apache-airflow for Docker's automated build published to the public Docker Hub Registry.
   </sup></sub>
2. Open Airflow on port [8080](http://localhost:8080).

    <sub><sup> 
    Note: You will initially receive the following message: "Broken DAG: [/usr/local/airflow/dags/dag.py] 'Variable s3_bucket does not exist'".
    This is because we have not yet set up the Airflow variable `s3_bucket` which is a variable used by the dag.
   </sup></sub>
3. Create variable s3_bucket on Airflow in Admin > Variables:
    ```
    {'s3_bucket': 'your-bucket-name'}
    ```
    <sub><sup> 
    Note: Variable key must be `s3_bucket` and the variable value should correspond to the name of the s3 bucket used to store the output of the project
    (e.g. if s3 bucket uri is 's3a://db-trading-data/' then the key value should be 'db-trading-data' without the quotation marks).
    </sup></sub>
4. Configure aws_default and emr_default Airflow configurations:
    
    <sub><sup> 
    Complete the fields according to the following (this project is set to run in eu-central-1 so please do no neglect the extra parameter):
    </sup></sub>
    
    For aws_default:
    
        Conn Id: aws_default
        Conn Type: Amazon Web Services
        Login: (your IAM user Access Key ID)
        Password: (your IAM user Secret Access Key)
        Extra: {"region_name": "eu-central-1"}
    
    For emr_default:
    
        Conn Id: emr_default
        Conn Type: Elastic MapReduce
        Login: (your IAM user Access Key ID)
        Password: (your IAM user Secret Access Key)
       
5. Run the Dag
6. Stop docker container once the the job is terminated:
    ```
    docker-compose down
    ```

### Final Considerations

The decision to use spark in cluster mode helped cooping with the amount of data handled in this project and the decision to choose 4 core nodes came from the inital estimation that both datasets would take up to 20 GB of space, thus making it impractical to wrangle this data locally. Additionally, Spark supports the data science workflow end-to-end, from data ingestion to integration to machine learning and business inteligence applications. By using spark we can take advantage out of it's growing machine-learning algorithms library MLl ib. Spark has a large online community which subsequently improves the coding experience.

Airflow makes it easy to monitor the state of a pipeline in their UI. Using Airflow enhanced the project experience not just for providing orchestration but also for specifically provide operators that create, manage and terminate the EMR cluster. Using Airflow also facilitates the handling of user-specific variables, since variables such as s3_bucket uri and aws credentials can me given directly to airflow.

### Addressing Other Scenarios

What if:

1. The data was increased by 100x.
    
    If the data increased by 100x we would consider increasing the amount EMR Cluster nodes in order to avoid bottlenecks. We would also consider to choose nodes that would be optimized for the job instead of the multi-purpose nodes chosen.

2. The pipelines would be run on a daily basis by 7 am every day.

    This is in fact a very real scenario considering that the the source dataset is updated on a daily basis with new trading data. To accommodate this one can modify the DAG to run a daily job scheduled for 7 am, and add  a function on the spark script that primarily analyses which data is already present in the output s3 bucket so that we only ingest new data. An alternative is to empty the output s3 bucket in the beginning of the DAG and ingest the whole dataset every day, however this option is far from being cost efficient.

3. The database needed to be accessed by 100+ people.
    
    The application as it is designed can easily achieve thousands of transactions per second when uploading and retrieving storage from Amazon S3 as S3 automatically scales to high request rates rates. The application can achieve at least 3,500 PUT/COPY/POST/DELET or 5,000 GET/HEAD requests per secod per prefix in a bucket. Therefore, if the number of users accessing and reading at the same time increases we can also increase the prefixes to parallelize reads.
