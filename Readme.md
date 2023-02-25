# WTX Challenge
Resolution proposal for Ports challenge

This spark pipeline was built in a simple way given the simplicity of the transformations. 

Production wise it would be recommendable to split the steps into actual different jobs, or modules. 
This could be easily achievable by, for example, using tools like Airflow/Dagster, Glue Workflows or Databricks to orchestrate these steps.
By doing so we would be able to implement monitoring and even data quality over the different phases of the pipeline, development would be easier and debugging faster.


# 1. Setup

  ### 1.1 Dependencies
   - DOCKER (run & build)
   https://www.docker.com/

  ### 1.2 Build
  Once the repository is downloaded browse to the directory and build the docker image (this can take up to 3 minutes, depending on internet connection and machine):

  - ```sudo docker build -t wtx-challenge .```

 # 2. Run

  After the build, run the docker image:

  - ```docker run -v $PWD/csv:/csv wtx-challenge```

# 3. Output
  The answers will be written to the project folder CSV.
  There you will find 2 CSV files, one for each step: port_info.csv, trades_qa

# 4. STEPS
  ### 4.1 Port Data Collection & Research
  - Developed a simple web crawler that will go to https://www.cogoport.com/en/knowledge-center/resources/port-info/ and collect most of the requested port information namely:
    - Get the UN/LOCODE for each port;
    - Name of the port;
    - Address of the port;
    - A List of most relevant Shipping Lines on this port;
    - I didn't include the whole import/export requisites tables but the effort to add them is minimal since the relevant shipping Lines was a field that was extracted from one of the tables;
  - For further collecting in the future the following website (https://www.searates.com/maritime/) may be worth it to further enrich the port data.
  - Collected detailed information dataset (UpdatedPub150.csv) that includes Latitude and Longitude information about each port from the maritime safety information website (https://msi.nga.mil/Publications/WPI).
  - Collected detailed information dataset (wld_trs_ports_wfp.csv) that could be used as yet another information source for each of the ports.
  - Collected isoAlpha2Code.csv dataset (https://datahub.io/core/country-list)

  ### 4.2 Port Data Enrichment
  - Enforced the correct schema and enriched the main port_info.csv dataset with the Latitude and Longitude information 
  - The main goal of this step is to geocode for the Latitude and Longitude fields and get a list of the closest cities of each port in order to optimize shipment costs.
  - The List of cities was achieved by taking full leverage of the GeoPandas library. This was achieved by crossing the latitude and longitude points for each seaport and see which cities they intersect in a given radius.
  - The full Country intersection can also be achieved by using the same method used for the cities.
  - The following Table shows an excerpt of the port_info_final.csv.
    ```
    +---------+-------+--------------------+--------------------+--------------------+
    |Port Name|Port ID|             Address|      Shipping Lines|       Nearby Cities|
    +---------+-------+--------------------+--------------------+--------------------+
    |   Durres|  ALDRZ|Kapitenerija e Po...|['maersk', 'cosco...|['Valletta', 'Ath...|
    |   Skikda|  DZSKI|BP 65, Avenue Rez...|    ['cosco', 'CMA']|['Tripoli', 'Vall...|
    |     Oran|  DZORN|1 Rue du 20 Aout,...|    ['cosco', 'CMA']|['Casablanca', 'R...|
    |   Bejaia|  DZBJA|13, avenue des fr...|    ['cosco', 'CMA']|['Tripoli', 'Vall...|
    |  Algiers|  DZALG|02 Rue d'Angkor B...|    ['cosco', 'CMA']|['Algiers', 'Tuni...|
    |   LUANDA|  AOLAD|Largo 4 de Fevere...|['cosco', 'CMA', ...|['Libreville', 'L...|
    |   Buenos|  ARBUE|Buenos Aires, Arg...|['cosco', 'CMA', ...|['Montevideo', 'B...|
    |   Sydney|  AUSYD|PO Box 25 Millers...|['cosco', 'CMA', ...|['Melbourne', 'Ca...|
    |Melbourne|  AUMEL|GPO Box 261, Melb...|['cosco', 'CMA', ...|['Melbourne', 'Ca...|
    |Fremantle|  AUFRE|PO Box 95, Freman...|    ['CMA', 'Hapag']|               [nan]|
    +---------+-------+--------------------+--------------------+--------------------+
    ```
  ### 4.3 Quality Assurance, Schema enforcing
  - Enforce of a simple set of rules on the trades.csv file:
    - isoAlpha2Code Compliance Check using retrieved Database.(https://datahub.io/core/country-list);
    - 8 bit hs_code trade code that starts with the '870423' digits;
    - Enforce that in case quantity is not specified then, if the trade is of value lesser than 80,000$ then the quantity is set to '1';
    - At this stage, Schema enforcing should also be implemented in the future. 
  ### 4.4 Extract Transform Load
  - Quick Example of enrichment and 
  ### 4.5 Scalability
  - Ideally what should be done at this point is to make sure that the trade data is being handled on a hdfs. For that one must be careful on importing the files straight into the data lake and make sure that the scripts are reading from there.
  - this trades information could also be stored in parquet for ease of querying using a partitioning by date and country perhaps 
  - The port info static table should never be big enough for us to worry about scalability since it is highly unlikely that it will be anything more than a one-off run.
  - 
  ### 4.6 Azure Cloud Implementation


# Tech challenge for data engineers

At WTX we want to enable all teams to have data literacy and be able to find insights that are cross-functional and could only be gathered if they had access to data coming from different workstreams.

Our goal as data engineers is to ensure that we are able to gather the following:

* Collect, Store and make it available from different data sources (both internal and external)
* Provide the appropriate infrastructure to enable anyone within WTX to gather data insights

### Background

#### Step 1 - Import / Export reports

Our business development team was able to access an international trade database that provides us with some information regarding import and export of "Motor vehicles for the transport of goods" which has the HS Code `8704`.

We will be able to start receiving monthly reports of these trades from their team, to which could help our sales team better understand the demand in certain countries as well as the operations team better understand the most used routes.

As we know that these data are collected partially manually and we need to validate the data quality and ensure it follows some rules:

* HS Code needs to start with `870423` (Trucks & Lorries) and have 8 characters
* Port code starts with the country's ISO Alpha-2 code
* Quantity is a numeric, where we can assume the number is 1 if the value is less than 80,000 USD when there's no information

The goal is to have a structured way of keeping track of the numbers such as:

* Most popular shipping routes (source port and destination port)
* Average import value (in USD) per country

The file with the collected information is attached as a delimited text file [trades.csv](resources/trades.csv).

#### Step 2 - Port information

As we expand, we want to be able to struck new deals with shipping lines to decrease our logistics costs. 

In order to do so, we found a website that provides additional information on ports:

* https://www.cogoport.com/ports

We want to understand the main shipping lines that operate on each of the source and destination ports from the import/export dataset (that we have information on) so that we can find if we can cover multiple ports with a single shipping line. This will also help us as we expand to new countries and regions.

The main information we are looking for (if present) is:
* Major towns near seaport
* List of main shipping lines serving the port
* Country Requirements & Restrictions (Import & Export)

### Goals

As one of the first data engineers at WTX we expect you to be able to take charge in defining the entire data pipeline on how to collect, transform and store this information to make it available for all teams.

 1. Write a `Python, JavaScript or Go` script to analyse the data based on the requirements stated above to determine the data's quality and sanitize the collected data.
- Make sure that your solution is able to scale and handle large volumes of data (millions of trades), feel free to show proof.

2. Collect data from the ports in our dataset (source and destination) from the [website mentioned above](https://www.cogoport.com/ports) - through automation, and store it a way that it can be used for the Step 3.
- If the port in our dataset does not have information on the website, then we can skip it but keep track that we have no information available.

3. Design the Data Pipeline from ingest until the data would be available for querying from other teams.
- Make sure to describe all components being used as well an overall cost estimation and how that would scale
- Bonus points if you draft how you would implement this data pipeline in Azure


*Note: As part of this challenge feel free to take some assumptions (when not clearly stated) but make sure to explain them.*
