## Testing dbt project: `Traffic_Weather_Events`

This dbt project transforms raw data from traffic and weather datasets into models ready for analytics.

### What is this repo?
What this repo _is_:
- A self-contained playground dbt project, useful for testing out scripts, and communicating some of the core dbt concepts.

What this repo _is not_:
- A tutorial — check out the [Getting Started Tutorial](https://docs.getdbt.com/tutorial/setting-up) for that. Notably, this repo contains some anti-patterns to make it self-contained, namely the use of seeds instead of sources.
- A demonstration of best practices — check out the [dbt Learn Demo](https://github.com/dbt-labs/dbt-learn-demo) repo instead. We want to keep this project as simple as possible. As such, we chose not to implement:
    - our standard file naming patterns (which make more sense on larger projects, rather than this five-model project)
    - a pull request flow
    - CI/CD integrations
- A demonstration of using dbt for a high-complex project, or a demo of advanced features (e.g. macros, packages, hooks, operations) — we're just trying to keep things simple here!

### What's in this repo?
This repo contains [seeds](https://docs.getdbt.com/docs/building-a-dbt-project/seeds) that includes some raw data from US holidays from 2014 to 2021.

The raw data used for the Weather and Traffic events can be found on the public AWS S3 bucket made available in this project.


### Running this project
To get up and running with this project:
1. Install dbt using [these instructions](https://docs.getdbt.com/docs/installation).

2. Clone this repository.

3. Change into the `TrafficWeather` directory from the command line:
```bash
$ cd TrafficWeather
```

4. Set up a profile called `Traffic_Weather_Events` to connect to a data warehouse by following [these instructions](https://docs.getdbt.com/docs/configure-your-profile). If you have access to a data warehouse, you can use those credentials – we recommend setting your [target schema](https://docs.getdbt.com/docs/configure-your-profile#section-populating-your-profile) to be a new schema (dbt will create the schema for you, as long as you have the right privileges). If you don't have access to an existing data warehouse, you can also setup a local postgres database and connect to it in your profile.

5. Ensure your profile is setup correctly from the command line:
```bash
$ dbt debug
```

6. Load the CSVs with the demo data set. This materializes the CSVs as tables in your target schema. Note that a typical dbt project **does not require this step** since dbt assumes your raw data is already in your warehouse.
```bash
$ dbt seed
```
7. Create a connection with a Snowflake account and set a database "TRIGGO_EXAMPLES_DATABASE" containing the a schema "TRAFFIC" and create a table with the definition:
<pre><code>create or replace US_WEATHER_AND_TRAFFIC_EVENTS.TRAFFIC.US_TRAFFIC_EVENTS_RAW (
	EVENTID VARCHAR(16777216) NOT NULL,
	TYPE VARCHAR(16777216) NOT NULL,
	SEVERITY NUMBER(38,0) NOT NULL,
	TMC NUMBER(38,0) NOT NULL,
	DESCRIPTION VARCHAR(16777216) NOT NULL,
	"StartTime(UTC)" TIMESTAMP_NTZ(9) NOT NULL,
	"EndTime(UTC)" TIMESTAMP_NTZ(9) NOT NULL,
	TIMEZONE VARCHAR(16777216) NOT NULL,
	LOCATIONLAT NUMBER(38,0),
	LOCATIONLNG NUMBER(38,0),
	"Distance(mi)" NUMBER(38,0),
	AIRPORTCODE VARCHAR(16777216),
	NUMBER NUMBER(38,0),
	STREET VARCHAR(16777216),
	SIDE VARCHAR(16777216),
	CITY VARCHAR(16777216),
	COUNTY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	ZIPCODE NUMBER(38,0)
)COMMENT='Raw data from us traffic events'
;</code></pre>
In the same database, create the schema "WEATHER" with the table definition:
<pre><code>create or replace US_WEATHER_AND_TRAFFIC_EVENTS.WEATHER.US_WEATHER_EVENTS_RAW (
	EVENTID VARCHAR(16777216) NOT NULL,
	TYPE VARCHAR(16777216) NOT NULL,
	SEVERITY VARCHAR(16777216),
	STARTTIME TIMESTAMP_NTZ(9) NOT NULL,
	ENDTIME TIMESTAMP_NTZ(9) NOT NULL,
	TIMEZONE VARCHAR(16777216) NOT NULL,
	LOCATIONLAT NUMBER(38,0),
	LOCATIONLNG NUMBER(38,0),
	AIRPORTCODE VARCHAR(16777216),
	CITY VARCHAR(16777216),
	COUNTY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	ZIPCODE NUMBER(38,0)
)COMMENT='Raw data from US Weather Events'
;</code></pre>
Then load the tables respectively with s3://weathertrafficevents/TrafficEvents_Aug16_Dec20_Publish.csv.gz and s3://weathertrafficevents/WeatherEvents_Aug16_Dec20_Publish.csv.gz using your preferable data warehouse and no credentials (Both username and password fields empty).

8. Run the models:
```bash
$ dbt run
```

> **NOTE:** If this steps fails, it might mean that you need to make small changes to the SQL in the models folder to adjust for the flavor of SQL of your target database.

9. Test the output of the models:
```bash
$ dbt test
```

9. Generate documentation for the project:
```bash
$ dbt docs generate
```

10. View the documentation for the project:
```bash
$ dbt docs serve
```

### Acknowledgment
The datasets used for the Weather and Traffic Events is being distributed only for Research purposes, under Creative Commons Attribution-Noncommercial-ShareAlike license (CC BY-NC-SA 4.0), by Sobhan Moosavi.
The datasets were created for his paper: "Moosavi, Sobhan, Mohammad Hossein Samavatian, Arnab Nandi, Srinivasan Parthasarathy, and Rajiv Ramnath. “Short and Long-term Pattern Discovery Over Large-Scale Geo-Spatiotemporal Data.” In proceedings of the 25th ACM SIGKDD International Conference on Knowledge Discovery & Data Mining, ACM, 2019."

---
For more information on dbt:
- Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction).
- Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
- Join the [dbt community](http://community.getdbt.com/).
---
