# DMI dbt project
Contains dbt models for transformations for the SHIELD data warehouse.

## Setting up locally
### Requirements 
1. Make sure you have python installed: Python 3.8 and above
2. For Windows preferably use Git Bash as your terminal. Download the git package here https://gitforwindows.org/ (It will include git bash)
3. Make sure you have a PostgreSQL client to interact with your PostgreSQL database e.g PgAdmin, DBeaver, Azure Data Studio etc

### Steps
- Clone the repo from GitHub and cd to the root folder.
- Create a python virtual environment by running: `python3.8 -m venv <name_of_environemt>` (e.g. `python3.8 -m venv venv`)
- Activate virtual environment by running: `source venv/Scripts/activate`
- Once virtual environment is activated install dbt adapter for PostgreSQL by running:
     `pip install dbt-postgres`
- After installing run version check to confirm dbt is installed in your virtual environment
    `dbt --version`
- Create a `.env` file on the root folder and paste the following environment variables (make sure there is no space between):

    ```
        export DBT_USER_DEV=<sql server user>
        export DBT_PASSWORD_DEV=<sql server password>
        export DBT_DATABASE_DEV=<database to build models on>
        export DBT_SERVER_DEV=<server ip address>
        export DBT_SCHEMA=< default schema to build models on>
        export DBT_PROFILES_DIR=./profiles/
    ```

For `DBT_SCHEMA` make sure you have a schema in the development Postgres instance that you will use to build your models & datasets. Ideally call it *dbt_<name_of_dev>*
- Run `source .env` to load your environment variables.
- Make sure you have the config file `profiles.yml` inside the profiles folder with the following configarations for dev:
    
```
dmi_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DBT_SERVER_DEV') }}"
      database: "{{ env_var('DBT_DATABASE_DEV') }}"
      schema: "{{ env_var('DBT_SCHEMA') }}"
      port: "{{ env_var('DBT_PORT_DEV') | int }}"
      user:  "{{ env_var('DBT_USER_DEV') }}"
      password: "{{ env_var('DBT_PASSWORD_DEV') }}"
      threads: 4
      
 ```

## Common commands to interact with dbt
    
- `dbt compile` - generates executable SQL from source
- `dbt run` - runs all models in the models folder
- `dbt run --select <model_name>` - runs a specified single model e.g `dbt run --select stg_sari_ili`
- `dbt run --models <path/to/my/models>` - runs all models in a specified directory e.g `dbt run --select dmi_data_mart`
- `dbt seed` - loads csv files (typically not for large files)
- `dbt test` - runs tests against your models and seeds
- `dbt docs generate` - generates your project's documentation
- `dbt docs serve` - starts a webserver on port 8000 to serve your documentation locally
- `dbt deps` -  pulls the most recent version of the dependencies listed in your packages.yml from git
### For more info on commands see here: https://docs.getdbt.com/reference/dbt-commands



