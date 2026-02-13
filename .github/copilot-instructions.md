# DMI ETL dbt Project - AI Agent Instructions

## Project Overview
This is a **dbt analytics project** that transforms raw health surveillance data from multiple sources into dimensional/fact tables in PostgreSQL. The "SHIELD" data warehouse consolidates disease surveillance data (AFI, SARI-ILI, KHIS, mortality, etc.) from Kenya. The project follows a **layered medallion architecture**: raw sources → staging → intermediate transformations → data mart (dimensions + facts).

## Critical Architecture Patterns

### 1. **Three-Layer Transformation Pipeline**
- **Staging Layer** (`models/staging/`): 1-1 mappings from raw sources, basic column renaming, data type casting. Example: `stg_afi_screening.sql` selects from `source('central_raw_afi', 'screening')` with minimal logic.
- **Intermediate Layer** (`models/dmi_intermediate/`): Business logic, case classifications, unpivoting, aggregations. Example: `intermediate_afi_case_classification.sql` applies complex conditions for MERS-CoV, SARI, UF, DF classifications.
- **Data Mart Layer** (`models/dmi_data_mart/`): Fact tables (fct_*) and dimensions (dim_*) for analytics. Example: `fct_afi_aggregate_cascade.sql` joins multiple staging/intermediate tables with dimension tables (gender, age_group, epi_week, facility, date).

**Key Principle**: Never apply transformations in staging; keep intermediate and data mart logic separate for maintainability.

### 2. **Source System Organization**
Raw data comes from multiple schemas in the SHIELD database:
- `central_raw_afi`: AFI (Acute Febrile Illness) surveillance tables
- `central_raw_khis`: Ministry of Health MOH 505/705 forms
- `central_raw_sari_ili`: SARI/ILI disease data
- `central_raw_mortality`: Mortality surveillance
- `central_raw_mdharura`: M-dharura app aggregate/linelist data
- `central_raw_krcs`: Red Cross CBS data
- `central_raw_ears`: COVID-19 E-bridge data

All sources are defined in [models/sources.yml](models/sources.yml). Always use `source()` function for raw tables, never hardcode schema names.

### 3. **Naming Conventions**
- **Staging tables**: `stg_<disease>_<entity>` (e.g., `stg_afi_screening.sql`, `stg_khis_moh_705.sql`)
- **Intermediate tables**: `intermediate_<domain>_<logic>` (e.g., `intermediate_afi_case_classification.sql`, `intermediate_khis_moh_505_wide_to_long.sql`)
- **Fact tables**: `fct_<domain>_<metric>` (e.g., `fct_afi_aggregate_cascade.sql`)
- **Dimension tables**: `dim_<entity>` (e.g., `dim_facility.sql`, `dim_epi_week.sql`, `dim_age_group_afi_and_mortality.sql`)

### 4. **Common Join Patterns in Data Mart**
Fact tables commonly use dimension tables via `ref()`:
```sql
-- Pattern for joining dimensions with coalesce for missing keys
left join {{ ref('dim_gender') }} as gender on gender.code = source_column
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group 
  on source_column >= age_group.start_age and source_column < age_group.end_age
left join {{ ref('dim_epi_week') }} as epi_week 
  on date_column >= epi_week.start_of_week and date_column <= epi_week.end_of_week
left join {{ ref('dim_facility') }} as facility on facility.code = left(pid, 3)
left join {{ ref('dim_date') }} as date on date.date = date_column

-- Use coalesce with 'unset' for missing dimension keys
coalesce(gender.gender_key, 'unset') as gender_key
```

## Development Workflow

### Environment Setup
1. Python 3.8+ virtual environment (see `README.md`)
2. Install dbt-postgres: `pip install dbt-postgres`
3. Create `.env` with variables: `DBT_USER_DEV`, `DBT_PASSWORD_DEV`, `DBT_DATABASE_DEV`, `DBT_SERVER_DEV`, `DBT_PORT_DEV`, `DBT_SCHEMA`
4. Profiles configured in [profiles/profiles.yml](profiles/profiles.yml) with dev/test/prod targets

### Essential dbt Commands
```bash
dbt run --select staging              # Build all staging models
dbt run --select dmi_data_mart        # Build all data mart models
dbt run --select stg_afi_screening    # Build specific model
dbt test                              # Run all tests
dbt compile                           # Check SQL syntax
dbt docs generate && dbt docs serve   # Generate and view documentation (port 8000)
dbt deps                              # Install packages from packages.yml
dbt seed                              # Load CSVs from seeds/ (small reference data)
```

## Project-Specific Patterns

### 1. **Macro Usage**
- Custom macro: `cross_apply_columns()` in [macros/cross_apply_columns.sql](macros/cross_apply_columns.sql) - unpivots columns to rows using CROSS JOIN LATERAL (PostgreSQL-specific)
- Standard packages: dbt_utils, dbt_expectations, tsql_utils (see [packages.yml](packages.yml))

### 2. **Data Type Casting**
Always cast date fields explicitly: `"InterviewDate"::date as interview_date`. This is critical when joining with dimension tables.

### 3. **Handling Null Dimensions**
When a lookup fails, use `coalesce(dimension_key, 'unset')` to provide default dimension keys. This prevents NULL fact records and ensures referential integrity.

### 4. **Reference Data**
Seeds in [seeds/](seeds/) (CSV files):
- `kenya_counties.csv`: County reference
- `facility_mapping.csv`: Facility codes and names
- `sub_county_population.csv`: Population estimates
- `source_disease_indicator_mapppings.csv`: Indicator mappings

Use `ref()` to reference seeds as tables.

## Common Tasks & Code Examples

### Adding a New Staging Model
1. Create `models/staging/stg_<new_disease>.sql`
2. Pull from source: `from {{ source('central_raw_<domain>', 'table_name') }}`
3. Select and rename columns, cast dates/numbers
4. NO business logic—keep it simple pass-through with schema alignment

### Adding a New Fact Aggregation
1. Create `models/dmi_data_mart/fct_<domain>_<metric>.sql`
2. Build CTEs joining staging/intermediate with dimension tables
3. Use `group by` with dimension keys
4. Pattern: `select coalesce(dim.key, 'unset') as key, count(*) as metric from ... group by ...`

### Wide-to-Long Transformation
Look at `intermediate_khis_moh_505_wide_to_long.sql` for reference pattern—unpivots MOH form questions into rows.

## Dependencies & External Packages
- **dbt_utils** (0.8.6): Helper macros (generate_surrogate_key, etc.)
- **dbt_expectations** (0.10.8): dbt test functions (expect_column_values_to_be_in_set, etc.)
- **tsql_utils** (0.8.1): SQL Server/T-SQL utilities (less used in PostgreSQL context)

See [packages.yml](packages.yml) for versions and [dbt_packages/](dbt_packages/) for source code.

## File Structure Reference
```
├── models/
│   ├── sources.yml                    # All raw source definitions
│   ├── staging/                       # 1-1 raw to clean mapping
│   ├── dmi_intermediate/              # Business logic & transformations
│   ├── dmi_data_mart/                 # Facts & dimensions for analytics
│   └── dmi_reporting/                 # End-user reports (if used)
├── tests/                             # dbt test files (empty - tests in .yml)
├── macros/                            # Custom SQL macros
├── seeds/                             # Static reference data (CSVs)
├── profiles/profiles.yml              # Database connection config
├── dbt_project.yml                    # Project metadata & model config
└── README.md                          # Setup & command reference
```

## Testing & Quality Assurance
- Tests are typically defined in schema.yml files within model directories
- Use `dbt_expectations` for complex tests (column value validation)
- Run `dbt test` before commits to catch data/lineage issues

## Debugging Tips
- Check compiled SQL in `target/compiled/` after `dbt compile` or `dbt run`
- View DAG: `dbt docs generate` and inspect dependency graph
- Use `dbt run --debug` for verbose logging
- Check `target/run_results.json` for test failures and execution timing

---
**Last Updated**: January 2026 | For questions, check README.md or examine similar models (e.g., AFI cascade pattern for fact table examples).
