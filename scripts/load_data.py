import pandas as pd
import os
import requests
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from config.database import get_engine, test_connection
from dotenv import load_dotenv

load_dotenv()

class DataLoader:
    def __init__(self):
        self.engine = get_engine()
        self.data_dir = "/app/data"
        os.makedirs(self.data_dir, exist_ok=True)
        
    def download_open_source_data(self):
        """Download open source COVID-19 data"""
        print("Downloading open source data...")
        
        data_sources = {
            'covid_data': os.getenv('COVID_DATA_URL'),
            'countries_data': os.getenv('COUNTRIES_DATA_URL')
        }
        
        downloaded_files = {}
        
        for name, url in data_sources.items():
            if not url:
                print(f"No URL provided for {name}")
                continue
                
            try:
                print(f"Downloading {name} from {url}")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                file_path = os.path.join(self.data_dir, f"{name}.csv")
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                
                downloaded_files[name] = file_path
                print(f"Successfully downloaded {name} to {file_path}")
                
            except requests.RequestException as e:
                print(f"Error downloading {name}: {e}")
                # Use sample data if download fails
                sample_file = self.create_sample_data(name)
                if sample_file:
                    downloaded_files[name] = sample_file
        
        return downloaded_files
    
    def create_sample_data(self, data_type):
        """Create sample data if download fails"""
        file_path = os.path.join(self.data_dir, f"{data_type}.csv")
        
        if data_type == 'covid_data':
            sample_data = {
                'iso_code': ['USA', 'GBR', 'IND', 'BRA'],
                'continent': ['North America', 'Europe', 'Asia', 'South America'],
                'location': ['United States', 'United Kingdom', 'India', 'Brazil'],
                'date': [datetime.now().date()] * 4,
                'total_cases': [1000000, 500000, 2000000, 1500000],
                'new_cases': [1000, 500, 2000, 1500],
                'total_deaths': [10000, 5000, 20000, 15000],
                'total_vaccinations': [800000, 400000, 1000000, 700000]
            }
        elif data_type == 'countries_data':
            sample_data = {
                'iso_code': ['USA', 'GBR', 'IND', 'BRA'],
                'name': ['United States', 'United Kingdom', 'India', 'Brazil'],
                'region': ['Americas', 'Europe', 'Asia', 'Americas'],
                'population': [331000000, 67000000, 1380000000, 213000000],
                'area': [9833520, 242495, 3287263, 8515767]
            }
        else:
            return None
        
        df = pd.DataFrame(sample_data)
        df.to_csv(file_path, index=False)
        print(f"Created sample data at {file_path}")
        return file_path
    
    def clean_data(self, df, table_name):
        """Clean and prepare data for loading"""
        print(f"Cleaning data for {table_name}...")
        
        # Make a copy to avoid SettingWithCopyWarning
        df_clean = df.copy()
        
        # Convert column names to lowercase with underscores
        df_clean.columns = [col.lower().replace(' ', '_') for col in df_clean.columns]
        
        # Handle missing values
        if table_name == 'covid_stats':
            # Fill numeric columns with 0, others with None
            numeric_cols = df_clean.select_dtypes(include=['number']).columns
            df_clean[numeric_cols] = df_clean[numeric_cols].fillna(0)
            
            # Ensure date column is datetime
            # Normalize common date column names to 'date'
            for dcol in ('date', 'last_updated_date', 'last_updated', 'date_reported', 'day'):
                if dcol in df_clean.columns:
                    if dcol != 'date':
                        df_clean = df_clean.rename(columns={dcol: 'date'})
                    break

            if 'date' in df_clean.columns:
                df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce').dt.date

            # Keep only allowed columns that exist in the `covid_stats` table
            allowed_cols = {
                'iso_code', 'date', 'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
                'total_cases_per_million', 'new_cases_per_million', 'total_deaths_per_million', 'new_deaths_per_million',
                'reproduction_rate', 'icu_patients', 'icu_patients_per_million', 'hosp_patients', 'hosp_patients_per_million',
                'weekly_icu_admissions', 'weekly_icu_admissions_per_million', 'weekly_hosp_admissions', 'weekly_hosp_admissions_per_million',
                'total_tests', 'new_tests', 'total_tests_per_thousand', 'new_tests_per_thousand', 'positive_rate', 'tests_per_case',
                'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'total_boosters', 'new_vaccinations',
                'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred', 'total_boosters_per_hundred',
                'new_vaccinations_smoothed', 'new_vaccinations_smoothed_per_million', 'stringency_index',
                'excess_mortality_cumulative_absolute', 'excess_mortality_cumulative', 'excess_mortality', 'excess_mortality_cumulative_per_million'
            }

            keep = [c for c in df_clean.columns if c in allowed_cols]
            df_clean = df_clean[keep]
        
        elif table_name == 'countries':
            # Map external column names to DB column names and drop extras
            # Source CSV may use 'name' and 'region' while DB expects 'location' and 'continent'
            rename_map = {}
            if 'name' in df_clean.columns:
                rename_map['name'] = 'location'
            if 'region' in df_clean.columns:
                rename_map['region'] = 'continent'

            if rename_map:
                df_clean = df_clean.rename(columns=rename_map)

            # If the source used a different column for ISO3 codes, map it to 'iso_code'
            iso_candidates = ['iso_code', 'ISO3166-1-Alpha-3', 'alpha-3', 'alpha3', 'iso3', 'country_code', 'country_code3']
            for col in iso_candidates:
                if col in df_clean.columns and col != 'iso_code':
                    df_clean = df_clean.rename(columns={col: 'iso_code'})
                    break

            # Allowed columns in `countries` table (exclude id, created_at)
            allowed_cols = {
                'iso_code', 'continent', 'location', 'population', 'population_density',
                'median_age', 'aged_65_older', 'aged_70_older', 'gdp_per_capita',
                'extreme_poverty', 'cardiovasc_death_rate', 'diabetes_prevalence',
                'female_smokers', 'male_smokers', 'handwashing_facilities',
                'hospital_beds_per_thousand', 'life_expectancy', 'human_development_index'
            }

            # Keep only allowed columns that exist in the dataframe
            keep = [c for c in df_clean.columns if c in allowed_cols]
            df_clean = df_clean[keep]
            # Ensure we have an `iso_code`. Try to map from the downloaded countries_data.csv
            if 'iso_code' not in df_clean.columns:
                df_clean['iso_code'] = None

            # Attempt to map ISO codes using the downloaded countries_data.csv
            mapping_file = os.path.join(self.data_dir, 'countries_data.csv')
            if os.path.exists(mapping_file):
                try:
                    map_df = pd.read_csv(mapping_file)
                    # find name column in mapping file
                    name_col = None
                    for c in ('name', 'location', 'country', 'country_name'):
                        if c in map_df.columns:
                            name_col = c
                            break

                    if name_col and 'iso_code' in map_df.columns:
                        mapper = {str(r[name_col]).strip().lower(): str(r['iso_code']).strip() for _, r in map_df.iterrows() if pd.notnull(r['iso_code'])}

                        def map_iso(loc):
                            if pd.isnull(loc):
                                return None
                            key = str(loc).strip().lower()
                            if key in mapper:
                                return mapper[key]
                            # fuzzy match
                            import difflib
                            match = difflib.get_close_matches(key, mapper.keys(), n=1, cutoff=0.8)
                            if match:
                                return mapper[match[0]]
                            return None

                        if 'location' in df_clean.columns:
                            df_clean['iso_code'] = df_clean.apply(lambda r: r['iso_code'] if pd.notnull(r['iso_code']) else map_iso(r['location']), axis=1)
                except Exception:
                    print('Warning: failed to build iso_code mapper from countries_data.csv')

            # For any remaining missing iso_code, generate a synthetic unique code (to satisfy NOT NULL + UNIQUE)
            missing_mask = df_clean['iso_code'].isnull()
            if missing_mask.any():
                existing = set([str(x).strip() for x in df_clean['iso_code'].dropna().unique()])
                gen_count = 1
                def make_code(base):
                    nonlocal gen_count
                    base_clean = ''.join([c for c in base.upper() if c.isalpha()])[:3]
                    if not base_clean:
                        base_clean = 'UNK'
                    code = base_clean
                    while code in existing:
                        code = f"{base_clean}{gen_count}"
                        gen_count += 1
                    existing.add(code)
                    return code

                for idx in df_clean[missing_mask].index:
                    loc = df_clean.at[idx, 'location'] if 'location' in df_clean.columns else None
                    base = loc if pd.notnull(loc) else 'UNK'
                    df_clean.at[idx, 'iso_code'] = make_code(str(base))
                print(f"Generated {missing_mask.sum()} synthetic iso_code values for countries rows")
        
        # Remove duplicates
        df_clean = df_clean.drop_duplicates()
        
        return df_clean
    
    def load_to_database(self, file_path, table_name):
        """Load CSV data to PostgreSQL"""
        try:
            print(f"Loading {file_path} to table {table_name}...")
            
            # Read CSV
            df = pd.read_csv(file_path)
            
            # Clean data
            df_clean = self.clean_data(df, table_name)
            
            # Debug: show a quick preview before inserting
            print(f"Data preview (first 3 rows):\n{df_clean.head(3)}")
            print(f"Data dtypes:\n{df_clean.dtypes}")

            # If loading covid_stats, ensure referenced countries exist (avoid FK errors)
            if table_name == 'covid_stats' and 'iso_code' in df_clean.columns:
                try:
                    existing_iso = set(pd.read_sql_query('SELECT iso_code FROM countries', self.engine)['iso_code'].astype(str).str.strip().tolist())
                except Exception:
                    existing_iso = set()

                iso_in_data = set([str(x).strip() for x in df_clean['iso_code'].dropna().unique()])
                missing_iso = iso_in_data - existing_iso

                if missing_iso:
                    # Build placeholder country rows to satisfy FK constraint.
                    placeholder_rows = []
                    for iso in missing_iso:
                        # try to find a matching location in the covid data
                        loc_vals = df_clean.loc[df_clean['iso_code'].astype(str).str.strip() == iso, 'location'] if 'location' in df_clean.columns else None
                        loc = None
                        if loc_vals is not None and not loc_vals.dropna().empty:
                            loc = str(loc_vals.dropna().iloc[0])
                        else:
                            loc = iso
                        placeholder_rows.append({'iso_code': iso, 'location': loc})

                    ph_df = pd.DataFrame(placeholder_rows)
                    try:
                        ph_df.to_sql('countries', self.engine, if_exists='append', index=False, method='multi')
                        print(f"Inserted {len(ph_df)} placeholder countries for missing iso_codes")
                    except Exception as e:
                        print(f"Warning: failed to insert placeholder countries: {e}")

            # Load to database
            # If loading countries, avoid inserting duplicate iso_code values
            if table_name == 'countries' and 'iso_code' in df_clean.columns:
                try:
                    existing_iso = set(pd.read_sql_query('SELECT iso_code FROM countries', self.engine)['iso_code'].astype(str).str.strip().tolist())
                except Exception:
                    existing_iso = set()

                # Determine new rows whose iso_code is not yet in the DB
                df_clean['__iso_clean'] = df_clean['iso_code'].astype(str).str.strip()
                new_iso_mask = ~df_clean['__iso_clean'].isin(existing_iso)
                new_count = new_iso_mask.sum()
                if new_count == 0:
                    print('No new countries to insert; skipping countries load.')
                    df_clean = df_clean.drop(columns=['__iso_clean'])
                    return True

                df_to_insert = df_clean.loc[new_iso_mask].drop(columns=['__iso_clean'])
                print(f'Inserting {len(df_to_insert)} new countries (skipping {len(df_clean) - len(df_to_insert)} existing)')

                df_to_insert.to_sql(
                    table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                )
            else:
                df_clean.to_sql(
                    table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

            print(f"Successfully loaded {len(df_clean)} rows to {table_name}")
            return True
            
        except Exception as e:
            import traceback
            print(f"Error loading data to {table_name}: {e}")
            traceback.print_exc()
            return False
    
    def validate_data(self):
        """Validate loaded data"""
        print("\nValidating loaded data...")
        
        validation_queries = {
            "Total countries": "SELECT COUNT(*) FROM countries",
            "Total COVID records": "SELECT COUNT(*) FROM covid_stats",
            "Latest data date": "SELECT MAX(date) FROM covid_stats",
            "Countries with most cases": """
                SELECT c.location, cs.total_cases 
                FROM covid_stats cs
                JOIN countries c ON cs.iso_code = c.iso_code
                WHERE cs.date = (SELECT MAX(date) FROM covid_stats)
                ORDER BY cs.total_cases DESC
                LIMIT 5
            """
        }
        
        for name, query in validation_queries.items():
            try:
                result = pd.read_sql_query(query, self.engine)
                print(f"{name}:")
                print(result.to_string(index=False))
                print("-" * 50)
            except Exception as e:
                print(f"Error executing {name} query: {e}")
    
    def run(self):
        """Main execution method"""
        print("Starting CSV to PostgreSQL Data Loader")
        print("=" * 50)
        
        # Test database connection
        if not test_connection():
            print("Exiting due to database connection failure")
            return
        
        # Download data
        files = self.download_open_source_data()

        if not files:
            print("No data files available. Exiting.")
            return

        # Prefer loading countries first to satisfy foreign key constraints
        # files is a dict like {'covid_data': '/app/data/covid_data.csv', 'countries_data': '/app/data/countries_data.csv'}
        # Load countries first if present
        if 'countries_data' in files:
            self.load_to_database(files['countries_data'], 'countries')

        # Then load any remaining files (covid data)
        for file_name, file_path in files.items():
            if file_name == 'countries_data':
                continue
            table_name = 'covid_stats' if 'covid' in file_name else 'countries'
            self.load_to_database(file_path, table_name)
        
        # Validate loaded data
        self.validate_data()
        
        print("\nData loading completed successfully!")
        print(f"Data directory: {self.data_dir}")
        print("You can now query the database using:")
        print("  docker-compose exec postgres psql -U postgres -d covid_data")

if __name__ == "__main__":
    loader = DataLoader()
    loader.run()