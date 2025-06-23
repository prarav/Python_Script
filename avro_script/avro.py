import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from fastavro import writer, parse_schema

# MariaDB connection
DB_NAME = "doptmoodle"
DB_USER = "root"
DB_PASSWORD = "redhat"
DB_HOST = "localhost"
DB_PORT = 3306

# Output directory
OUTPUT_DIR = "./avro_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Connect to MariaDB
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
inspector = inspect(engine)

# Get all table names
tables = inspector.get_table_names()

# Loop through all tables
for table in tables:
    print(f"Processing table: {table}")
    
    # Read table into DataFrame
    df = pd.read_sql_table(table, con=engine)

    # Auto-generate Avro schema from DataFrame
    def infer_avro_schema(df, name):
        fields = []
        for col in df.columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                avro_type = ["null", "long"]
            elif pd.api.types.is_float_dtype(dtype):
                avro_type = ["null", "double"]
            elif pd.api.types.is_bool_dtype(dtype):
                avro_type = ["null", "boolean"]
            else:
                avro_type = ["null", "string"]
            fields.append({"name": col, "type": avro_type})
        return {
            "type": "record",
            "name": name,
            "fields": fields
        }

    # Infer schema and convert rows to dicts
    schema = parse_schema(infer_avro_schema(df, table))
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    # Write to Avro file
    output_file = os.path.join(OUTPUT_DIR, f"{table}.avro")
    with open(output_file, "wb") as out:
        writer(out, schema, records)

    print(f"Written: {output_file}")

print("✅ Done exporting all tables to Avro.")
