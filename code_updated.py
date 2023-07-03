from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pyarrow as pa
import os

# Get a list of all .gz.parquet files in the folder
folder_path = '/home/database-user/database_dump/public.stories/1/'
files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.gz.parquet')]

print("Started")

# Create a SQLAlchemy engine
engine = create_engine('postgresql://postgresdbuser:1337@localhost:5432/storiesdb')

# Iterate through each file
for file in files:
    # Read the file into a pyarrow Table
    table = pq.read_table(file)

    # Convert the table to a DataFrame in chunks
    chunk_size = 10000  # Adjust the chunk size as per your memory constraints
    for i in range(0, len(table), chunk_size):
        chunk = table.slice(i, min(chunk_size, len(table) - i)).to_pandas()

        # Write the chunk to the PostgreSQL table
        chunk.to_sql('storiestable', engine, if_exists='append')

print("Conversion complete")
