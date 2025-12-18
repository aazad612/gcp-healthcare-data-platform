import pandas as pd

# 1. Create a simple DataFrame
data = {
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'score': [99.5, 88.0, 75.2]
}
df = pd.DataFrame(data)

# 2. Save as Parquet
filename = 'sample.parquet'
df.to_parquet(filename)

print(f"Created {filename}")