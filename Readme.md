# User Analysis & Similarity Visualization Tool

## Key Features

- **Data Storage**: Uses SQLite for lightweight, serverless storage while maintaining SQL querying capabilities
- **Similarity Analysis**: Leverages sentence-transformers (all-MiniLM-L6-v2) to generate embeddings from user descriptions, enabling semantic similarity comparisons
- **Network Visualization**: Creates interactive network graphs showing user similarities with:
  - Node sizes reflecting average similarity scores
  - Edge thickness indicating similarity strength
  - Color gradients representing similarity distributions
- **Statistical Visualizations**: Generates multiple plot types (pie, treemap, bar charts) for different properties to provide various perspectives on the data

## Quick Start

1. Install dependencies:

```
pip install -r requirements.txt
```

2. Run the tool:

```
python main.py fetch # Fetch sample user data
python main.py ingest # Store in SQLite database
python main.py analyze # Generate visualizations
```

## Results

The results can be seen inside the visualizations directory and its subdirectories.
