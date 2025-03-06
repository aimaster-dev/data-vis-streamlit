# Test Case Analysis Dashboard

This Streamlit application replicates a Test Case Analysis dashboard similar to the one shown in the reference images. It includes visualizations for test cases, labels, repositories, modules, methods, and groups across different stations.

## Project Structure

```
test_case_analysis/
│
├── app.py                    # Main Streamlit app entry point
├── requirements.txt          # Dependencies
│
├── pages/                    # Streamlit pages (multi-page app)
│   ├── __init__.py
│   ├── overview_summary.py   # Overview Summary page
│   └── ...                   # Other pages
│
├── components/              # Reusable UI components
│   ├── __init__.py
│   ├── filters.py           # Filter components
│   ├── metrics.py           # Metric components
│   └── charts.py            # Chart components
│
├── data/                    # Data management
│   ├── __init__.py
│   └── sample_data.py       # Generate sample data
│
├── utils/                   # Utility functions
│   ├── __init__.py
│   └── custom_css.py        # Custom CSS styling
│
└── assets/                  # Static assets
    └── logo.png             # Logo image
```

## Features

- Interactive time series visualizations for test metrics
- Multiple filter options (Station, Group, Label, Repo, Module)
- Summary metrics with key performance indicators
- Custom navigation sidebar
- Responsive layout optimized for wide screens

## Installation

1. Clone this repository:
```
git clone https://github.com/yourusername/test-case-analysis.git
cd test-case-analysis
```

2. Install the required packages:
```
pip install -r requirements.txt
```

## Usage

Run the Streamlit application:
```
streamlit run app.py
```

The application will start and open in your default web browser at `http://localhost:8501`.

## Customization

### Adding Real Data

To use real data instead of the generated sample data:

1. Modify the data loading functions in the `data` directory
2. Update the data structures to match your actual data schema
3. Adjust the visualizations as needed

### Adding More Pages

To add more pages to the dashboard:

1. Create a new file in the `pages` directory
2. Import and use the page in `app.py`
3. Add the page to the navigation options

## Technologies Used

- [Streamlit](https://streamlit.io/) - The web application framework
- [Plotly](https://plotly.com/) - Interactive visualizations
- [Pandas](https://pandas.pydata.org/) - Data manipulation and analysis
- [NumPy](https://numpy.org/) - Numerical computing

## License

This project is licensed under the MIT License - see the LICENSE file for details.