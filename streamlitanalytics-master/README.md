# Academy Insights Dashboard 🏆

A Streamlit application for drilling down through hierarchical sports academy data stored in Databricks. Visualize participant engagement, session attendance, skill achievements, and trends across regions, cities, communities, and individual academies.

## Features

- **Geographic Drill-Down**: Navigate from Region → City → Community → Academy
- **Real-time Metrics**: Track participants, sessions, attendance rates, and skill achievements
- **Trend Analysis**: Visualize attendance patterns over the last 90 days
- **Interactive Visualizations**: Plotly charts for distribution and comparative analysis
- **Cached Queries**: Optimized performance with intelligent data caching

## Data Model

The application supports the following hierarchical structure based on your Mermaid diagram:

```
REGION
  └─ CITY
      └─ COMMUNITY
          └─ ACADEMY
              └─ COHORT
                  ├─ PARTICIPANT
                  └─ SESSION
                      ├─ ATTENDANCE
                      ├─ ASSESSMENT
                      └─ SPORT_PARTICIPATION
```

## Installation

### Prerequisites

- Python 3.8 or higher
- Access to a Databricks workspace with SQL warehouse
- Databricks personal access token

### Setup

1. Clone or download this repository:
```bash
git clone <your-repo-url>
cd academy-insights-dashboard
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. (Optional) Create a `.env` file for easier connection management:
```bash
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxxxxxxx
DATABRICKS_TOKEN=dapi********************************
```

## Usage

### Running the Application

```bash
streamlit run academy_insights_app.py
```

The app will open in your default browser at `http://localhost:8501`

### Configuration

1. **Databricks Connection** (in sidebar):
   - **Server Hostname**: Your Databricks workspace URL (without `https://`)
   - **HTTP Path**: SQL warehouse HTTP path from Databricks
   - **Access Token**: Your personal access token
   - **Catalog**: Your Unity Catalog name (default: `main`)
   - **Schema**: Your schema name (default: `academy_data`)

2. Click **Connect** to establish the connection

3. Use the geographic filters to drill down through your data

### Getting Databricks Credentials

1. **Server Hostname**: 
   - Go to your Databricks workspace
   - Copy the URL (e.g., `adb-123456789.12.azuredatabricks.net`)

2. **HTTP Path**:
   - Navigate to SQL Warehouses in Databricks
   - Click on your warehouse
   - Go to Connection Details
   - Copy the HTTP Path

3. **Access Token**:
   - Click your profile icon → Settings
   - Navigate to Developer → Access Tokens
   - Click "Generate New Token"
   - Copy and save the token securely

## Database Schema Requirements

The application expects the following tables to exist in your Databricks catalog:

### Core Tables

- `region` - Regional divisions
- `city` - Cities within regions
- `community` - Communities within cities
- `academy` - Academy locations
- `cohort` - Program cohorts
- `participant` - Program participants
- `session` - Training sessions
- `attendance` - Session attendance records
- `skill_achievement` - Skill completions
- `assessment` - Participant assessments
- `sport_participation` - Sport activity logs

### Lookup Tables

- `product_type` - Academy product types
- `sport_type` - Types of sports offered
- `skill` - Available skills
- `outcome_type` - Outcome categories
- `workshop_theme` - Workshop themes
- `lesson_plan` - Lesson plan templates
- `staff` - Staff members

### Minimum Required Columns

**region:**
- `region_id` (INT)
- `region` (STRING)

**city:**
- `city_id` (INT)
- `city_name` (STRING)
- `region_id` (INT)

**community:**
- `community_id` (INT)
- `community_name` (STRING)
- `city_id` (INT)

**academy:**
- `academy_id` (INT)
- `academy_code` (STRING)
- `community_id` (INT)
- `product_type_id` (INT)

**cohort:**
- `cohort_id` (INT)
- `academy_id` (INT)

**participant:**
- `participant_id` (INT)
- `cohort_id` (INT)
- `enrollment_date` (DATE)
- `is_active` (BOOLEAN)

**session:**
- `session_id` (INT)
- `cohort_id` (INT)
- `session_date` (DATE)
- `duration_minutes` (INT)

**attendance:**
- `attendance_id` (INT)
- `session_id` (INT)
- `participant_id` (INT)
- `attended` (BOOLEAN)

**skill_achievement:**
- `skill_achievement_id` (INT)
- `participant_id` (INT)
- `skill_id` (INT)

## Customization

### Modifying Queries

All SQL queries are defined within the caching functions in `academy_insights_app.py`. To customize:

1. Locate the relevant function (e.g., `load_metrics_data`, `load_trend_data`)
2. Modify the SQL query string
3. Update column references in the visualization code if needed

### Adding New Visualizations

To add custom charts:

```python
# Add after existing visualizations
st.header("Your Custom Section")

custom_query = f"""
    SELECT ... FROM {catalog}.{schema}.your_table
    WHERE ... {where_clause}
"""

custom_data = st.session_state.db_conn.query(custom_query)

fig = px.bar(custom_data, x='column1', y='column2')
st.plotly_chart(fig, use_container_width=True)
```

### Adjusting Cache TTL

Modify the `@st.cache_data(ttl=3600)` decorator values:
- `ttl=3600` → 1 hour cache
- `ttl=1800` → 30 minutes cache
- Increase for more stable data, decrease for real-time requirements

## Performance Optimization

### For Large Datasets

1. **Add date range filters** to limit query scope:
```python
date_from = st.date_input("From Date", datetime.now() - timedelta(days=90))
date_to = st.date_input("To Date", datetime.now())
```

2. **Implement pagination** for large result sets:
```python
LIMIT = 1000
OFFSET = page_number * LIMIT
```

3. **Use materialized views** in Databricks for frequently accessed aggregations

4. **Add indexes** on frequently filtered columns (region_id, city_id, etc.)

### Memory Management

The app uses Streamlit's caching system. To clear cache manually:
- Click "Refresh Data" in the sidebar
- Or press 'C' in the app to open the menu and select "Clear cache"

## Troubleshooting

### Common Issues

**"Connection failed" error:**
- Verify your credentials are correct
- Ensure your SQL warehouse is running
- Check network connectivity to Databricks

**"Query failed" error:**
- Verify table names match your schema
- Check column names in the queries
- Review SQL warehouse logs in Databricks

**Empty visualizations:**
- Ensure you have data for the selected filters
- Check date ranges in trend queries
- Verify relationships between tables

**Slow performance:**
- Reduce cache TTL values
- Add WHERE clauses to limit data volume
- Consider using Databricks' serverless SQL warehouses

## Security Best Practices

1. **Never commit credentials** to version control
2. Use **environment variables** or `.env` files
3. Restrict **token permissions** to read-only access
4. Set token **expiration dates**
5. Use **IP access lists** in Databricks if possible
6. Consider **service principals** for production deployments

## Development

### Project Structure

```
academy-insights-dashboard/
├── academy_insights_app.py  # Main application
├── requirements.txt         # Python dependencies
├── .env.example            # Example environment file
└── README.md               # This file
```

### Extending the Application

To add new drill-down dimensions:

1. Add the hierarchy query to `load_hierarchy_data()`
2. Add a new selectbox in the drill-down section
3. Pass the new filter ID to `load_metrics_data()` and `load_trend_data()`
4. Update WHERE clauses in metric queries

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request with a clear description

## License

[Your License Here]

## Support

For issues or questions:
- Open an issue in the repository
- Contact your Databricks administrator for connection issues
- Review Databricks SQL documentation: https://docs.databricks.com/sql/

## Acknowledgments

Built with:
- [Streamlit](https://streamlit.io/) - Web framework
- [Plotly](https://plotly.com/) - Interactive visualizations
- [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html) - Database connectivity
