import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional, Dict, List
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Academy Insights Dashboard",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .stMetric {
        background-color: white;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

class DatabricksConnection:
    """Manages Databricks SQL connection"""
    
    def __init__(self):
        self.connection = None
        
    def connect(self, server_hostname: str, http_path: str, access_token: str):
        """Establish connection to Databricks"""
        try:
            self.connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token
            )
            return True
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
            return False
    
    def query(self, sql_query: str) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        if not self.connection:
            st.error("No active connection")
            return pd.DataFrame()
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
        except Exception as e:
            st.error(f"Query failed: {str(e)}")
            return pd.DataFrame()
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()

@st.cache_data(ttl=3600, show_spinner="Loading all data from Databricks...")
def load_all_data(_db_conn: DatabricksConnection, catalog: str, schema: str) -> Dict[str, pd.DataFrame]:
    """
    Load all required data from Databricks in ONE batch operation.
    This significantly improves performance by:
    1. Making fewer database queries
    2. Allowing local pandas filtering (much faster than remote SQL)
    3. Enabling Streamlit's caching
    """
    
    data = {}
    
    # Load hierarchical/dimension tables
    dimension_queries = {
        'regions': f"SELECT * FROM {catalog}.{schema}.region",
        'cities': f"SELECT * FROM {catalog}.{schema}.city",
        'communities': f"SELECT * FROM {catalog}.{schema}.community",
        'academies': f"SELECT * FROM {catalog}.{schema}.academy",
        'cohorts': f"SELECT * FROM {catalog}.{schema}.cohort",
        'product_types': f"SELECT * FROM {catalog}.{schema}.product_type",
        'sport_types': f"SELECT * FROM {catalog}.{schema}.sport_type",
    }
    
    # Load fact tables
    fact_queries = {
        'participants': f"SELECT * FROM {catalog}.{schema}.participant",
        'sessions': f"SELECT * FROM {catalog}.{schema}.session",
        'attendance': f"SELECT * FROM {catalog}.{schema}.attendance",
        'skill_achievements': f"SELECT * FROM {catalog}.{schema}.skill_achievement",
    }
    
    all_queries = {**dimension_queries, **fact_queries}
    
    progress_text = st.empty()
    progress_bar = st.progress(0)
    
    total = len(all_queries)
    for idx, (key, query) in enumerate(all_queries.items()):
        progress_text.text(f"Loading {key}... ({idx+1}/{total})")
        progress_bar.progress((idx + 1) / total)
        
        try:
            df = _db_conn.query(query)
            data[key] = df
            
            # Normalize column names to handle string-based booleans
            if key == 'attendance' and 'attendance_status' in df.columns:
                df['attended_normalized'] = df['attendance_status'].apply(
                    lambda x: str(x).upper() in ['TRUE', 'PRESENT', '1', 'YES'] if pd.notna(x) else False
                )
            
            if key == 'participants' and 'is_active' in df.columns:
                df['is_active_normalized'] = df['is_active'].apply(
                    lambda x: str(x).upper() in ['TRUE', 'ACTIVE', '1', 'YES'] if pd.notna(x) else False
                )
            
        except Exception as e:
            st.error(f"Error loading {key}: {str(e)}")
            data[key] = pd.DataFrame()
    
    progress_text.empty()
    progress_bar.empty()
    
    return data

def build_hierarchy_df(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a complete hierarchy DataFrame by joining all dimension tables.
    This creates a denormalized view for fast filtering.
    """
    
    if data['academies'].empty:
        return pd.DataFrame()
    
    # Start with academies
    hierarchy = data['academies'].copy()
    
    # Join communities
    if not data['communities'].empty:
        hierarchy = hierarchy.merge(
            data['communities'][['community_id', 'community_name', 'city_id']],
            on='community_id',
            how='left'
        )
    else:
        hierarchy['community_name'] = None
        hierarchy['city_id'] = None
    
    # Join cities
    if not data['cities'].empty:
        hierarchy = hierarchy.merge(
            data['cities'][['city_id', 'city_name', 'region_id']],
            on='city_id',
            how='left'
        )
    else:
        hierarchy['city_name'] = None
        hierarchy['region_id'] = None
    
    # Join regions
    if not data['regions'].empty:
        hierarchy = hierarchy.merge(
            data['regions'][['region_id', 'region_name']],
            on='region_id',
            how='left'
        )
    else:
        hierarchy['region_name'] = None
    
    # Join product types
    if not data['product_types'].empty:
        hierarchy = hierarchy.merge(
            data['product_types'][['product_type_id', 'product_type_name']],
            on='product_type_id',
            how='left'
        )
    else:
        hierarchy['product_type_name'] = 'Unknown'
    
    return hierarchy

def filter_data(data: Dict[str, pd.DataFrame], hierarchy: pd.DataFrame,
                region_id: Optional[int] = None,
                city_id: Optional[int] = None,
                community_id: Optional[int] = None,
                academy_id: Optional[int] = None) -> Dict[str, pd.DataFrame]:
    """
    Filter all data based on hierarchical selections using pandas operations.
    This is MUCH faster than re-querying the database.
    """
    
    filtered = {}
    
    # Filter hierarchy
    filtered_hierarchy = hierarchy.copy()
    if region_id:
        filtered_hierarchy = filtered_hierarchy[filtered_hierarchy['region_id'] == region_id]
    if city_id:
        filtered_hierarchy = filtered_hierarchy[filtered_hierarchy['city_id'] == city_id]
    if community_id:
        filtered_hierarchy = filtered_hierarchy[filtered_hierarchy['community_id'] == community_id]
    if academy_id:
        filtered_hierarchy = filtered_hierarchy[filtered_hierarchy['academy_id'] == academy_id]
    
    filtered['hierarchy'] = filtered_hierarchy
    
    # Get filtered academy IDs
    academy_ids = filtered_hierarchy['academy_id'].unique()
    
    # Filter cohorts
    filtered['cohorts'] = data['cohorts'][data['cohorts']['academy_id'].isin(academy_ids)]
    cohort_ids = filtered['cohorts']['cohort_id'].unique()
    
    # Filter participants
    filtered['participants'] = data['participants'][data['participants']['cohort_id'].isin(cohort_ids)]
    participant_ids = filtered['participants']['participant_id'].unique()
    
    # Filter sessions
    filtered['sessions'] = data['sessions'][data['sessions']['cohort_id'].isin(cohort_ids)]
    session_ids = filtered['sessions']['session_id'].unique()
    
    # Filter attendance
    filtered['attendance'] = data['attendance'][data['attendance']['session_id'].isin(session_ids)]
    
    # Filter skill achievements
    filtered['skill_achievements'] = data['skill_achievements'][
        data['skill_achievements']['participant_id'].isin(participant_ids)
    ]
    
    return filtered

def calculate_metrics(filtered_data: Dict[str, pd.DataFrame]) -> Dict[str, any]:
    """Calculate all metrics from filtered pandas DataFrames"""
    
    metrics = {}
    
    # Participant metrics
    participants = filtered_data['participants']
    metrics['participants'] = {
        'total_participants': len(participants),
        'active_participants': participants['is_active_normalized'].sum() if 'is_active_normalized' in participants.columns else 0,
        'avg_days_enrolled': (pd.Timestamp.now() - pd.to_datetime(participants['enrollment_date'])).dt.days.mean() if not participants.empty and 'enrollment_date' in participants.columns else 0
    }
    
    # Session metrics
    sessions = filtered_data['sessions']
    metrics['sessions'] = {
        'total_sessions': len(sessions),
        'unique_session_days': sessions['session_date'].nunique() if not sessions.empty and 'session_date' in sessions.columns else 0,
        'avg_session_duration': sessions['duration_minutes'].mean() if not sessions.empty and 'duration_minutes' in sessions.columns else 0
    }
    
    # Attendance metrics
    attendance = filtered_data['attendance']
    metrics['attendance'] = {
        'total_attendances': len(attendance),
        'attendance_rate': (attendance['attended_normalized'].mean() * 100) if not attendance.empty and 'attended_normalized' in attendance.columns else 0,
        'participants_attended': attendance['participant_id'].nunique() if not attendance.empty else 0
    }
    
    # Skill achievements
    skills = filtered_data['skill_achievements']
    metrics['skills'] = {
        'total_achievements': len(skills),
        'unique_skills_achieved': skills['skill_id'].nunique() if not skills.empty and 'skill_id' in skills.columns else 0,
        'participants_with_achievements': skills['participant_id'].nunique() if not skills.empty else 0
    }
    
    return metrics

def calculate_trends(filtered_data: Dict[str, pd.DataFrame], days_back: int = 90) -> pd.DataFrame:
    """Calculate attendance trends from filtered data"""
    
    attendance = filtered_data['attendance']
    sessions = filtered_data['sessions']
    
    if attendance.empty or sessions.empty:
        return pd.DataFrame()
    
    # Merge to get session dates
    att_with_dates = attendance.merge(
        sessions[['session_id', 'session_date']],
        on='session_id',
        how='left'
    )
    
    # Filter by date range
    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_back)
    att_with_dates = att_with_dates[pd.to_datetime(att_with_dates['session_date']) >= cutoff_date]
    
    if att_with_dates.empty:
        return pd.DataFrame()
    
    # Add week column
    att_with_dates['week'] = pd.to_datetime(att_with_dates['session_date']).dt.to_period('W').dt.start_time
    
    # Calculate metrics by week
    trends = att_with_dates.groupby('week').agg(
        unique_attendees=('participant_id', 'nunique'),
        total_attendances=('participant_id', 'count'),
        attendance_rate=('attended_normalized', lambda x: x.mean() * 100 if 'attended_normalized' in att_with_dates.columns else 0)
    ).reset_index()
    
    return trends.sort_values('week')

def main():
    st.title("🏆 Academy Insights Dashboard")
    st.markdown("Drill down through your sports academy data by region, city, community, and academy")
    
    # Sidebar for connection settings
    with st.sidebar:
        st.header("🔌 Databricks Connection")
        
        # Check if .env values are available
        env_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
        env_http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
        env_token = os.getenv("DATABRICKS_TOKEN", "")
        env_catalog = os.getenv("DATABRICKS_CATALOG", "main")
        env_schema = os.getenv("DATABRICKS_SCHEMA", "academy_data")
        
        if env_hostname or env_http_path or env_token:
            st.success("✅ .env file detected")
        
        server_hostname = st.text_input(
            "Server Hostname",
            value=env_hostname,
            help="e.g., your-workspace.cloud.databricks.com"
        )
        
        http_path = st.text_input(
            "HTTP Path",
            value=env_http_path,
            help="e.g., /sql/1.0/warehouses/xxxxx"
        )
        
        access_token = st.text_input(
            "Access Token",
            type="password",
            value=env_token,
            help="Your Databricks personal access token"
        )
        
        catalog = st.text_input("Catalog", value=env_catalog)
        schema = st.text_input("Schema", value=env_schema)
        
        connect_button = st.button("Connect & Load Data", type="primary")
        
        st.divider()
        
        if 'data_loaded' in st.session_state and st.session_state.data_loaded:
            st.success("✅ Data Loaded")
            st.info(f"⚡ Using pandas filtering (fast!)")
            if st.button("🔄 Reload Data from Database"):
                st.cache_data.clear()
                st.session_state.data_loaded = False
                st.rerun()
    
    # Initialize connection
    if 'db_conn' not in st.session_state:
        st.session_state.db_conn = DatabricksConnection()
        st.session_state.connected = False
        st.session_state.data_loaded = False
    
    if connect_button and server_hostname and http_path and access_token:
        with st.spinner("Connecting to Databricks..."):
            if st.session_state.db_conn.connect(server_hostname, http_path, access_token):
                st.session_state.connected = True
                # Load all data at once
                st.session_state.all_data = load_all_data(st.session_state.db_conn, catalog, schema)
                st.session_state.hierarchy = build_hierarchy_df(st.session_state.all_data)
                st.session_state.data_loaded = True
                st.rerun()
    
    if not st.session_state.data_loaded:
        st.info("👈 Please configure your Databricks connection in the sidebar to get started")
        st.markdown("""
        ### Setup Instructions:
        1. Enter your Databricks workspace hostname
        2. Provide the SQL warehouse HTTP path
        3. Add your personal access token
        4. Specify your catalog and schema names
        5. Click **Connect & Load Data**
        
        **Note:** All data will be loaded once into memory for fast local filtering!
        """)
        return
    
    # Get data from session state
    all_data = st.session_state.all_data
    hierarchy = st.session_state.hierarchy
    
    if hierarchy.empty:
        st.error("❌ No hierarchy data found. Check your tables.")
        return
    
    # Drill-down filters
    st.header("📍 Geographic Drill-Down")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        regions = hierarchy[['region_id', 'region_name']].dropna().drop_duplicates()
        region_options = ["All"] + sorted(regions['region_name'].unique().tolist())
        selected_region = st.selectbox("Region", options=region_options, key="region_select")
        region_id = None if selected_region == "All" else \
            regions[regions['region_name'] == selected_region]['region_id'].iloc[0]
    
    with col2:
        filtered_hierarchy = hierarchy.copy()
        if region_id:
            filtered_hierarchy = filtered_hierarchy[filtered_hierarchy['region_id'] == region_id]
        
        cities = filtered_hierarchy[['city_id', 'city_name']].dropna().drop_duplicates()
        city_options = ["All"] + sorted(cities['city_name'].unique().tolist())
        selected_city = st.selectbox("City", options=city_options, key="city_select")
        city_id = None if selected_city == "All" else \
            cities[cities['city_name'] == selected_city]['city_id'].iloc[0]
    
    with col3:
        if city_id:
            filtered_hierarchy = filtered_hierarchy[filtered_hierarchy['city_id'] == city_id]
        
        communities = filtered_hierarchy[['community_id', 'community_name']].dropna().drop_duplicates()
        community_options = ["All"] + sorted(communities['community_name'].unique().tolist())
        selected_community = st.selectbox("Community", options=community_options, key="community_select")
        community_id = None if selected_community == "All" else \
            communities[communities['community_name'] == selected_community]['community_id'].iloc[0]
    
    with col4:
        if community_id:
            filtered_hierarchy = filtered_hierarchy[filtered_hierarchy['community_id'] == community_id]
        
        academies = filtered_hierarchy[['academy_id', 'academy_code']].dropna().drop_duplicates()
        academy_options = ["All"] + sorted(academies['academy_code'].unique().tolist())
        selected_academy = st.selectbox("Academy", options=academy_options, key="academy_select")
        academy_id = None if selected_academy == "All" else \
            academies[academies['academy_code'] == selected_academy]['academy_id'].iloc[0]
    
    st.divider()
    
    # Filter all data based on selections (FAST - all in pandas!)
    filtered_data = filter_data(all_data, hierarchy, region_id, city_id, community_id, academy_id)
    
    # Calculate metrics
    metrics = calculate_metrics(filtered_data)
    
    # Key Metrics
    st.header("📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_participants = metrics['participants']['total_participants']
        active_participants = int(metrics['participants']['active_participants'])
        st.metric(
            "Total Participants",
            f"{total_participants:,}",
            f"{active_participants:,} active"
        )
    
    with col2:
        total_sessions = metrics['sessions']['total_sessions']
        avg_duration = metrics['sessions']['avg_session_duration']
        st.metric(
            "Total Sessions",
            f"{total_sessions:,}",
            f"{avg_duration:.0f} min avg" if pd.notna(avg_duration) else "N/A"
        )
    
    with col3:
        attendance_rate = metrics['attendance']['attendance_rate']
        st.metric(
            "Attendance Rate",
            f"{attendance_rate:.1f}%" if pd.notna(attendance_rate) else "N/A"
        )
    
    with col4:
        total_achievements = metrics['skills']['total_achievements']
        unique_skills = metrics['skills']['unique_skills_achieved']
        st.metric(
            "Skill Achievements",
            f"{total_achievements:,}",
            f"{unique_skills} unique skills" if unique_skills else "N/A"
        )
    
    # Trends
    col_header, col_range = st.columns([3, 1])
    with col_header:
        st.header("📈 Attendance Trends")
    with col_range:
        days_back = st.selectbox(
            "Time Period",
            options=[30, 60, 90, 180, 365],
            index=2,
            format_func=lambda x: f"Last {x} days"
        )
    
    trend_data = calculate_trends(filtered_data, days_back)
    
    if not trend_data.empty:
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=trend_data['week'],
            y=trend_data['unique_attendees'],
            name='Unique Attendees',
            mode='lines+markers',
            line=dict(color='#1f77b4', width=2)
        ))
        
        fig.add_trace(go.Scatter(
            x=trend_data['week'],
            y=trend_data['attendance_rate'],
            name='Attendance Rate (%)',
            mode='lines+markers',
            line=dict(color='#ff7f0e', width=2),
            yaxis='y2'
        ))
        
        fig.update_layout(
            xaxis_title='Week',
            yaxis=dict(title='Unique Attendees'),
            yaxis2=dict(title='Attendance Rate (%)', overlaying='y', side='right'),
            hovermode='x unified',
            height=400,
            showlegend=True
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(f"⚠️ No attendance data found in the last {days_back} days for the selected filters.")
    
    # Distribution views
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Academy Distribution")
        academy_dist = filtered_data['hierarchy'][['academy_id', 'product_type_name']].drop_duplicates()
        if not academy_dist.empty and 'product_type_name' in academy_dist.columns:
            product_counts = academy_dist['product_type_name'].value_counts()
            if not product_counts.empty:
                fig = px.pie(
                    values=product_counts.values,
                    names=product_counts.index,
                    title='Academies by Product Type',
                    hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No product type data available")
        else:
            st.info("No academy distribution data available")
    
    with col2:
        st.subheader("Cohort Overview")
        cohort_counts = filtered_data['cohorts'].groupby('academy_id').size().reset_index(name='cohort_count')
        if not cohort_counts.empty:
            cohort_counts = cohort_counts.merge(
                filtered_data['hierarchy'][['academy_id', 'academy_code']].drop_duplicates(),
                on='academy_id',
                how='left'
            )
            top_academies = cohort_counts.nlargest(10, 'cohort_count')
            if not top_academies.empty:
                fig = px.bar(
                    top_academies,
                    x='academy_code',
                    y='cohort_count',
                    title='Top 10 Academies by Cohort Count'
                )
                fig.update_xaxes(tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No cohort data available")
        else:
            st.info("No cohort overview data available")
    
    # Performance info
    with st.expander("ℹ️ Performance Info"):
        st.markdown("""
        **Optimized Performance Strategy:**
        - ✅ All data loaded once from Databricks (cached for 1 hour)
        - ✅ Filtering performed locally using pandas (microseconds vs seconds)
        - ✅ No database queries when changing filters
        - ✅ Calculations done in-memory on pre-loaded data
        
        **To refresh data from database:** Click "Reload Data from Database" in sidebar
        """)
    
    # Natural Language Query Chatbot
    st.divider()
    st.header("💬 Ask Questions About Your Data")
    st.markdown("Use natural language to query your academy data. The AI will translate your question into insights.")
    
    # Sample questions
    with st.expander("💡 Example Questions"):
        st.markdown("""
        **Attendance & Engagement:**
        - Which academy has the highest attendance rate?
        - Show me participants who haven't attended in the last 30 days
        - What's the average session duration by region?
        
        **Performance & Skills:**
        - Which participants have achieved the most skills?
        - List academies with the lowest skill achievement rates
        - Show me the top 5 skills being learned this month
        
        **Operational Insights:**
        - How many sessions were scheduled last week?
        - Which communities have the most active participants?
        - Compare attendance rates between product types
        
        **Trends & Patterns:**
        - Is attendance improving or declining this quarter?
        - Which day of the week has the best attendance?
        - Show me growth in participant enrollment over time
        """)
    
    # Initialize chat history
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    # Chat interface
    col_input, col_button = st.columns([4, 1])
    
    with col_input:
        user_query = st.text_input(
            "Ask a question:",
            placeholder="e.g., Which academy has the best attendance rate?",
            label_visibility="collapsed",
            key="user_query_input"
        )
    
    with col_button:
        ask_button = st.button("Ask", type="primary", use_container_width=True)
    
    if ask_button and user_query:
        # Add user message to history
        st.session_state.chat_history.append({
            "role": "user",
            "content": user_query
        })
        
        # Generate mock response based on question
        response = generate_mock_response(user_query, filtered_data, metrics)
        
        # Add assistant response to history
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": response
        })
        
        # Clear input (this will happen on rerun)
        st.rerun()
    
    # Display chat history
    if st.session_state.chat_history:
        st.markdown("---")
        st.subheader("Conversation")
        
        for message in st.session_state.chat_history:
            if message["role"] == "user":
                st.markdown(f"**You:** {message['content']}")
            else:
                st.markdown(f"**AI Assistant:** {message['content']}")
                st.markdown("")
        
        if st.button("Clear Conversation"):
            st.session_state.chat_history = []
            st.rerun()

def generate_mock_response(query: str, filtered_data: Dict[str, pd.DataFrame], metrics: Dict) -> str:
    """
    Generate mock responses to natural language queries.
    In production, this would call a Databricks SQL endpoint or LLM API.
    """
    
    query_lower = query.lower()
    
    # Attendance-related questions
    if "attendance rate" in query_lower and "highest" in query_lower:
        academies = filtered_data['hierarchy'][['academy_id', 'academy_code']].drop_duplicates()
        if not academies.empty:
            top_academy = academies.sample(1)['academy_code'].iloc[0]
            return f"Based on your current filters, **{top_academy}** has the highest attendance rate at **94.3%**. They've had consistently strong attendance over the past 3 months with an average of 87 participants per session."
        else:
            return "No academy data available with the current filters."
    
    elif "attendance rate" in query_lower and ("lowest" in query_lower or "worst" in query_lower):
        return "The academy with the lowest attendance rate is **Riverside Community Center** at **67.8%**. This represents a 12% decline from last quarter. Factors may include scheduling conflicts during exam periods and competing sports programs."
    
    elif "haven't attended" in query_lower or "inactive" in query_lower:
        total = metrics['participants']['total_participants']
        inactive_count = int(total * 0.15)  # Mock 15% inactive
        return f"I found **{inactive_count} participants** who haven't attended any sessions in the last 30 days. This represents 15% of your total participant base. Would you like me to generate a list for follow-up outreach?"
    
    elif "average session duration" in query_lower:
        avg_duration = metrics['sessions']['avg_session_duration']
        return f"The average session duration across your selected filters is **{avg_duration:.0f} minutes**. This breaks down as:\n- Short sessions (< 30 min): 12%\n- Standard sessions (30-60 min): 68%\n- Extended sessions (> 60 min): 20%\n\nExtended sessions typically correlate with tournament preparation and skill certification workshops."
    
    # Skills-related questions
    elif "most skills" in query_lower or "skill achievement" in query_lower:
        total_achievements = metrics['skills']['total_achievements']
        return f"**Top 5 Participants by Skill Achievements:**\n1. Sarah Chen - 23 skills (Basketball Program)\n2. Marcus Johnson - 21 skills (Soccer Academy)\n3. Aisha Patel - 19 skills (Multi-Sport)\n4. David Kim - 18 skills (Tennis Program)\n5. Emma Rodriguez - 17 skills (Swimming)\n\nTotal achievements across all participants: **{total_achievements:,}**"
    
    elif "top 5 skills" in query_lower or "top skills" in query_lower:
        return "**Most Popular Skills This Month:**\n1. Ball Control Fundamentals - 342 achievements\n2. Team Communication - 298 achievements\n3. Defensive Positioning - 276 achievements\n4. Endurance Training - 245 achievements\n5. Strategic Thinking - 223 achievements\n\nThese skills align well with the current competitive season focus."
    
    # Operational questions
    elif "scheduled last week" in query_lower or "sessions last week" in query_lower:
        total_sessions = metrics['sessions']['total_sessions']
        mock_weekly = int(total_sessions * 0.08)  # Mock as ~8% of total
        return f"**Last Week's Summary:**\n- Sessions scheduled: **{mock_weekly}**\n- Sessions completed: **{int(mock_weekly * 0.96)}** (96% completion rate)\n- Average attendance per session: **23 participants**\n- Total contact hours: **{int(mock_weekly * 45)}** hours\n\nThis is 8% above the same period last month."
    
    elif "most active participants" in query_lower:
        active = metrics['participants']['active_participants']
        return f"Based on your selected filters, you have **{active} active participants** across your communities. The communities with the highest engagement are:\n1. Downtown Sports Hub - 342 active\n2. Westside Athletic Center - 298 active\n3. Northpoint Academy - 267 active\n\nActive is defined as attending at least 2 sessions in the past 14 days."
    
    elif "compare attendance" in query_lower and "product type" in query_lower:
        return "**Attendance Rates by Product Type:**\n- Elite Training Program: **91.2%** ⬆️ (+3%)\n- Recreational Leagues: **84.7%** ⬇️ (-1%)\n- Youth Development: **88.3%** ➡️ (stable)\n- Adult Fitness: **79.5%** ⬆️ (+2%)\n\nElite programs maintain higher attendance due to competitive season requirements and parent investment."
    
    # Trends & patterns
    elif "improving or declining" in query_lower or "trend" in query_lower:
        return "**Quarterly Attendance Trend Analysis:**\n\nAttendance is **improving** this quarter:\n- Q1 2025: 82.3% average\n- Q4 2024: 79.8% average\n- **Change: +2.5% (↑)**\n\nKey drivers:\n- New flexible scheduling options (+1.2%)\n- Enhanced communication with parents (+0.8%)\n- Improved facility access (+0.5%)\n\nProjection: If current trend continues, Q2 should reach ~84% attendance."
    
    elif "day of the week" in query_lower:
        return "**Attendance by Day of Week:**\n1. Wednesday: **89.2%** 🥇\n2. Tuesday: **87.5%**\n3. Thursday: **86.8%**\n4. Monday: **82.3%**\n5. Friday: **76.5%**\n6. Saturday: **71.2%**\n\nMid-week sessions perform best. Friday and Saturday see lower attendance due to family commitments and competing activities. Consider shifting underperforming programs to Tuesday-Thursday slots."
    
    elif "enrollment" in query_lower and "growth" in query_lower:
        total = metrics['participants']['total_participants']
        return f"**Enrollment Growth:**\n- Current total: **{total:,} participants**\n- Year-over-year growth: **+18.5%**\n- Month-over-month: **+3.2%**\n\n**Growth by Segment:**\n- Ages 6-10: +24% (highest growth)\n- Ages 11-14: +16%\n- Ages 15-18: +12%\n- Adult programs: +8%\n\nThe 6-10 age group is driving growth, suggesting successful early engagement programs."
    
    # Default response for unrecognized patterns
    else:
        return f"I understand you're asking: *\"{query}\"*\n\nBased on your current filters ({metrics['participants']['total_participants']} participants, {metrics['sessions']['total_sessions']} sessions), here's what I found:\n\n**Current Performance Snapshot:**\n- Attendance rate: **{metrics['attendance']['attendance_rate']:.1f}%**\n- Active participants: **{metrics['participants']['active_participants']}**\n- Total skill achievements: **{metrics['skills']['total_achievements']:,}**\n\nCould you rephrase your question or try one of the example questions above? I can help you analyze attendance, skills, operational metrics, and trends."


if __name__ == "__main__":
    main()
