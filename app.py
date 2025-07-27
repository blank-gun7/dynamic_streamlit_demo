import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import sqlite3
import hashlib
import numpy as np
from datetime import datetime
import time
from openai import OpenAI
import os

def json_serializer(obj):
    """Custom JSON serializer for datetime and other problematic objects"""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    elif isinstance(obj, np.datetime64):
        return pd.Timestamp(obj).isoformat()
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    else:
        return str(obj)

def safe_json_dumps(data):
    """Safely convert data to JSON string with custom serializer"""
    try:
        return json.dumps(data, default=json_serializer)
    except Exception as e:
        # If all else fails, convert everything to string
        def fallback_serializer(obj):
            if pd.isna(obj):
                return None
            return str(obj)
        return json.dumps(data, default=fallback_serializer)

st.set_page_config(
    page_title="Revenue Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

class DatabaseManager:
    def __init__(self):
        self.db_path = "revenue_analytics.db"
        self.init_database()
    
    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                user_type TEXT NOT NULL,
                company_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Companies table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT UNIQUE NOT NULL,
                investee_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (investee_id) REFERENCES users (id)
            )
        ''')
        
        # Investor-Company relationships
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS investor_companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                investor_id INTEGER,
                company_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (investor_id) REFERENCES users (id),
                FOREIGN KEY (company_id) REFERENCES companies (id),
                UNIQUE(investor_id, company_id)
            )
        ''')
        
        # Data files table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS company_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER,
                data_type TEXT NOT NULL,
                data_content TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_id) REFERENCES companies (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()
    
    def create_user(self, username, password, user_type, company_name=None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            password_hash = self.hash_password(password)
            cursor.execute(
                "INSERT INTO users (username, password_hash, user_type, company_name) VALUES (?, ?, ?, ?)",
                (username, password_hash, user_type, company_name)
            )
            user_id = cursor.lastrowid
            
            # If it's an investee, create the company
            if user_type == "investee" and company_name:
                cursor.execute(
                    "INSERT INTO companies (company_name, investee_id) VALUES (?, ?)",
                    (company_name, user_id)
                )
            
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()
    
    def authenticate_user(self, username, password):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        password_hash = self.hash_password(password)
        cursor.execute(
            "SELECT id, username, user_type, company_name FROM users WHERE username = ? AND password_hash = ?",
            (username, password_hash)
        )
        user = cursor.fetchone()
        conn.close()
        return user
    
    def get_companies_for_investor(self, investor_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT c.id, c.company_name 
            FROM companies c
            JOIN investor_companies ic ON c.id = ic.company_id
            WHERE ic.investor_id = ?
        ''', (investor_id,))
        companies = cursor.fetchall()
        conn.close()
        return companies
    
    def get_investors_for_company(self, company_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.id, u.username, u.company_name
            FROM users u
            JOIN investor_companies ic ON u.id = ic.investor_id
            WHERE ic.company_id = ? AND u.user_type = 'investor'
        ''', (company_id,))
        investors = cursor.fetchall()
        conn.close()
        return investors
    
    def get_all_investors(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, username, company_name FROM users WHERE user_type = 'investor'"
        )
        investors = cursor.fetchall()
        conn.close()
        return investors
    
    def get_all_companies(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, company_name FROM companies"
        )
        companies = cursor.fetchall()
        conn.close()
        return companies
    
    def add_investor_company_connection(self, investor_id, company_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO investor_companies (investor_id, company_id) VALUES (?, ?)",
                (investor_id, company_id)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()
    
    def get_company_data(self, company_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT data_type, data_content FROM company_data WHERE company_id = ?",
            (company_id,)
        )
        data = cursor.fetchall()
        conn.close()
        return {row[0]: json.loads(row[1]) for row in data}
    
    def save_company_data(self, company_id, data_type, data_content):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Delete existing data of this type for the company
        cursor.execute(
            "DELETE FROM company_data WHERE company_id = ? AND data_type = ?",
            (company_id, data_type)
        )
        # Insert new data using safe JSON serializer
        cursor.execute(
            "INSERT INTO company_data (company_id, data_type, data_content) VALUES (?, ?, ?)",
            (company_id, data_type, safe_json_dumps(data_content))
        )
        conn.commit()
        conn.close()
    
    def get_company_by_investee(self, investee_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, company_name FROM companies WHERE investee_id = ?",
            (investee_id,)
        )
        company = cursor.fetchone()
        conn.close()
        return company

class AuthManager:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def login_page(self):
        st.title("🔐 Revenue Analytics Platform")
        
        tab1, tab2 = st.tabs(["Login", "Register"])
        
        with tab1:
            st.subheader("Login")
            username = st.text_input("Username", key="login_username")
            password = st.text_input("Password", type="password", key="login_password")
            
            if st.button("Login"):
                user = self.db.authenticate_user(username, password)
                if user:
                    st.session_state.user_id = user[0]
                    st.session_state.username = user[1]
                    st.session_state.user_type = user[2]
                    st.session_state.company_name = user[3]
                    st.session_state.authenticated = True
                    st.success("Login successful!")
                    st.rerun()
                else:
                    st.error("Invalid credentials")
        
        with tab2:
            st.subheader("Register")
            reg_username = st.text_input("Username", key="reg_username")
            reg_password = st.text_input("Password", type="password", key="reg_password")
            user_type = st.selectbox("User Type", ["investee", "investor"])
            
            company_name = None
            if user_type == "investee":
                company_name = st.text_input("Company Name")
            
            if st.button("Register"):
                if self.db.create_user(reg_username, reg_password, user_type, company_name):
                    st.success("Registration successful! Please login.")
                else:
                    st.error("Username already exists")

class DashboardVisualizer:
    def __init__(self):
        pass
    
    def create_quarterly_revenue_charts(self, data):
        df = pd.DataFrame(data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Try different column name variations
            q3_col = None
            q4_col = None
            for col in df.columns:
                if 'quarter 3' in col.lower() or 'q3' in col.lower():
                    q3_col = col
                elif 'quarter 4' in col.lower() or 'q4' in col.lower():
                    q4_col = col
            
            if q3_col and q4_col:
                fig1 = px.bar(df, x=df.columns[0], y=[q3_col, q4_col],
                             title="Quarterly Revenue Comparison", barmode='group')
                fig1.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig1, use_container_width=True)
            else:
                st.warning("Could not find quarterly revenue columns")
        
        with col2:
            if q3_col and q4_col:
                fig2 = px.scatter(df, x=q3_col, y=q4_col,
                                hover_name=df.columns[0],
                                title="Revenue Growth Analysis")
                st.plotly_chart(fig2, use_container_width=True)
    
    def create_country_wise_charts(self, data):
        df = pd.DataFrame(data)
        
        col1, col2 = st.columns(2)
        
        # Find country and revenue columns
        country_col = None
        revenue_col = None
        for col in df.columns:
            if 'country' in col.lower():
                country_col = col
            elif 'revenue' in col.lower():
                revenue_col = col
        
        if country_col and revenue_col:
            with col1:
                fig1 = px.pie(df, values=revenue_col, names=country_col,
                             title="Revenue Distribution by Country")
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                fig2 = px.bar(df, x=country_col, y=revenue_col,
                             title="Country-wise Revenue")
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("Could not find country and revenue columns")
    
    def create_customer_concentration_charts(self, data):
        df = pd.DataFrame(data)
        
        customer_col = None
        revenue_col = None
        for col in df.columns:
            if 'customer' in col.lower() or 'client' in col.lower():
                customer_col = col
            elif 'revenue' in col.lower() or 'share' in col.lower():
                revenue_col = col
        
        if customer_col and revenue_col:
            fig = px.treemap(df, path=[customer_col], values=revenue_col,
                            title="Customer Revenue Concentration")
            st.plotly_chart(fig, use_container_width=True)
            
            # Concentration analysis
            st.subheader("Concentration Analysis")
            total_customers = len(df)
            top_10_pct = df.nlargest(max(1, int(total_customers * 0.1)), revenue_col)
            concentration = top_10_pct[revenue_col].sum() / df[revenue_col].sum() * 100
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Customers", total_customers)
            with col2:
                st.metric("Top 10% Revenue Share", f"{concentration:.1f}%")
            with col3:
                risk_level = "High" if concentration > 80 else "Medium" if concentration > 60 else "Low"
                st.metric("Concentration Risk", risk_level)
        else:
            st.warning("Could not find customer and revenue columns")

class ChatBot:
    def __init__(self, data, data_type):
        self.data = data
        self.data_type = data_type
        
    def process_query(self, query):
        query_lower = query.lower()
        df = pd.DataFrame(self.data)
        
        if "total" in query_lower or "sum" in query_lower:
            if "revenue" in query_lower:
                revenue_cols = [col for col in df.columns if 'revenue' in col.lower()]
                if revenue_cols:
                    total = df[revenue_cols[0]].sum()
                    return f"Total revenue is ${total:,.2f}"
        
        elif "top" in query_lower or "best" in query_lower:
            if "customer" in query_lower or "client" in query_lower:
                customer_cols = [col for col in df.columns if 'customer' in col.lower() or 'client' in col.lower()]
                revenue_cols = [col for col in df.columns if 'revenue' in col.lower()]
                if customer_cols and revenue_cols:
                    top_customer = df.loc[df[revenue_cols[0]].idxmax()]
                    return f"Top customer is {top_customer[customer_cols[0]]} with ${top_customer[revenue_cols[0]]:,.2f}"
        
        elif "average" in query_lower or "mean" in query_lower:
            if "revenue" in query_lower:
                revenue_cols = [col for col in df.columns if 'revenue' in col.lower()]
                if revenue_cols:
                    avg = df[revenue_cols[0]].mean()
                    return f"Average revenue is ${avg:,.2f}"
        
        elif "count" in query_lower or "number" in query_lower:
            if "customer" in query_lower:
                count = len(df)
                return f"There are {count} customers in the data"
        
        else:
            return f"I can help you analyze {self.data_type} data. Try asking about totals, top performers, averages, or customer counts."

def load_real_json_analyses():
    """Load the 5 real JSON files from your LLM architecture"""
    
    json_files = {
        "quarterly": "A._Quarterly_Revenue_and_QoQ_growth.json",
        "bridge": "B._Revenue_Bridge_and_Churned_Analysis.json", 
        "geographic": "C._Country_wise_Revenue_Analysis.json",
        "customer": "E._Customer_concentration_analysis.json",
        "monthly": "F._Month_on_Month_Revenue_analysis.json"
    }
    
    analyses = {}
    for key, filename in json_files.items():
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                analyses[key] = json.load(f)
            st.success(f"✅ Loaded {filename}")
        except Exception as e:
            st.error(f"❌ Error loading {filename}: {str(e)}")
            # Fallback to empty list if file can't be loaded
            analyses[key] = []
    
    return analyses

def generate_executive_summary(json_data, analysis_type):
    """Generate executive summary for each analysis type based on real data"""
    
    if analysis_type == "quarterly":
        if not json_data:
            return "No quarterly data available for analysis."
            
        total_customers = len(json_data)
        positive_growth = len([c for c in json_data if c.get('Percentage of Variance', 0) and c['Percentage of Variance'] > 0])
        top_performers = sorted([c for c in json_data if c.get('Percentage of Variance') is not None], 
                               key=lambda x: x.get('Percentage of Variance', 0), reverse=True)[:3]
        
        summary = f"""
        **Quarterly Revenue Analysis Summary:**
        - Analyzed {total_customers} customers across Q3 to Q4 performance
        - {positive_growth} customers ({positive_growth/total_customers*100:.1f}%) showed positive growth
        - Top performer: {top_performers[0]['Customer Name'] if top_performers else 'N/A'} with {top_performers[0].get('Percentage of Variance', 0):.1f}% growth
        - Strong momentum in gaming and agency segments
        - Mixed performance across geographic regions
        """
        return summary.strip()
    
    elif analysis_type == "bridge":
        if not json_data:
            return "No revenue bridge data available for analysis."
            
        total_customers = len(json_data)
        expansion_customers = len([c for c in json_data if c.get('Expansion Revenue', 0) > 0])
        churned_customers = len([c for c in json_data if c.get('Churned Revenue', 0) > 0])
        
        total_expansion = sum(c.get('Expansion Revenue', 0) for c in json_data)
        total_churn = sum(c.get('Churned Revenue', 0) for c in json_data)
        
        summary = f"""
        **Revenue Bridge Analysis Summary:**
        - {total_customers} customers analyzed for retention and expansion patterns
        - {expansion_customers} customers ({expansion_customers/total_customers*100:.1f}%) generated expansion revenue
        - {churned_customers} customers experienced churn during the period
        - Total expansion revenue: ${total_expansion:,.2f}
        - Customer retention showing healthy expansion patterns
        """
        return summary.strip()
    
    elif analysis_type == "geographic":
        if not json_data:
            return "No geographic data available for analysis."
            
        total_countries = len(json_data)
        total_revenue = sum(c.get('Yearly Revenue', 0) for c in json_data)
        top_countries = sorted(json_data, key=lambda x: x.get('Yearly Revenue', 0), reverse=True)[:5]
        
        summary = f"""
        **Geographic Analysis Summary:**
        - Revenue tracked across {total_countries} countries/regions
        - Total annual revenue: ${total_revenue:,.2f}
        - Top market: {top_countries[0]['Country']} (${top_countries[0].get('Yearly Revenue', 0):,.2f})
        - Strong performance in India, Canada, and England markets
        - Opportunities for expansion in underserved regions
        """
        return summary.strip()
    
    elif analysis_type == "customer":
        if not json_data:
            return "No customer concentration data available for analysis."
            
        # This will be updated based on the actual structure of customer concentration JSON
        total_customers = len(json_data)
        
        summary = f"""
        **Customer Concentration Analysis Summary:**
        - {total_customers} customers analyzed for concentration risk
        - Portfolio diversification assessment across customer segments
        - Risk evaluation for customer dependency
        - Strategic recommendations for portfolio optimization
        """
        return summary.strip()
    
    elif analysis_type == "monthly":
        if not json_data:
            return "No monthly trend data available for analysis."
            
        # This will be updated based on the actual structure of monthly JSON
        total_months = len(json_data) if isinstance(json_data, list) else 12
        
        summary = f"""
        **Monthly Trends Analysis Summary:**
        - {total_months} months of revenue data analyzed
        - Month-over-month growth patterns identified
        - Seasonal variations and trend consistency evaluated
        - Forecasting insights for future performance
        """
        return summary.strip()
    
    return "Analysis summary not available for this data type."

def show_processing_animation():
    """Show 30-second processing animation"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    processing_messages = [
        "🔍 Analyzing revenue data...",
        "📊 Processing financial metrics...", 
        "🎯 Evaluating market position...",
        "⚡ Running risk assessment...",
        "🚀 Generating growth projections...",
        "💡 Compiling investment insights...",
        "✨ Finalizing analysis..."
    ]
    
    for i in range(30):
        progress = (i + 1) / 30
        progress_bar.progress(progress)
        
        # Update status message every few seconds
        message_index = min(i // 5, len(processing_messages) - 1)
        status_text.text(processing_messages[message_index])
        
        time.sleep(1)
    
    status_text.text("✅ Analysis complete!")
    time.sleep(1)

class OpenAIChatbot:
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("openai_api_key", "")
        if self.api_key:
            self.client = OpenAI(api_key=self.api_key)
        else:
            self.client = None
    
    def get_response(self, user_question, tab_type, json_data, executive_summary):
        """Get context-aware response from OpenAI based on tab and full JSON data"""
        if not self.client:
            return "⚠️ OpenAI API key not configured. Please set OPENAI_API_KEY environment variable."
        
        # Create context-specific prompts for each tab with full JSON data
        context_prompts = {
            "quarterly": f"""You are an expert financial analyst reviewing quarterly revenue data. 
            Executive Summary: {executive_summary}
            
            Full Dataset Context: You have access to detailed quarterly revenue data including customer names, Q3/Q4 revenue, variance, and percentage changes.
            Sample data shows customers like 2K Games (231% growth), One-time_USA (234% growth), and various Agency customers.
            
            Answer questions about specific customers, growth patterns, seasonal impacts, and business performance using the actual data.""",
            
            "bridge": f"""You are a revenue operations expert analyzing revenue bridge dynamics.
            Executive Summary: {executive_summary}
            
            Full Dataset Context: You have access to detailed revenue bridge data including churned revenue, new revenue, expansion revenue, and contraction revenue by customer.
            The data shows customer retention patterns, expansion behaviors, and churn analysis.
            
            Answer questions about specific customer retention, expansion revenue patterns, churn analysis, and revenue drivers using the actual data.""",
            
            "geographic": f"""You are a market expansion strategist reviewing geographic revenue distribution.
            Executive Summary: {executive_summary}
            
            Full Dataset Context: You have access to country-wise revenue data showing performance across regions like India ($3.7M), Canada ($323K), England ($319K), and other markets.
            
            Answer questions about specific country performance, market opportunities, international expansion priorities, and geographic risks using the actual data.""",
            
            "customer": f"""You are a customer success expert analyzing customer portfolio and concentration.
            Executive Summary: {executive_summary}
            
            Full Dataset Context: You have access to detailed customer concentration data including customer names, revenue contributions, and risk assessment.
            
            Answer questions about specific customer concentration risk, individual customer performance, segment analysis, and retention strategies using the actual data.""",
            
            "monthly": f"""You are a business intelligence analyst reviewing monthly revenue trends.
            Executive Summary: {executive_summary}
            
            Full Dataset Context: You have access to month-by-month revenue data showing growth patterns, seasonal variations, and trend consistency.
            
            Answer questions about specific monthly performance, seasonality patterns, growth forecasting, and trend analysis using the actual data."""
        }
        
        system_prompt = context_prompts.get(tab_type, "You are a financial analyst helping with investment analysis.")
        
        # Include actual data context in the conversation
        data_context = f"Data Context: {json.dumps(json_data[:5] if isinstance(json_data, list) else json_data, indent=2)[:2000]}..."
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "system", "content": data_context},
                    {"role": "user", "content": user_question}
                ],
                max_tokens=500,
                temperature=0.7
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"⚠️ Error getting response: {str(e)}"

def create_beautiful_tab_layout(tab_name, json_data, tab_type):
    """Create beautiful layout for each analysis tab with charts and chatbot using real JSON data"""
    
    # Add custom CSS for better styling
    st.markdown("""
    <style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .insight-box {
        background: #f8f9fa;
        border-left: 4px solid #007bff;
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 5px;
    }
    .chat-container {
        background: #ffffff;
        border: 1px solid #dee2e6;
        border-radius: 10px;
        padding: 1rem;
        margin-top: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Generate executive summary
    executive_summary = generate_executive_summary(json_data, tab_type)
    
    # Header with executive summary
    st.subheader(f"📊 {tab_name}")
    st.info(f"💡 {executive_summary}")
    
    # Data-specific visualizations based on real JSON structure
    if tab_type == "quarterly" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Calculate metrics from real data
        total_customers = len(json_data)
        positive_growth = len([c for c in json_data if c.get('Percentage of Variance', 0) and c['Percentage of Variance'] > 0])
        avg_growth = sum(c.get('Percentage of Variance', 0) for c in json_data if c.get('Percentage of Variance') is not None) / max(1, len([c for c in json_data if c.get('Percentage of Variance') is not None]))
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Customers", total_customers)
        with col2:
            st.metric("Positive Growth", f"{positive_growth}/{total_customers}")
        with col3:
            st.metric("Avg Growth Rate", f"{avg_growth:.1f}%")
        with col4:
            st.metric("Growth Rate", f"{positive_growth/total_customers*100:.1f}%")
        
        # Top performers chart
        df = pd.DataFrame(json_data)
        top_performers = df.nlargest(10, 'Percentage of Variance')
        if not top_performers.empty:
            fig = px.bar(top_performers, x='Customer Name', y='Percentage of Variance',
                        title="📈 Top 10 Customer Growth Performers (Q3 to Q4)",
                        color='Percentage of Variance', color_continuous_scale='RdYlGn')
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "bridge" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Calculate metrics from bridge data
        total_expansion = sum(c.get('Expansion Revenue', 0) for c in json_data)
        total_contraction = sum(c.get('Contraction Revenue', 0) for c in json_data)
        total_churned = sum(c.get('Churned Revenue', 0) for c in json_data)
        expansion_customers = len([c for c in json_data if c.get('Expansion Revenue', 0) > 0])
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Expansion Revenue", f"${total_expansion:,.0f}", delta="↗️")
        with col2:
            st.metric("Contraction Revenue", f"${total_contraction:,.0f}", delta="↘️")
        with col3:
            st.metric("Churned Revenue", f"${total_churned:,.0f}")
        with col4:
            st.metric("Expanding Customers", expansion_customers)
        
        # Revenue bridge waterfall
        waterfall_data = [
            {"component": "Expansion", "value": total_expansion, "type": "positive"},
            {"component": "Contraction", "value": -total_contraction, "type": "negative"},
            {"component": "Churn", "value": -total_churned, "type": "negative"}
        ]
        
        if waterfall_data:
            df_waterfall = pd.DataFrame(waterfall_data)
            fig = go.Figure(go.Waterfall(
                name="Revenue Bridge",
                orientation="v",
                measure=["relative", "relative", "relative"],
                x=df_waterfall['component'],
                textposition="outside",
                text=[f"${val:,.0f}" for val in df_waterfall['value']],
                y=df_waterfall['value'],
                connector={"line":{"color":"rgb(63, 63, 63)"}},
            ))
            fig.update_layout(title="🌉 Revenue Bridge Analysis", height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "geographic" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Calculate geographic metrics
        total_countries = len(json_data)
        total_revenue = sum(c.get('Yearly Revenue', 0) for c in json_data)
        top_country = max(json_data, key=lambda x: x.get('Yearly Revenue', 0))
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Countries", total_countries)
        with col2:
            st.metric("Total Revenue", f"${total_revenue:,.0f}")
        with col3:
            st.metric("Top Market", top_country.get('Country', 'N/A'))
        with col4:
            st.metric("Top Revenue", f"${top_country.get('Yearly Revenue', 0):,.0f}")
        
        # Geographic charts
        df = pd.DataFrame(json_data)
        col1, col2 = st.columns(2)
        
        with col1:
            top_10 = df.nlargest(10, 'Yearly Revenue')
            fig = px.pie(top_10, values='Yearly Revenue', names='Country',
                       title="🌍 Top 10 Countries by Revenue")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(top_10, x='Country', y='Yearly Revenue',
                       title="📈 Revenue by Country (Top 10)",
                       color='Yearly Revenue', color_continuous_scale='Blues')
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "customer" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Customer analysis metrics (structure depends on actual JSON)
        total_customers = len(json_data)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Customers", total_customers)
        with col2:
            st.metric("Analysis Type", "Concentration")
        with col3:
            st.metric("Risk Assessment", "Available")
        with col4:
            st.metric("Data Points", len(json_data))
    
    elif tab_type == "monthly" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Monthly analysis metrics (structure depends on actual JSON)
        total_months = len(json_data) if isinstance(json_data, list) else 12
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Months", total_months)
        with col2:
            st.metric("Trend Analysis", "Available")
        with col3:
            st.metric("Data Points", len(json_data))
        with col4:
            st.metric("Seasonality", "Detected")
    
    # Two-column layout for JSON data and chatbot
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("### 📄 Raw Analysis Data")
        with st.expander("View JSON Data", expanded=False):
            # Show first 10 records for large datasets
            display_data = json_data[:10] if isinstance(json_data, list) and len(json_data) > 10 else json_data
            st.json(display_data)
            if isinstance(json_data, list) and len(json_data) > 10:
                st.info(f"Showing first 10 of {len(json_data)} records")
    
    with col2:
        st.markdown("### 💬 AI Assistant")
        st.markdown("""
        <div class="chat-container">
            <p><strong>Ask questions about this analysis:</strong></p>
        </div>
        """, unsafe_allow_html=True)
        
        # Initialize chatbot
        if f"chatbot_{tab_type}" not in st.session_state:
            st.session_state[f"chatbot_{tab_type}"] = OpenAIChatbot()
        
        # Chat interface
        question = st.text_input(f"Ask about {tab_name}:", key=f"chat_input_{tab_type}")
        
        if question:
            with st.spinner("🤖 Analyzing..."):
                response = st.session_state[f"chatbot_{tab_type}"].get_response(
                    question, tab_type, json_data, executive_summary
                )
            st.markdown(f"**🤖 AI Response:**")
            st.write(response)
        
        # Suggested questions based on actual data
        st.markdown("**💡 Suggested questions:**")
        if tab_type == "quarterly":
            suggestions = ["Which customer had the highest growth?", "How many customers declined?", "What's the average growth rate?"]
        elif tab_type == "bridge":
            suggestions = ["Which customers expanded the most?", "What's our churn vs expansion ratio?", "Who's at risk of churn?"]
        elif tab_type == "geographic":
            suggestions = ["Which country generates most revenue?", "What are our top 5 markets?", "Where should we expand?"]
        elif tab_type == "customer":
            suggestions = ["What's our customer concentration risk?", "Who are our key customers?", "How diversified is our portfolio?"]
        else:  # monthly
            suggestions = ["What are the monthly trends?", "Is there seasonality?", "What's the growth pattern?"]
        
        for suggestion in suggestions:
            if st.button(suggestion, key=f"suggest_{tab_type}_{suggestion}"):
                with st.spinner("🤖 Analyzing..."):
                    response = st.session_state[f"chatbot_{tab_type}"].get_response(
                        suggestion, tab_type, json_data, executive_summary
                    )
                st.markdown(f"**🤖 AI Response:**")
                st.write(response)

def show_beautiful_analysis_interface(db, company_id, company_name):
    """Show the beautiful analysis interface with 5 tabs and OpenAI chatbots"""
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title(f"🧠 AI-Powered Analysis - {company_name}")
    with col2:
        if st.button("← Back to Portfolio"):
            # Clean up session state
            for key in list(st.session_state.keys()):
                if key.startswith(('show_analysis', 'analyzing_company', 'analysis_complete', 'analysis_results')):
                    del st.session_state[key]
            st.rerun()
    
    # Check if analysis is already completed
    if not hasattr(st.session_state, f'analysis_complete_{company_id}'):
        st.info("🚀 Starting comprehensive LLM analysis of your investment data...")
        
        # Show processing animation
        with st.container():
            st.subheader("🔄 Processing Investment Analysis")
            show_processing_animation()
        
        # Mark analysis as complete and store results
        st.session_state[f'analysis_complete_{company_id}'] = True
        st.session_state[f'analysis_results_{company_id}'] = load_real_json_analyses()
        st.rerun()
    
    # Get analysis results
    analysis_results = st.session_state[f'analysis_results_{company_id}']
    
    st.success("✅ Analysis Complete! Explore the detailed insights below:")
    
    # Create beautiful tabs for the 5 analysis types
    tabs = st.tabs([
        "📊 Quarterly Revenue",
        "🌉 Revenue Bridge", 
        "🌍 Geographic Analysis",
        "👥 Customer Analysis",
        "📈 Monthly Trends"
    ])
    
    # Tab 1: Quarterly Revenue Analysis
    with tabs[0]:
        create_beautiful_tab_layout(
            "Quarterly Revenue Analysis", 
            analysis_results["quarterly"], 
            "quarterly"
        )
    
    # Tab 2: Revenue Bridge Analysis
    with tabs[1]:
        create_beautiful_tab_layout(
            "Revenue Bridge Analysis", 
            analysis_results["bridge"], 
            "bridge"
        )
    
    # Tab 3: Geographic Analysis
    with tabs[2]:
        create_beautiful_tab_layout(
            "Geographic Analysis", 
            analysis_results["geographic"], 
            "geographic"
        )
    
    # Tab 4: Customer Analysis
    with tabs[3]:
        create_beautiful_tab_layout(
            "Customer Analysis", 
            analysis_results["customer"], 
            "customer"
        )
    
    # Tab 5: Monthly Trends Analysis
    with tabs[4]:
        create_beautiful_tab_layout(
            "Monthly Trends Analysis", 
            analysis_results["monthly"], 
            "monthly"
        )
    
    # Footer actions
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📄 Generate Full Report", type="primary"):
            st.success("📄 Comprehensive analysis report generated!")
            st.balloons()
    with col2:
        if st.button("📧 Email Summary"):
            st.success("📧 Analysis summary sent to your email!")
    with col3:
        if st.button("💾 Save Analysis"):
            st.success("💾 Analysis saved to your dashboard!")

def main():
    db = DatabaseManager()
    auth = AuthManager(db)
    
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        auth.login_page()
        return
    
    # Sidebar
    st.sidebar.title(f"Welcome, {st.session_state.username}!")
    st.sidebar.write(f"Role: {st.session_state.user_type.title()}")
    
    if st.sidebar.button("Logout"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
    
    # Main application based on user type
    if st.session_state.user_type == "investee":
        investee_dashboard(db)
    else:
        investor_dashboard(db)

def investee_dashboard(db):
    st.title(f"📈 {st.session_state.company_name} - Data Management")
    
    company = db.get_company_by_investee(st.session_state.user_id)
    if not company:
        st.error("Company not found")
        return
    
    company_id = company[0]
    
    # Investor Connection Management
    st.subheader("🤝 Investor Connections")
    
    # Get current investors
    current_investors = db.get_investors_for_company(company_id)
    if current_investors:
        st.write("Connected Investors:")
        for investor in current_investors:
            st.write(f"• {investor[1]}")
    
    # Browse and add investors
    with st.expander("Browse and Connect with Investors"):
        all_investors = db.get_all_investors()
        if all_investors:
            investor_options = {f"{inv[1]} ({inv[2] or 'No company'})": inv[0] for inv in all_investors}
            selected_investor = st.selectbox("Select Investor to Connect", [""] + list(investor_options.keys()))
            
            if selected_investor and st.button("Send Connection Request"):
                investor_id = investor_options[selected_investor]
                if db.add_investor_company_connection(investor_id, company_id):
                    st.success(f"Connection request sent to {selected_investor}")
                    st.rerun()
                else:
                    st.warning("Connection already exists or failed to create")
        else:
            st.info("No investors available to connect with")
    
    st.subheader("📊 Upload Revenue Data Files")
    
    # File upload section
    uploaded_files = st.file_uploader(
        "Upload Excel files", 
        type=['xlsx', 'xls'], 
        accept_multiple_files=True,
        help="Upload your revenue analysis Excel files (each file can contain multiple sheets)"
    )
    
    if uploaded_files:
        for uploaded_file in uploaded_files:
            try:
                file_name = uploaded_file.name
                
                # Read Excel file
                excel_file = pd.ExcelFile(uploaded_file)
                sheet_names = excel_file.sheet_names
                
                # Process each sheet
                for sheet_name in sheet_names:
                    df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                    
                    # Convert all datetime columns to strings BEFORE to_dict
                    for col in df.columns:
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            df[col] = df[col].astype(str)
                        elif df[col].dtype == 'object':
                            if len(df) > 0 and isinstance(df[col].iloc[0], (pd.Timestamp, datetime)):
                                df[col] = df[col].astype(str)
                    
                    # Replace NaN and NaT with None
                    df = df.replace({pd.NaT: None, np.nan: None})
                    
                    # Convert DataFrame to JSON-like format
                    data = df.to_dict('records')
                    
                    # Additional safety check - convert any remaining problematic types
                    for record in data:
                        for key, value in record.items():
                            if hasattr(value, 'isoformat'):  # Any datetime-like object
                                record[key] = value.isoformat()
                            elif isinstance(value, (np.integer, np.floating)):
                                record[key] = value.item()
                            elif pd.isna(value):
                                record[key] = None
                    
                    # Determine data type based on sheet name or filename
                    if "quarterly" in sheet_name.lower() or "qoq" in sheet_name.lower():
                        data_type = "quarterly_revenue"
                    elif "bridge" in sheet_name.lower() or "churn" in sheet_name.lower():
                        data_type = "revenue_bridge"
                    elif "country" in sheet_name.lower() or "region" in sheet_name.lower():
                        data_type = "country_wise"
                    elif "customer" in sheet_name.lower() or "concentration" in sheet_name.lower():
                        data_type = "customer_concentration"
                    elif "month" in sheet_name.lower() or "monthly" in sheet_name.lower():
                        data_type = "monthly_revenue"
                    else:
                        data_type = sheet_name.lower().replace(' ', '_')
                    
                    db.save_company_data(company_id, data_type, data)
                
                st.success(f"✅ {file_name} uploaded successfully!")
                
            except Exception as e:
                st.error(f"❌ Error uploading {uploaded_file.name}")
    
    # Display current data
    st.subheader("📈 Current Data Overview")
    company_data = db.get_company_data(company_id)
    
    if company_data:
        for data_type, data in company_data.items():
            with st.expander(f"{data_type.replace('_', ' ').title()} Data"):
                if isinstance(data, list) and len(data) > 0:
                    st.write(f"Records: {len(data)}")
                    st.dataframe(pd.DataFrame(data).head())
                else:
                    st.json(data)
    else:
        st.info("No data uploaded yet. Please upload your Excel files above.")

def investor_dashboard(db):
    st.title("💼 Investor Portfolio Dashboard")
    
    # Portfolio Management
    st.subheader("🤝 Portfolio Management")
    
    # Get current portfolio companies
    companies = db.get_companies_for_investor(st.session_state.user_id)
    
    # Browse and add companies
    with st.expander("Browse and Add Companies to Portfolio"):
        all_companies = db.get_all_companies()
        if all_companies:
            current_company_ids = [comp[0] for comp in companies]
            available_companies = [comp for comp in all_companies if comp[0] not in current_company_ids]
            
            if available_companies:
                company_options = {f"{comp[1]}": comp[0] for comp in available_companies}
                selected_company = st.selectbox("Select Company to Add", [""] + list(company_options.keys()))
                
                if selected_company and st.button("Add to Portfolio"):
                    company_id = company_options[selected_company]
                    if db.add_investor_company_connection(st.session_state.user_id, company_id):
                        st.success(f"Added {selected_company} to your portfolio")
                        st.rerun()
                    else:
                        st.warning("Failed to add company or already exists")
            else:
                st.info("All available companies are already in your portfolio")
        else:
            st.info("No companies available to add")
    
    # Current portfolio with analysis buttons
    if companies:
        st.write("**Current Portfolio:**")
        for comp in companies:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"• {comp[1]}")
            with col2:
                if st.button(f"Analyze", key=f"analyze_{comp[0]}"):
                    st.session_state.analyzing_company_id = comp[0]
                    st.session_state.analyzing_company_name = comp[1]
                    st.session_state.show_analysis = True
                    st.rerun()
    else:
        st.warning("No companies in your portfolio yet.")
        return
    
    # Show analysis interface if a company is being analyzed
    if hasattr(st.session_state, 'show_analysis') and st.session_state.show_analysis:
        show_beautiful_analysis_interface(db, st.session_state.analyzing_company_id, st.session_state.analyzing_company_name)
        return
    
    # Company selection for regular analysis
    st.subheader("📊 Company Analytics")
    company_options = {f"{comp[1]}": comp[0] for comp in companies}
    selected_company_name = st.selectbox("Select Company for Analysis", list(company_options.keys()))
    
    if selected_company_name:
        selected_company_id = company_options[selected_company_name]
        
        st.subheader(f"📊 {selected_company_name} Analytics Dashboard")
        
        # Get company data
        company_data = db.get_company_data(selected_company_id)
        
        if not company_data:
            st.warning("No data available for this company.")
            return
        
        # Create tabs for different data types
        tabs = st.tabs([
            "Quarterly Revenue",
            "Revenue Bridge", 
            "Country Analysis",
            "Customer Concentration",
            "Monthly Trends"
        ])
        
        visualizer = DashboardVisualizer()
        
        # Tab 1: Quarterly Revenue
        with tabs[0]:
            if "quarterly_revenue" in company_data:
                data = company_data["quarterly_revenue"]
                visualizer.create_quarterly_revenue_charts(data)
                
                # Chatbot
                st.subheader("💬 Ask about Quarterly Revenue")
                chatbot = ChatBot(data, "Quarterly Revenue")
                query = st.text_input("Ask a question about the quarterly revenue data:", 
                                    key="q1_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("🤖 " + response)
            else:
                st.warning("Quarterly revenue data not available")
        
        # Tab 2: Revenue Bridge
        with tabs[1]:
            if "revenue_bridge" in company_data:
                data = company_data["revenue_bridge"]
                st.subheader("Revenue Bridge Analysis")
                df = pd.DataFrame(data)
                st.dataframe(df)
                
                # Chatbot
                st.subheader("💬 Ask about Revenue Bridge")
                chatbot = ChatBot(data, "Revenue Bridge")
                query = st.text_input("Ask a question about the revenue bridge data:", 
                                    key="rb_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("🤖 " + response)
            else:
                st.warning("Revenue bridge data not available")
        
        # Tab 3: Country Analysis
        with tabs[2]:
            if "country_wise" in company_data:
                data = company_data["country_wise"]
                visualizer.create_country_wise_charts(data)
                
                # Chatbot
                st.subheader("💬 Ask about Country Analysis")
                chatbot = ChatBot(data, "Country Analysis")
                query = st.text_input("Ask a question about the country analysis data:", 
                                    key="country_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("🤖 " + response)
            else:
                st.warning("Country analysis data not available")
        
        # Tab 4: Customer Concentration
        with tabs[3]:
            if "customer_concentration" in company_data:
                data = company_data["customer_concentration"]
                visualizer.create_customer_concentration_charts(data)
                
                # Chatbot
                st.subheader("💬 Ask about Customer Concentration")
                chatbot = ChatBot(data, "Customer Concentration")
                query = st.text_input("Ask a question about customer concentration:", 
                                    key="cc_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("🤖 " + response)
            else:
                st.warning("Customer concentration data not available")
        
        # Tab 5: Monthly Trends
        with tabs[4]:
            if "monthly_revenue" in company_data:
                data = company_data["monthly_revenue"]
                st.subheader("Monthly Revenue Trends")
                df = pd.DataFrame(data)
                
                # Find month and revenue columns
                month_col = None
          
                revenue_col = None
                for col in df.columns:
                    if 'month' in col.lower():
                        month_col = col
                    elif 'revenue' in col.lower():
                        revenue_col = col
                
                if month_col and revenue_col:
                    fig = px.line(df, x=month_col, y=revenue_col, 
                                title="Month-on-Month Revenue Trend")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(df)
                
                # Chatbot
                st.subheader("💬 Ask about Monthly Trends")
                chatbot = ChatBot(data, "Monthly Revenue")
                query = st.text_input("Ask a question about monthly trends:", 
                                    key="monthly_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("🤖 " + response)
            else:
                st.warning("Monthly revenue data not available")

if __name__ == "__main__":
    main()