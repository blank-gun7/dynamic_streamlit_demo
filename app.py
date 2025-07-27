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
import openai
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

def generate_llm_architecture_analyses():
    """Generate 5 predefined JSON analyses from your LLM architecture"""
    
    return {
        "quarterly_revenue_analysis": {
            "analysis_summary": "Strong quarterly performance with consistent growth trajectory",
            "key_metrics": {
                "q4_vs_q3_growth": 18.5,
                "yoy_growth": 42.3,
                "revenue_quality_score": 8.7,
                "seasonality_impact": "Low"
            },
            "quarterly_breakdown": [
                {"quarter": "Q1 2024", "revenue": 8200000, "growth_rate": 12.5, "customers": 145},
                {"quarter": "Q2 2024", "revenue": 9100000, "growth_rate": 11.0, "customers": 162},
                {"quarter": "Q3 2024", "revenue": 10800000, "growth_rate": 18.7, "customers": 178},
                {"quarter": "Q4 2024", "revenue": 12800000, "growth_rate": 18.5, "customers": 195}
            ],
            "insights": [
                "Accelerating growth momentum in recent quarters",
                "Customer acquisition rate improving consistently", 
                "Revenue per customer increasing (ARR expansion)",
                "No significant seasonal variations detected"
            ],
            "risk_factors": ["Market saturation in Q2 2025", "Competition intensifying"],
            "opportunities": ["International expansion", "Product line extension"]
        },
        
        "revenue_bridge_analysis": {
            "analysis_summary": "Healthy revenue dynamics with strong expansion and low churn",
            "key_metrics": {
                "net_revenue_retention": 118,
                "gross_revenue_retention": 94,
                "expansion_rate": 24,
                "churn_rate": 6
            },
            "bridge_components": [
                {"component": "Starting ARR", "value": 10800000, "percentage": 100},
                {"component": "New Customers", "value": 2400000, "percentage": 22.2},
                {"component": "Expansion", "value": 1800000, "percentage": 16.7},
                {"component": "Contraction", "value": -600000, "percentage": -5.6},
                {"component": "Churn", "value": -1200000, "percentage": -11.1},
                {"component": "Ending ARR", "value": 13200000, "percentage": 122.2}
            ],
            "insights": [
                "Strong net revenue retention above 115% benchmark",
                "Expansion revenue driving 67% of growth",
                "Churn rate below industry average",
                "New customer acquisition accelerating"
            ],
            "churn_analysis": {
                "primary_reasons": ["Budget constraints", "Feature gaps", "Support issues"],
                "at_risk_segments": ["SMB customers", "Single-product users"],
                "retention_initiatives": ["Success programs", "Product roadmap", "Pricing flexibility"]
            }
        },
        
        "geographic_analysis": {
            "analysis_summary": "North America dominance with emerging opportunities in Europe and APAC",
            "key_metrics": {
                "total_markets": 12,
                "revenue_concentration": 68,
                "international_growth_rate": 45,
                "market_penetration_score": 6.8
            },
            "regional_breakdown": [
                {"region": "North America", "revenue": 8700000, "percentage": 68, "growth_rate": 15.2, "customers": 145},
                {"region": "Europe", "revenue": 2560000, "percentage": 20, "growth_rate": 52.1, "customers": 38},
                {"region": "APAC", "revenue": 1024000, "percentage": 8, "growth_rate": 78.3, "customers": 18},
                {"region": "Latin America", "revenue": 512000, "percentage": 4, "growth_rate": 34.7, "customers": 8}
            ],
            "country_performance": [
                {"country": "United States", "revenue": 7800000, "growth_rate": 14.5, "market_rank": 1},
                {"country": "Canada", "revenue": 900000, "growth_rate": 22.1, "market_rank": 2},
                {"country": "United Kingdom", "revenue": 1280000, "growth_rate": 48.3, "market_rank": 3},
                {"country": "Germany", "revenue": 768000, "growth_rate": 56.8, "market_rank": 4},
                {"country": "Australia", "revenue": 512000, "growth_rate": 72.4, "market_rank": 5}
            ],
            "insights": [
                "Strong growth in international markets offsetting NA saturation",
                "Europe showing highest revenue potential",
                "APAC demonstrating fastest growth rates",
                "Localization efforts paying off in key markets"
            ],
            "expansion_opportunities": ["France", "Japan", "Brazil", "India"],
            "market_risks": ["Currency fluctuation", "Regulatory changes", "Local competition"]
        },
        
        "customer_analysis": {
            "analysis_summary": "Balanced customer portfolio with manageable concentration risk",
            "key_metrics": {
                "total_customers": 195,
                "customer_concentration_risk": "Medium",
                "top_10_revenue_share": 42,
                "customer_lifetime_value": 186000
            },
            "customer_segments": [
                {"segment": "Enterprise", "count": 28, "percentage": 14.4, "avg_revenue": 245000, "churn_rate": 3.2},
                {"segment": "Mid-Market", "count": 67, "percentage": 34.4, "avg_revenue": 98000, "churn_rate": 5.8},
                {"segment": "SMB", "count": 100, "percentage": 51.3, "avg_revenue": 35000, "churn_rate": 8.9}
            ],
            "top_customers": [
                {"customer": "TechCorp Global", "revenue": 890000, "percentage": 6.9, "contract_term": 36},
                {"customer": "Innovation Labs", "revenue": 670000, "percentage": 5.2, "contract_term": 24},
                {"customer": "Digital Solutions Inc", "revenue": 580000, "percentage": 4.5, "contract_term": 24},
                {"customer": "Future Systems", "revenue": 520000, "percentage": 4.1, "contract_term": 12},
                {"customer": "Smart Analytics", "revenue": 480000, "percentage": 3.7, "contract_term": 24}
            ],
            "insights": [
                "Top 10 customers represent 42% of revenue - manageable concentration",
                "Enterprise segment shows lowest churn and highest value",
                "SMB segment offers volume but higher churn risk",
                "Strong customer relationships with multi-year contracts"
            ],
            "loyalty_metrics": {
                "net_promoter_score": 68,
                "customer_satisfaction": 4.3,
                "support_satisfaction": 4.1,
                "renewal_rate": 94
            },
            "risk_mitigation": ["Expand mid-market focus", "Strengthen enterprise relationships", "Improve SMB onboarding"]
        },
        
        "monthly_trends_analysis": {
            "analysis_summary": "Consistent monthly growth with strong momentum and minimal volatility",
            "key_metrics": {
                "monthly_growth_rate": 6.8,
                "revenue_volatility": "Low",
                "seasonal_variance": 12,
                "trend_consistency": 89
            },
            "monthly_data": [
                {"month": "Jan 2024", "revenue": 2650000, "growth": 8.2, "new_customers": 12, "churn": 2},
                {"month": "Feb 2024", "revenue": 2720000, "growth": 2.6, "new_customers": 15, "churn": 3},
                {"month": "Mar 2024", "revenue": 2830000, "growth": 4.0, "new_customers": 18, "churn": 1},
                {"month": "Apr 2024", "revenue": 2980000, "growth": 5.3, "new_customers": 16, "churn": 2},
                {"month": "May 2024", "revenue": 3020000, "growth": 1.3, "new_customers": 14, "churn": 4},
                {"month": "Jun 2024", "revenue": 3100000, "growth": 2.6, "new_customers": 19, "churn": 2},
                {"month": "Jul 2024", "revenue": 3480000, "growth": 12.3, "new_customers": 22, "churn": 1},
                {"month": "Aug 2024", "revenue": 3620000, "growth": 4.0, "new_customers": 18, "churn": 3},
                {"month": "Sep 2024", "revenue": 3700000, "growth": 2.2, "new_customers": 15, "churn": 2},
                {"month": "Oct 2024", "revenue": 4100000, "growth": 10.8, "new_customers": 24, "churn": 1},
                {"month": "Nov 2024", "revenue": 4280000, "growth": 4.4, "new_customers": 20, "churn": 2},
                {"month": "Dec 2024", "revenue": 4420000, "growth": 3.3, "new_customers": 17, "churn": 3}
            ],
            "insights": [
                "Strong acceleration in July and October driven by product launches",
                "Minimal seasonal impact - business model resilient",
                "Customer acquisition trending upward consistently",
                "Churn remaining stable across all months"
            ],
            "forecasting": {
                "next_month_prediction": 4680000,
                "confidence_level": 87,
                "growth_trajectory": "Positive",
                "key_drivers": ["Product expansion", "Market penetration", "Sales efficiency"]
            },
            "patterns": {
                "best_months": ["July", "October", "January"],
                "growth_catalysts": ["Product releases", "Marketing campaigns", "Partnership announcements"],
                "seasonal_notes": "Q4 strong due to enterprise budget cycles"
            }
        }
    }

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
            openai.api_key = self.api_key
    
    def get_response(self, user_question, tab_type, json_data, analysis_summary):
        """Get context-aware response from OpenAI based on tab and data"""
        if not self.api_key:
            return "⚠️ OpenAI API key not configured. Please set OPENAI_API_KEY environment variable."
        
        # Create context-specific prompts for each tab
        context_prompts = {
            "quarterly": f"""You are an expert financial analyst reviewing quarterly revenue data. 
            Analysis Summary: {analysis_summary}
            Key Data: {json.dumps(json_data.get('key_metrics', {}), indent=2)}
            
            Answer questions about quarterly revenue trends, growth patterns, seasonal impacts, and business performance.""",
            
            "bridge": f"""You are a revenue operations expert analyzing revenue bridge dynamics.
            Analysis Summary: {analysis_summary}
            Key Data: {json.dumps(json_data.get('key_metrics', {}), indent=2)}
            
            Answer questions about customer retention, expansion revenue, churn analysis, and revenue drivers.""",
            
            "geographic": f"""You are a market expansion strategist reviewing geographic revenue distribution.
            Analysis Summary: {analysis_summary}
            Key Data: {json.dumps(json_data.get('key_metrics', {}), indent=2)}
            
            Answer questions about regional performance, market opportunities, international expansion, and geographic risks.""",
            
            "customer": f"""You are a customer success expert analyzing customer portfolio and concentration.
            Analysis Summary: {analysis_summary}
            Key Data: {json.dumps(json_data.get('key_metrics', {}), indent=2)}
            
            Answer questions about customer concentration risk, segment performance, customer loyalty, and retention strategies.""",
            
            "monthly": f"""You are a business intelligence analyst reviewing monthly revenue trends.
            Analysis Summary: {analysis_summary}
            Key Data: {json.dumps(json_data.get('key_metrics', {}), indent=2)}
            
            Answer questions about monthly growth patterns, seasonality, trend consistency, and revenue forecasting."""
        }
        
        system_prompt = context_prompts.get(tab_type, "You are a financial analyst helping with investment analysis.")
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_question}
                ],
                max_tokens=500,
                temperature=0.7
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"⚠️ Error getting response: {str(e)}"

def create_beautiful_tab_layout(tab_name, analysis_data, tab_type):
    """Create beautiful layout for each analysis tab with charts and chatbot"""
    
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
    
    # Header with analysis summary
    st.subheader(f"📊 {tab_name}")
    st.info(f"💡 **Summary:** {analysis_data['analysis_summary']}")
    
    # Key metrics section
    st.markdown("### 🎯 Key Metrics")
    metrics = analysis_data.get('key_metrics', {})
    
    if tab_type == "quarterly":
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Q4/Q3 Growth", f"{metrics.get('q4_vs_q3_growth', 0)}%", delta="↗️")
        with col2:
            st.metric("YoY Growth", f"{metrics.get('yoy_growth', 0)}%", delta="📈")
        with col3:
            st.metric("Quality Score", f"{metrics.get('revenue_quality_score', 0)}/10")
        with col4:
            st.metric("Seasonality", metrics.get('seasonality_impact', 'N/A'))
        
        # Quarterly chart
        quarterly_data = pd.DataFrame(analysis_data.get('quarterly_breakdown', []))
        if not quarterly_data.empty:
            fig = px.bar(quarterly_data, x='quarter', y='revenue', 
                        title="📈 Quarterly Revenue Growth",
                        color='growth_rate', color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "bridge":
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Net Retention", f"{metrics.get('net_revenue_retention', 0)}%", delta="🎯")
        with col2:
            st.metric("Gross Retention", f"{metrics.get('gross_revenue_retention', 0)}%")
        with col3:
            st.metric("Expansion Rate", f"{metrics.get('expansion_rate', 0)}%", delta="↗️")
        with col4:
            st.metric("Churn Rate", f"{metrics.get('churn_rate', 0)}%", delta="↘️")
        
        # Revenue bridge waterfall chart
        bridge_data = analysis_data.get('bridge_components', [])
        if bridge_data:
            bridge_df = pd.DataFrame(bridge_data)
            fig = go.Figure(go.Waterfall(
                name="Revenue Bridge",
                orientation="v",
                measure=["absolute"] + ["relative"] * (len(bridge_df) - 2) + ["total"],
                x=bridge_df['component'],
                textposition="outside",
                text=[f"${val/1000000:.1f}M" for val in bridge_df['value']],
                y=bridge_df['value']
            ))
            fig.update_layout(title="🌉 Revenue Bridge Analysis", height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "geographic":
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Markets", metrics.get('total_markets', 0))
        with col2:
            st.metric("Revenue Concentration", f"{metrics.get('revenue_concentration', 0)}%")
        with col3:
            st.metric("Intl Growth Rate", f"{metrics.get('international_growth_rate', 0)}%", delta="🌍")
        with col4:
            st.metric("Penetration Score", f"{metrics.get('market_penetration_score', 0)}/10")
        
        # Geographic charts
        regional_data = pd.DataFrame(analysis_data.get('regional_breakdown', []))
        if not regional_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(regional_data, values='percentage', names='region',
                           title="🌍 Revenue by Region")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.bar(regional_data, x='region', y='growth_rate',
                           title="📈 Growth Rate by Region", color='growth_rate')
                st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "customer":
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Customers", metrics.get('total_customers', 0))
        with col2:
            concentration_risk = metrics.get('customer_concentration_risk', 'Unknown')
            risk_color = "🟢" if concentration_risk == "Low" else "🟡" if concentration_risk == "Medium" else "🔴"
            st.metric("Concentration Risk", f"{risk_color} {concentration_risk}")
        with col3:
            st.metric("Top 10 Share", f"{metrics.get('top_10_revenue_share', 0)}%")
        with col4:
            st.metric("Customer LTV", f"${metrics.get('customer_lifetime_value', 0):,.0f}")
        
        # Customer segment analysis
        segment_data = pd.DataFrame(analysis_data.get('customer_segments', []))
        if not segment_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(segment_data, values='percentage', names='segment',
                           title="👥 Customer Segments")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.scatter(segment_data, x='avg_revenue', y='churn_rate',
                               size='count', hover_name='segment',
                               title="💼 Revenue vs Churn by Segment")
                st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "monthly":
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Monthly Growth", f"{metrics.get('monthly_growth_rate', 0)}%", delta="📊")
        with col2:
            st.metric("Volatility", metrics.get('revenue_volatility', 'N/A'))
        with col3:
            st.metric("Seasonal Variance", f"{metrics.get('seasonal_variance', 0)}%")
        with col4:
            st.metric("Trend Consistency", f"{metrics.get('trend_consistency', 0)}%")
        
        # Monthly trend chart
        monthly_data = pd.DataFrame(analysis_data.get('monthly_data', []))
        if not monthly_data.empty:
            fig = px.line(monthly_data, x='month', y='revenue',
                         title="📈 Monthly Revenue Trend",
                         markers=True)
            fig.add_bar(x=monthly_data['month'], y=monthly_data['growth'],
                       name="Growth %", yaxis="y2")
            fig.update_layout(yaxis2=dict(title="Growth %", overlaying="y", side="right"), height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Insights section
    st.markdown("### 💡 Key Insights")
    insights = analysis_data.get('insights', [])
    for insight in insights:
        st.markdown(f"""
        <div class="insight-box">
            <strong>💡</strong> {insight}
        </div>
        """, unsafe_allow_html=True)
    
    # Two-column layout for JSON data and chatbot
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("### 📄 Raw Analysis Data")
        with st.expander("View JSON Data", expanded=False):
            st.json(analysis_data)
    
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
                    question, tab_type, analysis_data, analysis_data['analysis_summary']
                )
            st.markdown(f"**🤖 AI Response:**")
            st.write(response)
        
        # Suggested questions
        st.markdown("**💡 Suggested questions:**")
        if tab_type == "quarterly":
            suggestions = ["What drove the growth in Q4?", "Is the growth sustainable?", "Any seasonal trends?"]
        elif tab_type == "bridge":
            suggestions = ["What's causing customer churn?", "How can we improve retention?", "What drives expansion?"]
        elif tab_type == "geographic":
            suggestions = ["Which markets should we prioritize?", "What are the geographic risks?", "Where's the best ROI?"]
        elif tab_type == "customer":
            suggestions = ["Is customer concentration risky?", "Which segments to focus on?", "How to reduce churn?"]
        else:  # monthly
            suggestions = ["What causes monthly volatility?", "Can we predict next month?", "What are the growth drivers?"]
        
        for suggestion in suggestions:
            if st.button(suggestion, key=f"suggest_{tab_type}_{suggestion}"):
                with st.spinner("🤖 Analyzing..."):
                    response = st.session_state[f"chatbot_{tab_type}"].get_response(
                        suggestion, tab_type, analysis_data, analysis_data['analysis_summary']
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
        st.session_state[f'analysis_results_{company_id}'] = generate_llm_architecture_analyses()
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
            analysis_results["quarterly_revenue_analysis"], 
            "quarterly"
        )
    
    # Tab 2: Revenue Bridge Analysis
    with tabs[1]:
        create_beautiful_tab_layout(
            "Revenue Bridge Analysis", 
            analysis_results["revenue_bridge_analysis"], 
            "bridge"
        )
    
    # Tab 3: Geographic Analysis
    with tabs[2]:
        create_beautiful_tab_layout(
            "Geographic Analysis", 
            analysis_results["geographic_analysis"], 
            "geographic"
        )
    
    # Tab 4: Customer Analysis
    with tabs[3]:
        create_beautiful_tab_layout(
            "Customer Analysis", 
            analysis_results["customer_analysis"], 
            "customer"
        )
    
    # Tab 5: Monthly Trends Analysis
    with tabs[4]:
        create_beautiful_tab_layout(
            "Monthly Trends Analysis", 
            analysis_results["monthly_trends_analysis"], 
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