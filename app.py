import streamlit as st
import pandas as pd
import plotly.express as px
import json
import sqlite3
import hashlib
import numpy as np
from datetime import datetime
import time
import random

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

def generate_mock_llm_analysis():
    """Generate 5 mock JSON analysis results simulating LLM output"""
    
    # Mock analysis results that would come from your LLM architecture
    mock_analyses = {
        "financial_health_score": {
            "overall_score": 85,
            "revenue_growth": 22.5,
            "market_position": "Strong",
            "risk_factors": ["Customer concentration", "Geographic dependency"],
            "opportunities": ["International expansion", "Product diversification"],
            "financial_metrics": {
                "revenue_growth_rate": "22.5%",
                "profit_margin": "18.3%",
                "cash_flow": "Positive",
                "debt_to_equity": "0.45"
            },
            "prediction": "Strong growth trajectory expected for next 2 quarters"
        },
        
        "market_analysis": {
            "market_size": "$2.4B",
            "company_market_share": "3.2%",
            "competitive_position": "Top 10 player",
            "growth_potential": "High",
            "key_competitors": ["CompetitorA", "CompetitorB", "CompetitorC"],
            "market_trends": {
                "trend_1": "Increasing demand for digital solutions",
                "trend_2": "Shift towards subscription models",
                "trend_3": "ESG compliance becoming critical"
            },
            "swot_analysis": {
                "strengths": ["Strong technology", "Experienced team", "Market leadership"],
                "weaknesses": ["Limited geographical presence", "High customer concentration"],
                "opportunities": ["AI integration", "International markets", "New verticals"],
                "threats": ["Economic downturn", "New regulations", "Competitive pressure"]
            }
        },
        
        "risk_assessment": {
            "overall_risk_level": "Medium",
            "risk_score": 65,
            "key_risks": [
                {
                    "risk": "Customer Concentration",
                    "impact": "High",
                    "probability": "Medium",
                    "mitigation": "Diversify customer base"
                },
                {
                    "risk": "Market Competition",
                    "impact": "Medium", 
                    "probability": "High",
                    "mitigation": "Strengthen competitive moat"
                },
                {
                    "risk": "Economic Downturn",
                    "impact": "High",
                    "probability": "Low",
                    "mitigation": "Build cash reserves"
                }
            ],
            "regulatory_compliance": "Good",
            "financial_stability": "Strong",
            "operational_risks": "Low to Medium"
        },
        
        "growth_projections": {
            "next_quarter_revenue": "$12.5M",
            "next_year_revenue": "$48.2M",
            "growth_rate_projection": "28% annually",
            "confidence_level": "High",
            "quarterly_projections": [
                {"quarter": "Q1 2024", "projected_revenue": 12.5, "confidence": 85},
                {"quarter": "Q2 2024", "projected_revenue": 13.8, "confidence": 80},
                {"quarter": "Q3 2024", "projected_revenue": 14.2, "confidence": 75},
                {"quarter": "Q4 2024", "projected_revenue": 15.1, "confidence": 70}
            ],
            "key_growth_drivers": [
                "Product innovation",
                "Market expansion", 
                "Strategic partnerships",
                "Operational efficiency"
            ],
            "potential_challenges": [
                "Supply chain disruption",
                "Talent acquisition",
                "Regulatory changes"
            ]
        },
        
        "investment_recommendation": {
            "recommendation": "BUY",
            "target_valuation": "$85M",
            "confidence_score": 78,
            "investment_horizon": "2-3 years",
            "key_reasons": [
                "Strong revenue growth trajectory",
                "Experienced management team",
                "Large addressable market",
                "Competitive advantages"
            ],
            "concerns": [
                "Customer concentration risk",
                "Market competition intensity"
            ],
            "suggested_investment_amount": "$5M - $8M",
            "expected_roi": "3.2x - 4.5x",
            "exit_strategy": ["Strategic acquisition", "IPO in 3-4 years"],
            "due_diligence_notes": "Recommend deeper dive into customer contracts and competitive positioning"
        }
    }
    
    return mock_analyses

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

def show_llm_analysis_interface(db, company_id, company_name):
    """Show the LLM analysis interface with processing and results"""
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title(f"🧠 LLM Analysis - {company_name}")
    with col2:
        if st.button("← Back to Portfolio"):
            if hasattr(st.session_state, 'show_analysis'):
                del st.session_state.show_analysis
            if hasattr(st.session_state, 'analyzing_company_id'):
                del st.session_state.analyzing_company_id
            if hasattr(st.session_state, 'analyzing_company_name'):
                del st.session_state.analyzing_company_name
            st.rerun()
    
    # Check if analysis is already completed
    if not hasattr(st.session_state, f'analysis_complete_{company_id}'):
        st.info("🚀 Starting comprehensive LLM analysis of your investment data...")
        
        # Show processing animation
        with st.container():
            st.subheader("🔄 Processing Investment Analysis")
            show_processing_animation()
        
        # Mark analysis as complete
        st.session_state[f'analysis_complete_{company_id}'] = True
        st.session_state[f'analysis_results_{company_id}'] = generate_mock_llm_analysis()
        st.rerun()
    
    # Show analysis results
    analysis_results = st.session_state[f'analysis_results_{company_id}']
    
    st.success("✅ Analysis Complete! Here are your detailed insights:")
    
    # Create tabs for the 5 analysis types
    tabs = st.tabs([
        "💰 Financial Health",
        "📈 Market Analysis", 
        "⚠️ Risk Assessment",
        "🚀 Growth Projections",
        "💎 Investment Recommendation"
    ])
    
    # Tab 1: Financial Health Score
    with tabs[0]:
        st.subheader("💰 Financial Health Analysis")
        data = analysis_results["financial_health_score"]
        
        # Key metrics in columns
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Overall Score", f"{data['overall_score']}/100", delta="↗️ Strong")
        with col2:
            st.metric("Revenue Growth", f"{data['revenue_growth']}%", delta="22.5%")
        with col3:
            st.metric("Market Position", data['market_position'])
        with col4:
            st.metric("Profit Margin", data['financial_metrics']['profit_margin'])
        
        # Financial metrics chart
        metrics_df = pd.DataFrame([
            {"Metric": "Revenue Growth", "Value": 22.5, "Benchmark": 15},
            {"Metric": "Profit Margin", "Value": 18.3, "Benchmark": 12},
            {"Metric": "Cash Flow", "Value": 85, "Benchmark": 70},
            {"Metric": "Debt Management", "Value": 78, "Benchmark": 65}
        ])
        
        fig = px.bar(metrics_df, x="Metric", y=["Value", "Benchmark"], 
                    title="Financial Performance vs Industry Benchmark",
                    barmode='group')
        st.plotly_chart(fig, use_container_width=True)
        
        # Risk factors and opportunities
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("⚠️ Risk Factors")
            for risk in data['risk_factors']:
                st.write(f"• {risk}")
        with col2:
            st.subheader("🎯 Opportunities") 
            for opp in data['opportunities']:
                st.write(f"• {opp}")
        
        st.info(f"📈 Prediction: {data['prediction']}")
    
    # Tab 2: Market Analysis
    with tabs[1]:
        st.subheader("📈 Market Analysis")
        data = analysis_results["market_analysis"]
        
        # Market overview
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Market Size", data['market_size'])
        with col2:
            st.metric("Market Share", data['company_market_share'])
        with col3:
            st.metric("Competitive Position", data['competitive_position'])
        
        # SWOT Analysis
        st.subheader("🎯 SWOT Analysis")
        swot_col1, swot_col2 = st.columns(2)
        
        with swot_col1:
            st.markdown("**💪 Strengths**")
            for strength in data['swot_analysis']['strengths']:
                st.write(f"✅ {strength}")
            
            st.markdown("**🚀 Opportunities**")
            for opp in data['swot_analysis']['opportunities']:
                st.write(f"💡 {opp}")
        
        with swot_col2:
            st.markdown("**🔧 Weaknesses**")
            for weakness in data['swot_analysis']['weaknesses']:
                st.write(f"⚠️ {weakness}")
                
            st.markdown("**⚡ Threats**")
            for threat in data['swot_analysis']['threats']:
                st.write(f"🚨 {threat}")
        
        # Market trends
        st.subheader("📊 Key Market Trends")
        for trend, desc in data['market_trends'].items():
            st.write(f"📈 {desc}")
    
    # Tab 3: Risk Assessment
    with tabs[2]:
        st.subheader("⚠️ Risk Assessment")
        data = analysis_results["risk_assessment"]
        
        # Overall risk metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Risk Level", data['overall_risk_level'])
        with col2:
            st.metric("Risk Score", f"{data['risk_score']}/100")
        with col3:
            st.metric("Financial Stability", data['financial_stability'])
        
        # Risk breakdown
        st.subheader("🎯 Key Risk Analysis")
        for risk in data['key_risks']:
            with st.expander(f"🚨 {risk['risk']} - {risk['impact']} Impact"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Impact:** {risk['impact']}")
                    st.write(f"**Probability:** {risk['probability']}")
                with col2:
                    st.write(f"**Mitigation:** {risk['mitigation']}")
        
        # Risk visualization
        risk_df = pd.DataFrame(data['key_risks'])
        fig = px.scatter(risk_df, x="probability", y="impact", 
                        size=[10, 8, 6], hover_name="risk",
                        title="Risk Impact vs Probability Matrix")
        st.plotly_chart(fig, use_container_width=True)
    
    # Tab 4: Growth Projections  
    with tabs[3]:
        st.subheader("🚀 Growth Projections")
        data = analysis_results["growth_projections"]
        
        # Key projections
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Next Quarter", data['next_quarter_revenue'])
        with col2:
            st.metric("Next Year", data['next_year_revenue']) 
        with col3:
            st.metric("Growth Rate", data['growth_rate_projection'])
        
        # Quarterly projections chart
        projections_df = pd.DataFrame(data['quarterly_projections'])
        
        fig = px.line(projections_df, x="quarter", y="projected_revenue",
                     title="Quarterly Revenue Projections",
                     markers=True)
        fig.add_bar(x=projections_df["quarter"], y=projections_df["confidence"],
                   name="Confidence %", yaxis="y2")
        fig.update_layout(yaxis2=dict(title="Confidence %", overlaying="y", side="right"))
        st.plotly_chart(fig, use_container_width=True)
        
        # Growth drivers and challenges
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🎯 Growth Drivers")
            for driver in data['key_growth_drivers']:
                st.write(f"🚀 {driver}")
        with col2:
            st.subheader("⚠️ Potential Challenges")
            for challenge in data['potential_challenges']:
                st.write(f"🔧 {challenge}")
    
    # Tab 5: Investment Recommendation
    with tabs[4]:
        st.subheader("💎 Investment Recommendation")
        data = analysis_results["investment_recommendation"]
        
        # Main recommendation
        if data['recommendation'] == 'BUY':
            st.success(f"🎯 **Recommendation: {data['recommendation']}**")
        elif data['recommendation'] == 'HOLD':
            st.warning(f"⚖️ **Recommendation: {data['recommendation']}**")
        else:
            st.error(f"🚫 **Recommendation: {data['recommendation']}**")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Target Valuation", data['target_valuation'])
        with col2:
            st.metric("Confidence Score", f"{data['confidence_score']}%")
        with col3:
            st.metric("Investment Horizon", data['investment_horizon'])
        with col4:
            st.metric("Expected ROI", data['expected_roi'])
        
        # Investment details
        st.subheader("💰 Investment Details")
        col1, col2 = st.columns(2)
        
        with col1:
            st.write(f"**Suggested Investment:** {data['suggested_investment_amount']}")
            st.write("**Exit Strategy:**")
            for strategy in data['exit_strategy']:
                st.write(f"• {strategy}")
        
        with col2:
            st.subheader("✅ Key Reasons")
            for reason in data['key_reasons']:
                st.write(f"✅ {reason}")
        
        # Concerns and due diligence
        st.subheader("⚠️ Areas of Concern")
        for concern in data['concerns']:
            st.write(f"⚠️ {concern}")
        
        st.info(f"📋 Due Diligence: {data['due_diligence_notes']}")
        
        # Final CTA
        st.subheader("🎯 Next Steps")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📊 Request Detailed DD"):
                st.success("Due diligence package requested!")
        with col2:
            if st.button("📞 Schedule Meeting"):
                st.success("Meeting request sent!")
        with col3:
            if st.button("💌 Contact Company"):
                st.success("Contact request sent!")
    
    # Generate PDF report button
    st.markdown("---")
    if st.button("📄 Generate PDF Report", type="primary"):
        st.success("📄 PDF report generated and sent to your email!")
        st.balloons()

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
                st.info(f"Processing {file_name}...")
                
                # Read Excel file
                excel_file = pd.ExcelFile(uploaded_file)
                sheet_names = excel_file.sheet_names
                
                st.write(f"Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")
                
                # Process each sheet
                for sheet_name in sheet_names:
                    df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                    
                    # Show processing info
                    st.info(f"Processing sheet: {sheet_name} ({len(df)} rows, {len(df.columns)} columns)")
                    
                    # Convert all datetime columns to strings BEFORE to_dict
                    for col in df.columns:
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            df[col] = df[col].astype(str)
                        elif df[col].dtype == 'object':
                            # Check if object column contains datetime objects
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
                    st.success(f"✅ Sheet '{sheet_name}' from {file_name} uploaded successfully!")
                
            except Exception as e:
                st.error(f"❌ Error reading {uploaded_file.name}: {str(e)}")
    
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
        show_llm_analysis_interface(db, st.session_state.analyzing_company_id, st.session_state.analyzing_company_name)
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