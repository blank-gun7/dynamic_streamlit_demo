import streamlit as st
import pandas as pd
import plotly.express as px
import json
import sqlite3
import hashlib

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
                FOREIGN KEY (company_id) REFERENCES companies (id)
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
        # Insert new data
        cursor.execute(
            "INSERT INTO company_data (company_id, data_type, data_content) VALUES (?, ?, ?)",
            (company_id, data_type, json.dumps(data_content))
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
        st.title("= Revenue Analytics Platform")
        
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
            fig1 = px.bar(df, x='Customer Name', y=['Quarter 3 Revenue', 'Quarter 4 Revenue'],
                         title="Quarterly Revenue Comparison", barmode='group')
            fig1.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            fig2 = px.scatter(df, x='Quarter 3 Revenue', y='Quarter 4 Revenue',
                            size='Percentage of Variance', hover_name='Customer Name',
                            title="Revenue Growth Analysis")
            st.plotly_chart(fig2, use_container_width=True)
        
        # Top performers
        top_growth = df.nlargest(5, 'Percentage of Variance')
        st.subheader("Top 5 Growth Performers")
        st.dataframe(top_growth[['Customer Name', 'Percentage of Variance']])
    
    def create_country_wise_charts(self, data):
        df = pd.DataFrame(data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig1 = px.pie(df, values='Revenue', names='Country',
                         title="Revenue Distribution by Country")
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            fig2 = px.bar(df, x='Country', y='Revenue',
                         title="Country-wise Revenue")
            st.plotly_chart(fig2, use_container_width=True)
    
    def create_customer_concentration_charts(self, data):
        df = pd.DataFrame(data)
        
        fig = px.treemap(df, path=['Customer Name'], values='Revenue Share',
                        title="Customer Revenue Concentration")
        st.plotly_chart(fig, use_container_width=True)
        
        # Concentration analysis
        st.subheader("Concentration Analysis")
        total_customers = len(df)
        top_10_pct = df.nlargest(max(1, int(total_customers * 0.1)), 'Revenue Share')
        concentration = top_10_pct['Revenue Share'].sum()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Customers", total_customers)
        with col2:
            st.metric("Top 10% Revenue Share", f"{concentration:.1f}%")
        with col3:
            risk_level = "High" if concentration > 80 else "Medium" if concentration > 60 else "Low"
            st.metric("Concentration Risk", risk_level)

class ChatBot:
    def __init__(self, data, data_type):
        self.data = data
        self.data_type = data_type
        
    def process_query(self, query):
        # Simple rule-based chatbot for demo purposes
        # In production, you'd integrate with OpenAI or similar
        
        query_lower = query.lower()
        df = pd.DataFrame(self.data)
        
        if "total" in query_lower or "sum" in query_lower:
            if "revenue" in query_lower:
                if 'Revenue' in df.columns:
                    total = df['Revenue'].sum()
                    return f"Total revenue is ${total:,.2f}"
                elif 'Quarter 4 Revenue' in df.columns:
                    total = df['Quarter 4 Revenue'].sum()
                    return f"Total Q4 revenue is ${total:,.2f}"
        
        elif "top" in query_lower or "best" in query_lower:
            if "customer" in query_lower or "client" in query_lower:
                if 'Customer Name' in df.columns and 'Revenue' in df.columns:
                    top_customer = df.loc[df['Revenue'].idxmax()]
                    return f"Top customer is {top_customer['Customer Name']} with ${top_customer['Revenue']:,.2f}"
                elif 'Customer Name' in df.columns and 'Quarter 4 Revenue' in df.columns:
                    top_customer = df.loc[df['Quarter 4 Revenue'].idxmax()]
                    return f"Top customer is {top_customer['Customer Name']} with Q4 revenue of ${top_customer['Quarter 4 Revenue']:,.2f}"
        
        elif "average" in query_lower or "mean" in query_lower:
            if "revenue" in query_lower:
                if 'Revenue' in df.columns:
                    avg = df['Revenue'].mean()
                    return f"Average revenue is ${avg:,.2f}"
        
        elif "count" in query_lower or "number" in query_lower:
            if "customer" in query_lower:
                count = len(df)
                return f"There are {count} customers in the data"
        
        else:
            return f"I can help you analyze {self.data_type} data. Try asking about totals, top performers, averages, or customer counts."

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
    st.title(f"=� {st.session_state.company_name} - Data Management")
    
    company = db.get_company_by_investee(st.session_state.user_id)
    if not company:
        st.error("Company not found")
        return
    
    company_id = company[0]
    
    st.subheader("Upload Revenue Data Files")
    
    # File upload section
    uploaded_files = st.file_uploader(
        "Upload JSON files", 
        type=['json'], 
        accept_multiple_files=True,
        help="Upload your 5 revenue analysis JSON files"
    )
    
    if uploaded_files:
        for uploaded_file in uploaded_files:
            try:
                data = json.load(uploaded_file)
                file_name = uploaded_file.name
                
                # Determine data type based on filename
                if "Quarterly_Revenue" in file_name:
                    data_type = "quarterly_revenue"
                elif "Revenue_Bridge" in file_name:
                    data_type = "revenue_bridge"
                elif "Country_wise" in file_name:
                    data_type = "country_wise"
                elif "Customer_concentration" in file_name:
                    data_type = "customer_concentration"
                elif "Month_on_Month" in file_name:
                    data_type = "monthly_revenue"
                else:
                    data_type = file_name.replace('.json', '')
                
                db.save_company_data(company_id, data_type, data)
                st.success(f" {file_name} uploaded successfully!")
                
            except json.JSONDecodeError:
                st.error(f"L Error reading {uploaded_file.name}: Invalid JSON format")
    
    # Display current data
    st.subheader("Current Data Overview")
    company_data = db.get_company_data(company_id)
    
    if company_data:
        for data_type, data in company_data.items():
            with st.expander(f"{data_type.replace('_', ' ').title()} Data"):
                st.json(data[:3] if isinstance(data, list) else data)  # Show first 3 records
    else:
        st.info("No data uploaded yet. Please upload your JSON files above.")

def investor_dashboard(db):
    st.title("=� Investor Portfolio Dashboard")
    
    # Get companies for this investor
    companies = db.get_companies_for_investor(st.session_state.user_id)
    
    if not companies:
        st.warning("No companies in your portfolio yet.")
        return
    
    # Company selection
    company_options = {f"{comp[1]}": comp[0] for comp in companies}
    selected_company_name = st.selectbox("Select Company", list(company_options.keys()))
    
    if selected_company_name:
        selected_company_id = company_options[selected_company_name]
        
        st.subheader(f"=� {selected_company_name} Analytics Dashboard")
        
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
                st.subheader("=� Ask about Quarterly Revenue")
                chatbot = ChatBot(data, "Quarterly Revenue")
                query = st.text_input("Ask a question about the quarterly revenue data:", 
                                    key="q1_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("> " + response)
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
                st.subheader("=� Ask about Revenue Bridge")
                chatbot = ChatBot(data, "Revenue Bridge")
                query = st.text_input("Ask a question about the revenue bridge data:", 
                                    key="rb_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("> " + response)
            else:
                st.warning("Revenue bridge data not available")
        
        # Tab 3: Country Analysis
        with tabs[2]:
            if "country_wise" in company_data:
                data = company_data["country_wise"]
                visualizer.create_country_wise_charts(data)
                
                # Chatbot
                st.subheader("=� Ask about Country Analysis")
                chatbot = ChatBot(data, "Country Analysis")
                query = st.text_input("Ask a question about the country analysis data:", 
                                    key="country_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("> " + response)
            else:
                st.warning("Country analysis data not available")
        
        # Tab 4: Customer Concentration
        with tabs[3]:
            if "customer_concentration" in company_data:
                data = company_data["customer_concentration"]
                visualizer.create_customer_concentration_charts(data)
                
                # Chatbot
                st.subheader("=� Ask about Customer Concentration")
                chatbot = ChatBot(data, "Customer Concentration")
                query = st.text_input("Ask a question about customer concentration:", 
                                    key="cc_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("> " + response)
            else:
                st.warning("Customer concentration data not available")
        
        # Tab 5: Monthly Trends
        with tabs[4]:
            if "monthly_revenue" in company_data:
                data = company_data["monthly_revenue"]
                st.subheader("Monthly Revenue Trends")
                df = pd.DataFrame(data)
                
                if 'Month' in df.columns and 'Revenue' in df.columns:
                    fig = px.line(df, x='Month', y='Revenue', 
                                title="Month-on-Month Revenue Trend")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(df)
                
                # Chatbot
                st.subheader("=� Ask about Monthly Trends")
                chatbot = ChatBot(data, "Monthly Revenue")
                query = st.text_input("Ask a question about monthly trends:", 
                                    key="monthly_chat")
                if query:
                    response = chatbot.process_query(query)
                    st.write("> " + response)
            else:
                st.warning("Monthly revenue data not available")

if __name__ == "__main__":
    main()