import streamlit as st
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import openai
import os
from dotenv import load_dotenv
import time

st.set_page_config(
    page_title="Zenalyst.ai - Revenue Analytics Dashboard",
    #page_icon="zenalyst ai.jpg",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_json_data(file_path):
    """Load and return JSON data from file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Error loading {file_path}: {e}")
        return []


def initialize_openai():
    """Initialize OpenAI client using environment variables or Streamlit secrets"""
    # Try Streamlit secrets first (for cloud deployment), then fall back to .env
    try:
        api_key = st.secrets["OPENAI_API_KEY"]
    except:
        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
    
    if api_key and api_key.strip():
        try:
            # Test the API key by creating a client
            client = openai.OpenAI(api_key=api_key.strip())
            # Store the client in session state for reuse
            st.session_state.openai_client = client
            return True
        except Exception as e:
            st.error(f"Error initializing OpenAI client: {str(e)}")
            return False
    else:
        st.error("OpenAI API key not found. Please configure OPENAI_API_KEY in Streamlit secrets or .env file.")
        return False

def validate_uploaded_file(uploaded_file):
    """Validate uploaded Excel file"""
    try:
        # Read Excel file
        df = pd.read_excel(uploaded_file)
        
        # Convert datetime columns to strings to avoid JSON serialization issues
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]' or pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Fill NaN values with None for JSON compatibility
        df = df.where(pd.notnull(df), None)
        
        # Convert DataFrame to list of dictionaries (JSON-like format)
        data = df.to_dict('records')
        return data
    except Exception as e:
        st.error(f"Error parsing {uploaded_file.name}: {str(e)}")
        return None

def show_loading_screen():
    """Display 30-second loading screen"""
    st.markdown("### 🚀 Processing Your Data...")
    st.markdown("Please wait while we analyze your revenue files...")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(30):
        progress = (i + 1) / 30
        progress_bar.progress(progress)
        status_text.text(f'⚡ Analyzing data... {30-i} seconds remaining')
        time.sleep(1)
    
    status_text.text('✅ Analysis complete! Redirecting to dashboard...')
    time.sleep(1)
    
    # Mark loading as complete
    st.session_state.loading_complete = True
    st.rerun()

def main():
    # Initialize OpenAI at the start
    if 'openai_initialized' not in st.session_state:
        st.session_state.openai_initialized = initialize_openai()
    
    # Add company branding
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        #st.image("zenalyst ai.jpg", width=200)
        st.markdown("<h1 style='text-align: center; color: #1f77b4;'> Zenalyst.ai</h1>", unsafe_allow_html=True)
        st.markdown("<h3 style='text-align: center; color: #666;'>📊 Revenue Analytics Dashboard</h3>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Original JSON files mapping
    original_files = {
        "Quarterly Revenue & QoQ Growth": "A._Quarterly_Revenue_and_QoQ_growth.json",
        "Revenue Bridge & Churn Analysis": "B._Revenue_Bridge_and_Churned_Analysis.json", 
        "Country-wise Revenue Analysis": "C._Country_wise_Revenue_Analysis.json",
        "Customer Concentration Analysis": "E._Customer_concentration_analysis.json",
        "Month-on-Month Revenue Analysis": "F._Month_on_Month_Revenue_analysis.json"
    }
    
    # Initialize session state
    if "data_source" not in st.session_state:
        st.session_state.data_source = "original"  # Default to original files
    if "files_uploaded" not in st.session_state:
        st.session_state.files_uploaded = False
    if "loading_complete" not in st.session_state:
        st.session_state.loading_complete = False
    if "uploaded_data" not in st.session_state:
        st.session_state.uploaded_data = {}
    
    # Sidebar for navigation
    st.sidebar.image("zenalyst ai.jpg", width=150)
    st.sidebar.title("📈 Analytics Views")
    
    # Data source selector
    data_source = st.sidebar.radio(
        "Choose Data Source:",
        ["📄 Original JSON Files", "📊 Upload New Files"],
        index=0 if st.session_state.data_source == "original" else 1
    )
    
    if data_source == "📄 Original JSON Files":
        st.session_state.data_source = "original"
        
        # Show original JSON file options
        available_views = list(original_files.keys())
        selected_view = st.sidebar.selectbox("Select Analysis Type:", available_views)
        
        # Load and display original data
        file_path = original_files[selected_view]
        data = load_json_data(file_path)
        
        if not data:
            st.error(f"No data available for {selected_view}")
            return
        
        df = pd.DataFrame(data)
        
        # Display based on selected view
        if selected_view == "Quarterly Revenue & QoQ Growth":
            display_quarterly_analysis(df, data, selected_view)
        elif selected_view == "Revenue Bridge & Churn Analysis": 
            display_churn_analysis(df, data, selected_view)
        elif selected_view == "Country-wise Revenue Analysis":
            display_country_analysis(df, data, selected_view)
        elif selected_view == "Customer Concentration Analysis":
            display_customer_concentration_analysis(df, data, selected_view)
        elif selected_view == "Month-on-Month Revenue Analysis":
            display_month_on_month_analysis(df, data, selected_view)
    
    else:  # Upload New Files
        st.session_state.data_source = "uploaded"
        
        # Show upload interface if files not uploaded or loading not complete
        if not st.session_state.files_uploaded or not st.session_state.loading_complete:
            
            if not st.session_state.files_uploaded:
                # File upload interface
                st.markdown("### 📁 Upload Your Revenue Data")
                st.markdown("Upload your Excel files to start analyzing your revenue data with AI-powered insights.")
                
                uploaded_files = st.file_uploader(
                    "Choose Excel files",
                    type=['xlsx', 'xls'],
                    accept_multiple_files=True,
                    help="Upload your revenue analysis Excel files (.xlsx or .xls)"
                )
                
                if uploaded_files and len(uploaded_files) > 0:
                    st.success(f"✅ {len(uploaded_files)} file(s) uploaded successfully!")
                    
                    # Validate and store uploaded files
                    valid_files = {}
                    for uploaded_file in uploaded_files:
                        data = validate_uploaded_file(uploaded_file)
                        if data:
                            # Map files based on content patterns
                            filename = uploaded_file.name.lower()
                            if "quarterly" in filename or "qoq" in filename:
                                valid_files["Quarterly Revenue & QoQ Growth"] = data
                            elif "bridge" in filename or "churn" in filename:
                                valid_files["Revenue Bridge & Churn Analysis"] = data
                            elif "country" in filename:
                                valid_files["Country-wise Revenue Analysis"] = data
                            elif "customer" in filename or "concentration" in filename:
                                valid_files["Customer Concentration Analysis"] = data
                            elif "month" in filename or "mom" in filename:
                                valid_files["Month-on-Month Revenue Analysis"] = data
                            else:
                                # Default mapping based on file order or name
                                view_name = f"Analysis - {uploaded_file.name}"
                                valid_files[view_name] = data
                    
                    if valid_files:
                        st.session_state.uploaded_data = valid_files
                        
                        if st.button("📊 Start Analysis", type="primary"):
                            st.session_state.files_uploaded = True
                            st.rerun()
            
            elif st.session_state.files_uploaded and not st.session_state.loading_complete:
                # Show loading screen
                show_loading_screen()
            
            return  # Don't show dashboard until upload and loading complete
        
        # Show dashboard for uploaded files
        available_views = list(st.session_state.uploaded_data.keys())
        if not available_views:
            st.error("No valid data files found. Please upload valid Excel files.")
            if st.sidebar.button("🔄 Reset Upload"):
                st.session_state.files_uploaded = False
                st.session_state.loading_complete = False
                st.session_state.uploaded_data = {}
                st.rerun()
            return
        
        selected_view = st.sidebar.selectbox("Select Analysis Type:", available_views)
        
        # Add reset button in sidebar
        st.sidebar.markdown("---")
        if st.sidebar.button("🔄 Upload New Files"):
            st.session_state.files_uploaded = False
            st.session_state.loading_complete = False
            st.session_state.uploaded_data = {}
            st.rerun()
        
        # Load selected data from uploaded files
        data = st.session_state.uploaded_data[selected_view]
        df = pd.DataFrame(data)
        
        # Display based on selected view (now with integrated AI)
        if "quarterly" in selected_view.lower() or "qoq" in selected_view.lower():
            display_quarterly_analysis(df, data, selected_view)
        elif "bridge" in selected_view.lower() or "churn" in selected_view.lower():
            display_churn_analysis(df, data, selected_view)
        elif "country" in selected_view.lower():
            display_country_analysis(df, data, selected_view)
        elif "customer" in selected_view.lower() or "concentration" in selected_view.lower():
            display_customer_concentration_analysis(df, data, selected_view)
        elif "month" in selected_view.lower() or "mom" in selected_view.lower():
            display_month_on_month_analysis(df, data, selected_view)
        else:
            # Generic display for unknown file types
            st.header(f"📊 {selected_view}")
            st.subheader("Data Overview")
            st.dataframe(df, use_container_width=True)
            add_ai_sections(data, selected_view)

def load_specific_data_context(data, view_title):
    """Load specific JSON data as formatted context for OpenAI"""
    context = f"Revenue Analytics Data for {view_title}:\n\n"
    context += f"Dataset: {view_title}\n"
    
    try:
        if len(str(data)) > 15000:  # Truncate very large datasets
            context += f"{json.dumps(data[:100], indent=2, default=str)}\n... (truncated, showing first 100 records)\n\n"
        else:
            context += f"{json.dumps(data, indent=2, default=str)}\n\n"
    except (TypeError, ValueError) as e:
        # Fallback to string representation if JSON serialization fails
        context += f"Data (as string): {str(data)[:10000]}...\n\n"
    
    return context

def generate_view_summary(data, view_title):
    """Generate AI executive summary for specific view"""
    if not st.session_state.get('openai_initialized', False):
        return "OpenAI API key not configured. Please check your .env file."
    
    try:
        data_context = load_specific_data_context(data, view_title)
        
        # Create view-specific prompt
        view_prompts = {
            "Quarterly Revenue & QoQ Growth": "Analyze Q3 vs Q4 customer performance, growth patterns, and variance insights. Focus on top performers and concerning trends.",
            "Revenue Bridge & Churn Analysis": "Analyze churn patterns, expansion/contraction insights, new vs lost revenue, and customer retention metrics.",
            "Country-wise Revenue Analysis": "Analyze geographic revenue distribution, market performance, and international growth opportunities.",
            "Region-wise Revenue Analysis": "Analyze regional performance patterns, growth opportunities, and geographic concentration risks.",
            "Customer Concentration Analysis": "Analyze customer concentration risks, top performer insights, revenue dependencies, and customer portfolio health.",
            "Month-on-Month Revenue Analysis": "Analyze monthly revenue trends, seasonal patterns, growth momentum, and forecasting insights."
        }
        
        specific_instruction = view_prompts.get(view_title, "Analyze the revenue data and provide key insights.")
        
        prompt = f"""You are a senior business intelligence analyst. {specific_instruction}

{data_context}

Create a focused executive summary with:

## Key Insights
- 3-4 most important findings from this data
- Specific numbers and percentages

## Performance Highlights
- Top performers and standout metrics
- Notable trends and patterns

## Risk Factors
- Potential concerns or red flags
- Areas requiring attention

## Strategic Recommendations
- 2-3 actionable next steps
- Optimization opportunities

Keep the response concise but comprehensive, focusing specifically on this dataset only."""

        client = st.session_state.openai_client
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1500,
            temperature=0.3
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        return f"Error generating summary: {str(e)}"

def display_view_chatbot(data, view_title):
    """Display chatbot for specific view"""
    if not st.session_state.get('openai_initialized', False):
        st.error("OpenAI API key not configured. Please check your .env file.")
        return
    
    st.subheader(f"💬 Ask Questions About {view_title}")
    
    # Initialize chat history for this view
    chat_key = f"chat_history_{view_title}"
    if chat_key not in st.session_state:
        st.session_state[chat_key] = []
    
    # Quick question buttons specific to each view
    view_questions = {
        "Quarterly Revenue & QoQ Growth": [
            "Which customers had the highest growth from Q3 to Q4?",
            "What was the overall revenue variance between quarters?",
            "Which customers are showing declining performance?"
        ],
        "Revenue Bridge & Churn Analysis": [
            "What's our churn rate and which customers churned?",
            "How much revenue came from expansion vs new customers?",
            "Which customers show contraction risks?"
        ],
        "Country-wise Revenue Analysis": [
            "Which countries generate the most revenue?",
            "What percentage of revenue comes from the top 5 countries?",
            "Are there emerging markets with growth potential?"
        ],
        "Region-wise Revenue Analysis": [
            "How is revenue distributed across regions?",
            "Which regions show the strongest performance?",
            "Are there regional concentration risks?"
        ],
        "Customer Concentration Analysis": [
            "Who are our top 10 customers by revenue?",
            "What's our customer concentration risk?",
            "How dependent are we on our largest customers?"
        ],
        "Month-on-Month Revenue Analysis": [
            "What are the monthly growth trends in 2024?",
            "Which months showed the strongest performance?",
            "Are there seasonal patterns in our revenue?"
        ]
    }
    
    questions = view_questions.get(view_title, [])
    
    if questions:
        st.write("**Quick Questions:**")
        cols = st.columns(len(questions))
        for i, question in enumerate(questions):
            with cols[i]:
                if st.button(f"❓ {question[:30]}...", key=f"q_{view_title}_{i}"):
                    st.session_state[f"current_question_{view_title}"] = question
    
    # Chat input
    question_key = f"current_question_{view_title}"
    if question_key in st.session_state:
        user_question = st.session_state[question_key]
        del st.session_state[question_key]
    else:
        user_question = st.chat_input(f"Ask about {view_title} data...", key=f"chat_{view_title}")
    
    # Display chat history
    for message in st.session_state[chat_key]:
        with st.chat_message(message["role"]):
            st.write(message["content"])
    
    # Process new question
    if user_question:
        # Add user message
        st.session_state[chat_key].append({"role": "user", "content": user_question})
        
        with st.chat_message("user"):
            st.write(user_question)
        
        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                try:
                    data_context = load_specific_data_context(data, view_title)
                    
                    prompt = f"""You are a business analyst. Answer the user's question based on this specific dataset only.

{data_context}

User Question: {user_question}

Provide a focused answer with:
1. Direct response to the question
2. Specific data points and numbers
3. Brief analysis of what this means
4. One actionable insight if applicable

Keep it concise and data-driven."""

                    client = st.session_state.openai_client
                    response = client.chat.completions.create(
                        model="gpt-4o",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=800,
                        temperature=0.5
                    )
                    
                    ai_response = response.choices[0].message.content
                    st.write(ai_response)
                    
                    # Add to chat history
                    st.session_state[chat_key].append({"role": "assistant", "content": ai_response})
                    
                except Exception as e:
                    st.error(f"Error: {str(e)}")

def add_ai_sections(data, view_title):
    """Add AI executive summary and chatbot sections to any view"""
    st.markdown("---")
    
    # Executive Summary Section
    st.subheader("🤖 AI Executive Summary")
    
    summary_key = f"summary_{view_title}"
    if summary_key not in st.session_state:
        with st.spinner("Generating AI insights..."):
            st.session_state[summary_key] = generate_view_summary(data, view_title)
    
    if st.session_state[summary_key]:
        st.markdown(st.session_state[summary_key])
        
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("🔄 Regenerate", key=f"regen_{view_title}"):
                with st.spinner("Regenerating insights..."):
                    st.session_state[summary_key] = generate_view_summary(data, view_title)
                    st.rerun()
    
    st.markdown("---")
    
    # Chatbot Section
    display_view_chatbot(data, view_title)

def display_quarterly_analysis(df, data, view_title):
    st.header("📅 Quarterly Revenue & QoQ Growth Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Key Metrics")
        total_q3 = df['Quarter 3 Revenue'].sum()
        total_q4 = df['Quarter 4 Revenue'].sum()
        total_variance = df['Variance'].sum()
        
        st.metric("Total Q3 Revenue", f"${total_q3:,.2f}")
        st.metric("Total Q4 Revenue", f"${total_q4:,.2f}")
        st.metric("Total Variance", f"${total_variance:,.2f}")
    
    with col2:
        # Top performers by variance
        top_growth = df.nlargest(10, 'Variance')
        fig = px.bar(top_growth, x='Variance', y='Customer Name', 
                    title="Top 10 Revenue Growth (Q3 to Q4)",
                    orientation='h')
        st.plotly_chart(fig, use_container_width=True)
    
    # Data table with filters
    st.subheader("Detailed Customer Analysis")
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        min_revenue = st.number_input("Min Q4 Revenue", value=0.0)
    with col2:
        growth_only = st.checkbox("Show only positive growth")
    
    filtered_df = df[df['Quarter 4 Revenue'] >= min_revenue]
    if growth_only:
        filtered_df = filtered_df[filtered_df['Variance'] > 0]
    
    st.dataframe(filtered_df, use_container_width=True)
    
    # Add AI sections
    add_ai_sections(data, view_title)

def display_churn_analysis(df, data, view_title):
    st.header("🔄 Revenue Bridge & Churn Analysis")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_churned = df['Churned Revenue'].sum()
        st.metric("Total Churned Revenue", f"${total_churned:,.2f}")
        
    with col2:
        total_new = df['New Revenue'].sum()
        st.metric("Total New Revenue", f"${total_new:,.2f}")
        
    with col3:
        total_expansion = df['Expansion Revenue'].sum()
        st.metric("Total Expansion Revenue", f"${total_expansion:,.2f}")
    
    # Revenue bridge waterfall chart
    st.subheader("Revenue Bridge Analysis")
    
    revenue_categories = ['Nov Revenue', 'New Revenue', 'Expansion Revenue', 
                         'Contraction Revenue', 'Churned Revenue', 'Dec Revenue']
    
    q3_total = df['Quarter 3 Revenue'].sum()
    new_total = df['New Revenue'].sum()
    expansion_total = df['Expansion Revenue'].sum()
    contraction_total = -df['Contraction Revenue'].sum()
    churned_total = -df['Churned Revenue'].sum()
    q4_total = df['Quarter 4 Revenue'].sum()
    
    values = [q3_total, new_total, expansion_total, contraction_total, churned_total, q4_total]
    
    fig = go.Figure(go.Waterfall(
        name="Revenue Bridge",
        orientation="v",
        measure=["absolute", "relative", "relative", "relative", "relative", "total"],
        x=revenue_categories,
        text=[f"${v:,.0f}" for v in values],
        y=values,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
    ))
    
    fig.update_layout(title="Revenue Bridge: November to December", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.subheader("Customer-wise Revenue Bridge")
    st.dataframe(df, use_container_width=True)
    
    # Add AI sections
    add_ai_sections(data, view_title)

def display_country_analysis(df, data, view_title):
    st.header("🌍 Country-wise Revenue Analysis")
    
    # Remove null values and sort by revenue
    df_clean = df[df['Yearly Revenue'].notna()].sort_values('Yearly Revenue', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Countries by Revenue")
        top_10 = df_clean.head(10)
        fig = px.bar(top_10, x='Yearly Revenue', y='Country',
                    title="Top 10 Countries by Revenue",
                    orientation='h')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Revenue Distribution")
        fig = px.pie(df_clean.head(8), values='Yearly Revenue', names='Country',
                    title="Revenue Share by Country (Top 8)")
        st.plotly_chart(fig, use_container_width=True)
    
    # Key metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        total_revenue = df_clean['Yearly Revenue'].sum()
        st.metric("Total Global Revenue", f"${total_revenue:,.2f}")
    with col2:
        top_country = df_clean.iloc[0]
        st.metric("Top Country", f"{top_country['Country']}")
    with col3:
        top_revenue = top_country['Yearly Revenue']
        st.metric("Top Country Revenue", f"${top_revenue:,.2f}")
    
    # Full data table
    st.subheader("All Countries")
    st.dataframe(df_clean, use_container_width=True)
    
    # Add AI sections
    add_ai_sections(data, view_title)

def display_customer_concentration_analysis(df, data, view_title):
    st.header("👥 Customer Concentration Analysis")
    
    # Sort by revenue descending
    df_sorted = df.sort_values('Total Revenue', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Key Metrics")
        total_revenue = df_sorted['Total Revenue'].sum()
        top_customer = df_sorted.iloc[0]
        top_5_revenue = df_sorted.head(5)['Total Revenue'].sum()
        top_10_revenue = df_sorted.head(10)['Total Revenue'].sum()
        
        st.metric("Total Revenue", f"${total_revenue:,.2f}")
        st.metric("Top Customer", top_customer['Customer Name'])
        st.metric("Top Customer Revenue", f"${top_customer['Total Revenue']:,.2f}")
        st.metric("Top 5 Customers %", f"{(top_5_revenue/total_revenue)*100:.1f}%")
        st.metric("Top 10 Customers %", f"{(top_10_revenue/total_revenue)*100:.1f}%")
    
    with col2:
        st.subheader("Top 10 Customers by Revenue")
        top_10 = df_sorted.head(10)
        fig = px.bar(top_10, x='Total Revenue', y='Customer Name',
                    title="Top 10 Customers by Total Revenue",
                    orientation='h')
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Revenue concentration analysis
    st.subheader("Revenue Concentration Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Pareto chart
        df_sorted_reset = df_sorted.reset_index(drop=True)
        df_sorted_reset['Cumulative Revenue'] = df_sorted_reset['Total Revenue'].cumsum()
        df_sorted_reset['Cumulative %'] = (df_sorted_reset['Cumulative Revenue'] / total_revenue) * 100
        
        # Show top 20 for better visualization
        top_20 = df_sorted_reset.head(20)
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=top_20.index + 1,
            y=top_20['Total Revenue'],
            name='Revenue',
            yaxis='y'
        ))
        fig.add_trace(go.Scatter(
            x=top_20.index + 1,
            y=top_20['Cumulative %'],
            mode='lines+markers',
            name='Cumulative %',
            yaxis='y2',
            line=dict(color='red')
        ))
        
        fig.update_layout(
            title='Customer Revenue Pareto Analysis (Top 20)',
            xaxis_title='Customer Rank',
            yaxis=dict(title='Revenue ($)', side='left'),
            yaxis2=dict(title='Cumulative %', side='right', overlaying='y'),
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Revenue distribution pie chart
        top_15 = df_sorted.head(15)
        others_revenue = total_revenue - top_15['Total Revenue'].sum()
        
        # Create pie chart data
        pie_data = top_15[['Customer Name', 'Total Revenue']].copy()
        if others_revenue > 0:
            pie_data = pd.concat([pie_data, pd.DataFrame({
                'Customer Name': ['Others'],
                'Total Revenue': [others_revenue]
            })], ignore_index=True)
        
        fig = px.pie(pie_data, values='Total Revenue', names='Customer Name',
                    title="Revenue Distribution (Top 15 + Others)")
        st.plotly_chart(fig, use_container_width=True)
    
    # Revenue tiers analysis
    st.subheader("Customer Revenue Tiers")
    
    # Define revenue tiers
    tier_1M = df_sorted[df_sorted['Total Revenue'] >= 1000000]
    tier_500K = df_sorted[(df_sorted['Total Revenue'] >= 500000) & (df_sorted['Total Revenue'] < 1000000)]
    tier_100K = df_sorted[(df_sorted['Total Revenue'] >= 100000) & (df_sorted['Total Revenue'] < 500000)]
    tier_below_100K = df_sorted[df_sorted['Total Revenue'] < 100000]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("$1M+ Customers", len(tier_1M))
        st.metric("$1M+ Revenue", f"${tier_1M['Total Revenue'].sum():,.2f}")
    
    with col2:
        st.metric("$500K-$1M Customers", len(tier_500K))
        st.metric("$500K-$1M Revenue", f"${tier_500K['Total Revenue'].sum():,.2f}")
    
    with col3:
        st.metric("$100K-$500K Customers", len(tier_100K))
        st.metric("$100K-$500K Revenue", f"${tier_100K['Total Revenue'].sum():,.2f}")
    
    with col4:
        st.metric("Below $100K Customers", len(tier_below_100K))
        st.metric("Below $100K Revenue", f"${tier_below_100K['Total Revenue'].sum():,.2f}")
    
    # Search and filter functionality
    st.subheader("Customer Search & Analysis")
    
    col1, col2 = st.columns(2)
    with col1:
        search_term = st.text_input("Search Customer Name:", "")
        min_revenue_filter = st.number_input("Minimum Revenue Filter:", value=0.0, step=1000.0)
    
    with col2:
        show_top_n = st.slider("Show Top N Customers:", min_value=10, max_value=100, value=50)
    
    # Apply filters
    filtered_df = df_sorted.copy()
    
    if search_term:
        filtered_df = filtered_df[filtered_df['Customer Name'].str.contains(search_term, case=False, na=False)]
    
    if min_revenue_filter > 0:
        filtered_df = filtered_df[filtered_df['Total Revenue'] >= min_revenue_filter]
    
    filtered_df = filtered_df.head(show_top_n)
    
    st.dataframe(filtered_df, use_container_width=True)
    
    # Add AI sections
    add_ai_sections(data, view_title)

def display_month_on_month_analysis(df, data, view_title):
    st.header("📈 Month-on-Month Revenue Analysis")
    
    # Convert Month to datetime
    df['Month'] = pd.to_datetime(df['Month'])
    df['Month_Label'] = df['Month'].dt.strftime('%b %Y')
    df = df.sort_values('Month')
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = df['Revenue'].sum()
        st.metric("Total Revenue (2024)", f"${total_revenue:,.2f}")
    
    with col2:
        avg_monthly = df['Revenue'].mean()
        st.metric("Average Monthly Revenue", f"${avg_monthly:,.2f}")
    
    with col3:
        max_month = df.loc[df['Revenue'].idxmax()]
        st.metric("Best Month", max_month['Month_Label'])
        st.metric("Best Month Revenue", f"${max_month['Revenue']:,.2f}")
    
    with col4:
        latest_variance = df.iloc[-1]['Variance in %']
        st.metric("Latest MoM Growth", f"{latest_variance:.2f}%")
    
    # Revenue trend chart
    st.subheader("Monthly Revenue Trend")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.line(df, x='Month_Label', y='Revenue', 
                     title='Monthly Revenue Trend',
                     markers=True)
        fig.update_layout(xaxis_tickangle=-45)
        fig.update_traces(line=dict(width=3), marker=dict(size=8))
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Month-over-Month variance chart
        df_positive = df[df['Variance in %'] >= 0]
        df_negative = df[df['Variance in %'] < 0]
        
        fig = go.Figure()
        
        if not df_positive.empty:
            fig.add_trace(go.Bar(
                x=df_positive['Month_Label'],
                y=df_positive['Variance in %'],
                name='Positive Growth',
                marker_color='green',
                text=[f"{x:.1f}%" for x in df_positive['Variance in %']],
                textposition='outside'
            ))
        
        if not df_negative.empty:
            fig.add_trace(go.Bar(
                x=df_negative['Month_Label'],
                y=df_negative['Variance in %'],
                name='Negative Growth',
                marker_color='red',
                text=[f"{x:.1f}%" for x in df_negative['Variance in %']],
                textposition='outside'
            ))
        
        fig.update_layout(
            title='Month-over-Month Growth %',
            xaxis_title='Month',
            yaxis_title='Growth %',
            xaxis_tickangle=-45,
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Revenue variance analysis
    st.subheader("Revenue Variance Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Variance amount chart
        colors = ['green' if x >= 0 else 'red' for x in df['Variance in amount']]
        fig = px.bar(df, x='Month_Label', y='Variance in amount',
                    title='Monthly Revenue Variance (Amount)',
                    color=df['Variance in amount'],
                    color_continuous_scale=['red', 'green'])
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Growth phases analysis
        growth_months = len(df[df['Variance in %'] > 0])
        decline_months = len(df[df['Variance in %'] < 0])
        stable_months = len(df[df['Variance in %'] == 0])
        
        phase_data = pd.DataFrame({
            'Phase': ['Growth', 'Decline', 'Stable'],
            'Months': [growth_months, decline_months, stable_months]
        })
        
        fig = px.pie(phase_data, values='Months', names='Phase',
                    title='Growth vs Decline Months',
                    color_discrete_map={'Growth': 'green', 'Decline': 'red', 'Stable': 'blue'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Quarterly aggregation
    st.subheader("Quarterly Performance Summary")
    
    df['Quarter'] = df['Month'].dt.to_period('Q')
    quarterly_data = df.groupby('Quarter').agg({
        'Revenue': 'sum',
        'Variance in amount': 'sum'
    }).reset_index()
    quarterly_data['Quarter'] = quarterly_data['Quarter'].astype(str)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(quarterly_data, x='Quarter', y='Revenue',
                    title='Quarterly Revenue Summary',
                    text='Revenue')
        fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Quarterly Metrics")
        for _, row in quarterly_data.iterrows():
            st.metric(
                f"{row['Quarter']} Revenue", 
                f"${row['Revenue']:,.2f}",
                f"${row['Variance in amount']:,.2f}"
            )
    
    # Growth insights
    st.subheader("Growth Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        best_growth_month = df.loc[df['Variance in %'].idxmax()]
        st.info(f"**Best Growth Month:** {best_growth_month['Month_Label']} with {best_growth_month['Variance in %']:.2f}% growth")
    
    with col2:
        worst_decline_month = df.loc[df['Variance in %'].idxmin()]
        st.warning(f"**Worst Decline Month:** {worst_decline_month['Month_Label']} with {worst_decline_month['Variance in %']:.2f}% decline")
    
    with col3:
        avg_growth_rate = df['Variance in %'].mean()
        st.success(f"**Average MoM Growth:** {avg_growth_rate:.2f}%")
    
    # Detailed monthly table
    st.subheader("Detailed Monthly Data")
    
    # Format the display dataframe
    display_df = df[['Month_Label', 'Revenue', 'Variance in amount', 'Variance in %']].copy()
    display_df.columns = ['Month', 'Revenue ($)', 'Variance Amount ($)', 'Variance (%)']
    display_df['Revenue ($)'] = display_df['Revenue ($)'].apply(lambda x: f"${x:,.2f}")
    display_df['Variance Amount ($)'] = display_df['Variance Amount ($)'].apply(lambda x: f"${x:,.2f}")
    display_df['Variance (%)'] = display_df['Variance (%)'].apply(lambda x: f"{x:.2f}%")
    
    st.dataframe(display_df, use_container_width=True)
    
    # Add AI sections
    add_ai_sections(data, view_title)

if __name__ == "__main__":
    main()