import streamlit as st
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(page_title="Revenue Analytics Dashboard", layout="wide")

def load_json_data(file_path):
    """Load and return JSON data from file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Error loading {file_path}: {e}")
        return []

def main():
    st.title("📊 Revenue Analytics Dashboard")
    st.markdown("---")
    
    # File mapping with clean titles
    files = {
        "Quarterly Revenue & QoQ Growth": "A._Quarterly_Revenue_and_QoQ_growth.json",
        "Revenue Bridge & Churn Analysis": "B._Revenue_Bridge_and_Churned_Analysis.json", 
        "Country-wise Revenue Analysis": "C._Country_wise_Revenue_Analysis.json",
        "Region-wise Revenue Analysis": "D._Region_wise_Revenue_Analysis.json"
    }
    
    # Sidebar for navigation
    st.sidebar.title("📈 Analytics Views")
    selected_view = st.sidebar.selectbox("Select Analysis Type:", list(files.keys()))
    
    # Load selected data
    file_path = files[selected_view]
    data = load_json_data(file_path)
    
    if not data:
        st.error("No data available for selected view")
        return
    
    df = pd.DataFrame(data)
    
    # Display based on selected view
    if selected_view == "Quarterly Revenue & QoQ Growth":
        display_quarterly_analysis(df)
    elif selected_view == "Revenue Bridge & Churn Analysis": 
        display_churn_analysis(df)
    elif selected_view == "Country-wise Revenue Analysis":
        display_country_analysis(df)
    elif selected_view == "Region-wise Revenue Analysis":
        display_region_analysis(df)

def display_quarterly_analysis(df):
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

def display_churn_analysis(df):
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
    
    revenue_categories = ['Q3 Revenue', 'New Revenue', 'Expansion Revenue', 
                         'Contraction Revenue', 'Churned Revenue', 'Q4 Revenue']
    
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
    
    fig.update_layout(title="Revenue Bridge: Q3 to Q4", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.subheader("Customer-wise Revenue Bridge")
    st.dataframe(df, use_container_width=True)

def display_country_analysis(df):
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

def display_region_analysis(df):
    st.header("🗺️ Region-wise Revenue Analysis")
    
    # Filter out null revenues
    df_clean = df[df['Yearly Revenue'].notna()]
    
    if df_clean.empty:
        st.warning("No revenue data available for regions")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Regional Revenue")
        fig = px.bar(df_clean, x='Region', y='Yearly Revenue',
                    title="Revenue by Region")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Regional Distribution")
        fig = px.pie(df_clean, values='Yearly Revenue', names='Region',
                    title="Revenue Share by Region")
        st.plotly_chart(fig, use_container_width=True)
    
    # Metrics
    total_revenue = df_clean['Yearly Revenue'].sum()
    st.metric("Total Regional Revenue", f"${total_revenue:,.2f}")
    
    # Data table
    st.subheader("Regional Breakdown")
    st.dataframe(df_clean, use_container_width=True)

if __name__ == "__main__":
    main()