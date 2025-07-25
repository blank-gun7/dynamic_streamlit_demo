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
        "Region-wise Revenue Analysis": "D._Region_wise_Revenue_Analysis.json",
        "Customer Concentration Analysis": "E._Customer_concentration_analysis.json",
        "Month-on-Month Revenue Analysis": "F._Month_on_Month_Revenue_analysis.json"
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
    elif selected_view == "Customer Concentration Analysis":
        display_customer_concentration_analysis(df)
    elif selected_view == "Month-on-Month Revenue Analysis":
        display_month_on_month_analysis(df)

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

def display_customer_concentration_analysis(df):
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

def display_month_on_month_analysis(df):
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

if __name__ == "__main__":
    main()