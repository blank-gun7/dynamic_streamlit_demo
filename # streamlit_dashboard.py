# streamlit_dashboard.py
  import streamlit as st
  import requests
  import pandas as pd
  import plotly.express as px
  import plotly.graph_objects as go
  from datetime import datetime
  import time

  # Configuration
  MAIN_API_URL = "http://52.66.205.91:8000"  # Your main API
  # MAIN_API_URL = "http://localhost:8000"    # For local 
  development

  st.set_page_config(
      page_title="Business Intelligence Dashboard",
      page_icon="📊",
      layout="wide"
  )

  def main():
      st.title("🏢 Business Intelligence Dashboard")
      st.markdown("Upload your business data and get comprehensive 
  AI-powered analysis")

      # Sidebar for navigation
      st.sidebar.title("Navigation")
      page = st.sidebar.selectbox("Choose a page", [
          "📈 Generate New Report",
          "📋 View Reports History",
          "🤖 Chat with Reports"
      ])

      if page == "📈 Generate New Report":
          generate_report_page()
      elif page == "📋 View Reports History":
          reports_history_page()
      elif page == "🤖 Chat with Reports":
          chatbot_page()

  def generate_report_page():
      st.header("📈 Generate New Report")

      # File upload section
      st.subheader("1. Upload Your Data Files")
      uploaded_files = st.file_uploader(
          "Choose CSV, Excel, or JSON files",
          type=['csv', 'xlsx', 'xls', 'json'],
          accept_multiple_files=True,
          help="Upload multiple files for comprehensive analysis"
      )

      if uploaded_files:
          st.success(f"✅ {len(uploaded_files)} file(s) selected:")
          for file in uploaded_files:
              st.text(f"📄 {file.name} ({file.size} bytes)")

          # Generate report button
          if st.button("🚀 Generate Comprehensive Report",
  type="primary"):
              generate_report(uploaded_files)

  def generate_report(uploaded_files):
      """Generate report using Main API"""

      # Show progress
      progress_bar = st.progress(0)
      status_text = st.empty()

      try:
          # Prepare files for upload
          files = []
          for uploaded_file in uploaded_files:
              files.append(("files", (uploaded_file.name,
  uploaded_file.getvalue(), uploaded_file.type)))

          # Add form data
          data = {"report_type": "general"}

          status_text.text("📤 Uploading files and generating 
  report...")
          progress_bar.progress(25)

          # Call Main API
          response = requests.post(
              f"{MAIN_API_URL}/upload-and-generate",
              files=files,
              data=data,
              timeout=300  # 5 minute timeout
          )

          progress_bar.progress(50)

          if response.status_code == 200:
              result = response.json()

              # Extract report ID and dashboard data
              report_id = result.get("_id")
              dashboard_data = result.get("llm_response", {})

              progress_bar.progress(75)
              status_text.text("✅ Report generated successfully!")
              progress_bar.progress(100)

              # Store in session state for persistence
              st.session_state.current_report = {
                  "report_id": report_id,
                  "dashboard_data": dashboard_data,
                  "generated_at": datetime.now().strftime("%Y-%m-%d 
  %H:%M:%S")
              }

              # Display dashboard
              time.sleep(1)  # Brief pause for UX
              status_text.empty()
              progress_bar.empty()

              display_dashboard(dashboard_data, report_id)

          else:
              st.error(f"❌ Error generating report: 
  {response.status_code}")
              st.text(response.text)

      except requests.exceptions.Timeout:
          st.error("⏰ Request timed out. The analysis is taking 
  longer than expected.")
      except requests.exceptions.ConnectionError:
          st.error("🔌 Could not connect to the API. Please check if
   the server is running.")
      except Exception as e:
          st.error(f"💥 Unexpected error: {str(e)}")

  def display_dashboard(dashboard_data, report_id):
      """Display the comprehensive dashboard"""

      st.header("📊 Analysis Results")

      # Report metadata
      col1, col2, col3 = st.columns(3)
      with col1:
          st.metric("Report ID", report_id[:8] + "...")
      with col2:
          st.metric("Status", dashboard_data.get("status",
  "Unknown"))
      with col3:
          st.metric("Generated", dashboard_data.get("generated_at",
  "")[:16])

      # Summary and insights
      if "summary" in dashboard_data:
          st.subheader("📋 Executive Summary")
          st.info(dashboard_data["summary"])

      if "insights" in dashboard_data:
          st.subheader("💡 Key Insights")
          for i, insight in
  enumerate(dashboard_data["insights"][:5], 1):
              st.write(f"{i}. {insight}")

      # KPI Metrics
      if "metrics" in dashboard_data:
          st.subheader("📈 Key Performance Indicators")
          metrics = dashboard_data["metrics"]

          # Create columns for metrics
          metric_cols = st.columns(4)

          metric_items = list(metrics.items())[:4]  # Show first 4 
  metrics
          for i, (key, value) in enumerate(metric_items):
              with metric_cols[i]:
                  formatted_key = key.replace('_', ' ').title()
                  if isinstance(value, (int, float)):
                      if 'revenue' in key.lower():
                          st.metric(formatted_key, f"${value:,.0f}")
                      else:
                          st.metric(formatted_key, f"{value:,.0f}")
                  else:
                      st.metric(formatted_key, str(value))

      # Dashboard Sections
      if "sections" in dashboard_data:
          sections = dashboard_data["sections"]

          # Revenue Section
          if "revenue" in sections:
              display_revenue_section(sections["revenue"])

          # Geographic Section  
          if "geographic" in sections:
              display_geographic_section(sections["geographic"])

          # Customer Section
          if "customer" in sections:
              display_customer_section(sections["customer"])

          # Churn Section
          if "churn" in sections:
              display_churn_section(sections["churn"])

  def display_revenue_section(revenue_section):
      """Display revenue analysis charts"""

      st.subheader("💰 Revenue Analysis")

      charts = revenue_section.get("charts", [])

      for chart in charts:
          chart_type = chart.get("type")
          chart_title = chart.get("title", "Chart")
          chart_data = chart.get("data", [])

          if not chart_data:
              continue

          st.write(f"**{chart_title}**")

          if chart_type == "waterfall_chart":
              # Create waterfall chart
              categories = [d.get("category", "") for d in
  chart_data]
              values = [d.get("value", 0) for d in chart_data]

              fig = go.Figure(go.Waterfall(
                  name="Revenue Bridge",
                  orientation="v",
                  measure=["relative"] * len(categories),
                  x=categories,
                  y=values,
                  connector={"line": {"color": "rgb(63, 63, 63)"}},
              ))
              fig.update_layout(title=chart_title,
  xaxis_title="Category", yaxis_title="Revenue")
              st.plotly_chart(fig, use_container_width=True)

          elif chart_type == "bar_chart":
              # Create bar chart
              df = pd.DataFrame(chart_data)
              if "quarter" in df.columns and "revenue" in
  df.columns:
                  fig = px.bar(df, x="quarter", y="revenue",
  title=chart_title)
                  st.plotly_chart(fig, use_container_width=True)
              elif "customer" in df.columns:
                  # Customer revenue chart
                  fig = px.bar(df.head(10), x="customer",
  y="q4_revenue", title=chart_title)
                  fig.update_xaxes(tickangle=45)
                  st.plotly_chart(fig, use_container_width=True)

          elif chart_type == "pie_chart":
              # Create pie chart
              df = pd.DataFrame(chart_data)
              if "category" in df.columns and "count" in df.columns:
                  fig = px.pie(df, values="count", names="category",
   title=chart_title)
                  st.plotly_chart(fig, use_container_width=True)

  def display_geographic_section(geo_section):
      """Display geographic analysis charts"""

      st.subheader("🗺️ Geographic Analysis")

      charts = geo_section.get("charts", [])

      for chart in charts:
          chart_data = chart.get("data", [])
          chart_title = chart.get("title", "Geographic Chart")

          if chart_data and isinstance(chart_data[0], dict):
              if "country" in chart_data[0] and "revenue" in
  chart_data[0]:
                  df = pd.DataFrame(chart_data)

                  # Bar chart for top countries
                  st.write(f"**{chart_title}**")
                  fig = px.bar(df.head(15), x="country",
  y="revenue", title=chart_title)
                  fig.update_xaxes(tickangle=45)
                  st.plotly_chart(fig, use_container_width=True)

  def display_customer_section(customer_section):
      """Display customer analysis charts"""

      st.subheader("👥 Customer Analysis")

      charts = customer_section.get("charts", [])

      for chart in charts:
          chart_data = chart.get("data", [])
          chart_title = chart.get("title", "Customer Chart")

          if chart_data:
              df = pd.DataFrame(chart_data)
              st.write(f"**{chart_title}**")

              if "segment" in df.columns and "count" in df.columns:
                  fig = px.pie(df, values="count", names="segment",
  title=chart_title)
                  st.plotly_chart(fig, use_container_width=True)

  def display_churn_section(churn_section):
      """Display churn analysis charts"""

      st.subheader("📉 Churn Analysis")

      charts = churn_section.get("charts", [])

      for chart in charts:
          chart_data = chart.get("data", [])
          chart_title = chart.get("title", "Churn Chart")

          if chart_data:
              df = pd.DataFrame(chart_data)
              st.write(f"**{chart_title}**")

              if "status" in df.columns and "count" in df.columns:
                  fig = px.bar(df, x="status", y="count",
  title=chart_title)
                  st.plotly_chart(fig, use_container_width=True)

  def reports_history_page():
      """View all generated reports"""

      st.header("📋 Reports History")

      try:
          # Get reports list from Main API
          response =
  requests.get(f"{MAIN_API_URL}/reports?limit=20")

          if response.status_code == 200:
              data = response.json()
              reports = data.get("reports", [])

              if reports:
                  st.success(f"Found {len(reports)} reports")

                  # Display reports in a table
                  for report in reports:
                      with st.expander(f"Report 
  {report['_id'][:8]}... - {report.get('created_at', '')[:16]}"):
                          col1, col2, col3 = st.columns(3)

                          with col1:
                              st.write(f"**Type:** 
  {report.get('report_type', 'General')}")
                              st.write(f"**Files:** {', 
  '.join(report.get('filenames', []))}")

                          with col2:
                              st.write(f"**Summary:** 
  {report.get('summary', 'No summary')}")

                          with col3:
                              if st.button(f"📖 View Report",
  key=f"view_{report['_id']}"):
                                  view_saved_report(report['_id'])
              else:
                  st.info("No reports found. Generate your first 
  report!")

          else:
              st.error("Failed to fetch reports")

      except Exception as e:
          st.error(f"Error fetching reports: {str(e)}")

  def view_saved_report(report_id):
      """View a saved report by ID"""

      try:
          response =
  requests.get(f"{MAIN_API_URL}/report/{report_id}")

          if response.status_code == 200:
              dashboard_data = response.json()
              st.success("✅ Report loaded successfully!")
              display_dashboard(dashboard_data, report_id)
          else:
              st.error("Failed to load report")

      except Exception as e:
          st.error(f"Error loading report: {str(e)}")

  def chatbot_page():
      """Chatbot interface for asking questions about reports"""

      st.header("🤖 Chat with Your Reports")

      # Chat interface
      if "chat_history" not in st.session_state:
          st.session_state.chat_history = []

      # Display chat history
      for message in st.session_state.chat_history:
          if message["role"] == "user":
              st.write(f"👤 **You:** {message['content']}")
          else:
              st.write(f"🤖 **AI:** {message['content']}")

      # Chat input
      user_question = st.text_input("Ask a question about your 
  data:", placeholder="Why did revenue grow in Q4?")

      if st.button("Send") and user_question:
          # Add user message to history
          st.session_state.chat_history.append({"role": "user",
  "content": user_question})

          try:
              # Call chatbot API
              response = requests.post(
                  f"{MAIN_API_URL}/chatbot",
                  json={
                      "question": user_question,
                      "context": {}
                  }
              )

              if response.status_code == 200:
                  result = response.json()
                  answer = result.get("answer", "I couldn't generate
   an answer.")

                  # Add AI response to history
                  st.session_state.chat_history.append({"role":
  "assistant", "content": answer})

                  # Rerun to update display
                  st.rerun()
              else:
                  st.error("Failed to get chatbot response")

          except Exception as e:
              st.error(f"Chatbot error: {str(e)}")

  if __name__ == "__main__":
      main()