# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Standard Workflow

1. First think through the problem, read the codebase for relevant files, and write a plan using the TodoWrite tool.
2. The plan should have a list of todo items that you can check off as you complete them
3. Before you begin working, check in with me and I will verify the plan.
4. Then, begin working on the todo items, marking them as complete as you go.
5. Please every step of the way just give me a high level explanation of what changes you made
6. Make every task and code change you do as simple as possible. We want to avoid making any massive or complex changes. Every change should impact as little code as possible. Everything is about simplicity.
7. Finally, add a review section to the todo list with a summary of the changes you made and any other relevant information.



## Project Overview

This is a Streamlit-based revenue analytics dashboard called "Zenalyst.ai" that provides comprehensive investment analysis with AI-powered insights. The application features:

- **Dynamic Multi-Source Architecture**: Supports both AWS S3 bucket integration and local file processing
- **Multi-tenant Architecture**: Supporting both investors and investees with user management
- **SQLite Database**: For user management and data storage  
- **Dynamic Dashboard Generation**: Automatically creates tabs and visualizations based on discovered data types
- **Advanced Schema Detection**: AI-powered analysis of JSON structure for automatic dashboard configuration
- **S3 Integration**: Real-time discovery and processing of JSON files from AWS S3 buckets
- **Enhanced AI Integration**: Schema-aware OpenAI GPT-4 integration for dynamic executive summaries and chatbots
- **Performance Optimization**: Multi-level caching system for improved performance
- **Error Handling**: Comprehensive error management and user guidance system

## Development Commands

**Running the application:**
```bash
streamlit run app.py
```

**Installing dependencies:**
```bash
pip install -r requirements.txt
```

**Environment setup:**
The application supports multiple configuration options via environment variables or Streamlit secrets (`.streamlit/secrets.toml`):

**Required for AI features:**
- `OPENAI_API_KEY` - Your OpenAI API key for GPT-4 integration

**Optional for S3 integration (Two-Bucket Architecture):**

*For investor analytics (JSON data reading):*
- `AWS_ACCESS_KEY_ID` - Your AWS access key
- `AWS_SECRET_ACCESS_KEY` - Your AWS secret key  
- `S3_BUCKET_NAME` - S3 bucket for JSON analytics data (investor side)
- `S3_REGION` - AWS region (default: us-east-1)
- `S3_PREFIX` - Optional folder prefix in analytics bucket
- `DATA_REFRESH_INTERVAL` - Cache refresh interval in minutes (default: 60)

*For investee file storage (raw file uploads):*
- `S3_FILE_STORAGE_BUCKET` - S3 bucket for raw file storage (investee side)
- `S3_FILE_STORAGE_REGION` - AWS region for file storage (default: us-east-1)
- `S3_FILE_STORAGE_PREFIX` - Optional folder prefix for file storage (default: uploads)

**Configuration Priority:**
1. Environment variables are checked first
2. Streamlit secrets (`.streamlit/secrets.toml`) are used as fallback
3. Default values are used if neither is available

**Database:**
- SQLite database (`revenue_analytics.db`) is created automatically
- No manual database setup required

## Architecture Overview

### Core Components

**app.py** - Main application file containing:
- `DatabaseManager` class for SQLite operations and user/company management
- `AuthManager` class for login/registration functionality  
- `S3ConfigManager` class for AWS S3 configuration and connection management (investor side)
- `S3DataDiscovery` class for discovering and categorizing JSON files from S3 (investor side)
- `S3FileStorageManager` class for raw file uploads to S3 storage (investee side)
- `JSONSchemaAnalyzer` class for automatic schema detection and analysis type inference
- `DynamicDashboardGenerator` class for runtime dashboard generation based on schema
- `CacheManager` class for performance optimization with TTL-based caching
- `ErrorHandler` class for centralized error handling and user-friendly messages
- `ConfigValidator` class for configuration validation and setup guidance
- Enhanced `OpenAIChatbot` class with schema-aware responses
- Five specialized display functions for known analytics types (backward compatibility)
- Dynamic tab generation system that adapts to any data structure

### Data Architecture

**User Types:**
- **Investors**: Can view multiple portfolio companies and analyze their data
- **Investees**: Can upload data and manage investor connections

**Database Schema:**
- `users` table with user authentication and type management
- `companies` table linking investees to their companies
- `investor_companies` table for many-to-many relationships
- `company_data` table storing JSON data by type (quarterly, bridge, geographic, customer, monthly) - *used by investor side*
- `uploaded_files` table for tracking S3 file uploads (file metadata, S3 keys, upload timestamps) - *used by investee side*

**Investor Data Processing Pipeline (Analytics):**
1. **S3 Discovery Phase**: Scan analytics S3 bucket for JSON files using `S3DataDiscovery`
2. **Schema Analysis Phase**: Analyze each JSON file structure using `JSONSchemaAnalyzer`
3. **Auto-Classification**: Automatically detect analysis types based on content and naming
4. **Dynamic Dashboard Generation**: Create appropriate tabs and visualizations using `DynamicDashboardGenerator`
5. **Caching Layer**: Cache results for performance using `CacheManager`
6. **Fallback to Local**: If S3 not configured, fall back to local JSON files

**Investee File Storage Pipeline (Raw Files):**
1. **File Upload**: Multi-format file upload (Excel, PDF, Markdown) via Streamlit interface
2. **S3 Storage**: Direct upload to dedicated file storage S3 bucket using `S3FileStorageManager`
3. **Metadata Tracking**: File information stored in `uploaded_files` database table
4. **File Management**: Download, view, and delete functionality with presigned URLs
5. **Organized Structure**: Files stored with company/date hierarchy for easy organization

### AI Integration

**Dynamic Executive Summaries:**
- Generated via `generate_ai_executive_summary()` with schema-aware prompts
- `generate_dynamic_prompt()` creates custom prompts based on detected schema
- Automatic analysis sections based on detected data patterns (revenue, time-series, categorical)
- Fallback to static prompts for known analysis types
- Fallback summary generation when OpenAI is unavailable

**Schema-Aware Interactive Chatbots:**
- `OpenAIChatbot` class enhanced with schema awareness
- `display_chatbot_with_schema()` provides context-specific responses
- Dynamic question suggestions based on detected data patterns
- `generate_schema_based_suggestions()` creates relevant question prompts
- Per-tab chatbot instances with conversation history
- Context includes schema analysis, executive summary, and actual JSON data

## Data Structure

The application now supports **any JSON data structure** with automatic schema detection:

### Dynamic Data Support
- **Automatic Schema Detection**: `JSONSchemaAnalyzer` analyzes any JSON structure
- **Content-Based Classification**: Detects data types based on column names and patterns
- **Flexible Visualization**: `DynamicDashboardGenerator` creates appropriate charts for any data
- **Naming-Based Categories**: Files categorized by filename patterns and folder structure

### Standard Data Formats (Legacy Support)
1. **Quarterly Revenue** (`A._Quarterly_Revenue_and_QoQ_growth.json`): Customer-level Q3/Q4 revenue with variance analysis
2. **Revenue Bridge** (`B._Revenue_Bridge_and_Churned_Analysis.json`): Customer expansion, contraction, and churn data  
3. **Geographic** (`C._Country_wise_Revenue_Analysis.json`): Country-wise revenue distribution
4. **Customer Concentration** (`E._Customer_concentration_analysis.json`): Customer portfolio and concentration metrics
5. **Monthly Trends** (`F._Month_on_Month_Revenue_analysis.json`): Month-over-month revenue patterns

### Schema Detection Patterns
- **Revenue Columns**: revenue, amount, value, price, cost
- **Date Columns**: date, month, quarter, year, time
- **ID Columns**: id, name, customer, client
- **Categorical Columns**: country, region, category, type
- **Percentage Columns**: percent, %, rate, ratio

## Key Implementation Details

**Data Type Auto-Detection:**
The system automatically classifies uploaded Excel data based on sheet names and column headers:
- "quarterly"/"qoq" → quarterly_revenue
- "bridge"/"churn" → revenue_bridge  
- "country"/"region" → country_wise
- "customer"/"concentration" → customer_concentration
- "month"/"monthly" → monthly_revenue

**JSON Serialization:**
Custom serialization handles problematic data types:
- DateTime objects → ISO format strings
- NumPy types → Python native types  
- NaN/NaT values → None
- Infinity values → None

**Multi-User Data Isolation:**
Each company's data is stored separately with company_id foreign keys, ensuring proper data isolation between different portfolio companies.

**Session State Management:**
Complex session state handling for:
- Chat history per analytics tab
- Analysis completion tracking per company
- User authentication state
- Pending questions from suggestion buttons

## File Structure

```
├── app.py                          # Main application file (single-file architecture)
├── app_backup.py                   # Backup version
├── revenue_analytics.db            # SQLite database (auto-created)
├── requirements.txt                # Python dependencies
├── packages.txt                    # System packages for deployment
├── .streamlit/
│   ├── config.toml                # Streamlit configuration
│   └── secrets.toml               # Local secrets (not committed)
├── *.json                         # Sample JSON data files
└── CLAUDE.md                      # This file

## Testing Approach

No formal test framework is configured. Test the application by:
1. Running the Streamlit app locally with `streamlit run app.py`
2. Testing both investor and investee user flows
3. Uploading sample Excel files with various formats
4. Verifying AI responses with valid OpenAI API key
5. Testing data isolation between different companies/users
6. Testing S3 integration if configured

## Dependencies

Key dependencies include:
- `streamlit` - Web application framework
- `pandas` - Data processing and Excel reading
- `plotly` - Interactive visualizations  
- `openai` - GPT-4 API integration
- `sqlite3` - Database operations (built-in Python)
- `numpy` - Numerical computations
- `hashlib` - Password hashing (built-in Python)

## Security Considerations

- Passwords are hashed using SHA-256 before storage
- Session-based authentication with Streamlit session state
- Data isolation enforced at database level with foreign keys
- No sensitive data should be committed to version control
- OpenAI API key should be set via environment variables or Streamlit secrets