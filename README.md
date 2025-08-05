# Zenalyst.ai - Dynamic Revenue Analytics Dashboard

[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![OpenAI](https://img.shields.io/badge/OpenAI-412991?style=for-the-badge&logo=openai&logoColor=white)](https://openai.com/)
[![AWS S3](https://img.shields.io/badge/AWS%20S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)

A comprehensive revenue analytics platform built with Streamlit that provides AI-powered insights for investment analysis. Features dynamic dashboard generation, multi-source data integration, and intelligent chatbot assistance.

## 🚀 Key Features

### 🏗️ **Dynamic Multi-Source Architecture**
- **AWS S3 Integration**: Real-time discovery and processing of JSON files from S3 buckets
- **Local File Processing**: Fallback support for local JSON file analysis
- **Multi-tenant Support**: Separate investor and investee dashboards with user management
- **Automatic Schema Detection**: AI-powered analysis of JSON structures for dynamic dashboard configuration

### 📊 **Intelligent Dashboard Generation**
- **Smart Visualization Router**: Automatically selects appropriate charts based on data patterns
- **Pattern Detection Engine**: Recognizes business data types (quarterly, bridge, geographic, customer, monthly)
- **Future-Proof Design**: Adapts to any new JSON structure automatically
- **Interactive Visualizations**: Beautiful waterfall charts, pie charts, line charts, and more using Plotly

### 🤖 **Dual AI Chatbot System**
- **Tab-Specific Expert Analysts**: 
  - Financial Analyst (Quarterly data)
  - Revenue Operations Expert (Bridge analysis)
  - Market Expansion Strategist (Geographic data)
  - Customer Success Executive (Customer concentration)
  - Business Intelligence Analyst (Monthly trends)
- **Universal Sidebar Assistant**: General business and investment questions
- **GPT-4o Integration**: Latest OpenAI model for comprehensive, data-grounded responses
- **Schema-Aware Responses**: Context-specific analysis based on actual data structure

### 🎯 **Specialized Analysis Types**
1. **Quarterly Revenue**: Customer-level Q3/Q4 performance with growth analysis and waterfall charts
2. **Revenue Bridge**: Customer expansion, contraction, and churn dynamics with interactive visualizations
3. **Geographic Analysis**: Country-wise revenue distribution with pie charts and market opportunities
4. **Customer Concentration**: Portfolio risk assessment with concentration metrics and diversification insights
5. **Monthly Trends**: Time-series analysis with seasonal pattern recognition and forecasting

### 👥 **Multi-User Architecture**
- **Investors**: Portfolio analytics across multiple companies with AI-powered insights
- **Investees**: File upload interface with S3 storage and investor connection management
- **Secure Authentication**: Session-based login with password hashing and data isolation

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8+
- OpenAI API Key
- AWS Account (optional, for S3 integration)

### 1. Clone the Repository
```bash
git clone https://github.com/Zenalyst-ai/dynamic_streamlit_demo.git
cd dynamic_streamlit_demo
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Configuration

Create a `.streamlit/secrets.toml` file:
```toml
OPENAI_API_KEY = "your-openai-api-key"

# Optional: AWS S3 Configuration for Investor Analytics
aws_access_key_id = "your-aws-access-key"
aws_secret_access_key = "your-aws-secret-key"
s3_bucket_name = "json-for-streamlit"
s3_region = "eu-north-1"
s3_prefix = "data/"

# Optional: AWS S3 Configuration for Investee File Storage
s3_file_storage_bucket = "your-file-storage-bucket"
s3_file_storage_region = "eu-north-1"
s3_file_storage_prefix = "uploads"

# SSL Configuration (for corporate environments)
aws_ssl_verify = "false"
```

Alternatively, set environment variables:
```bash
export OPENAI_API_KEY="your-openai-api-key"
export AWS_ACCESS_KEY_ID="your-aws-access-key"
export AWS_SECRET_ACCESS_KEY="your-aws-secret-key"
export S3_BUCKET_NAME="json-for-streamlit"
```

### 4. Run the Application
```bash
streamlit run app.py
```

The application will be available at `http://localhost:8501`

## 🎯 Usage Guide

### For Investors
1. **Login** with investor credentials
2. **Portfolio Overview**: Access multiple company analyses automatically
3. **Dynamic Tabs**: Tabs are generated based on available data in S3 bucket
4. **AI Analysis**: Each tab has a specialized AI analyst for comprehensive insights
5. **Interactive Charts**: Explore data with waterfall charts, pie charts, and trend analysis
6. **Universal Assistant**: Use sidebar chatbot for general business questions

### For Investees
1. **Register/Login** with investee credentials
2. **File Upload**: Upload Excel, PDF, or Markdown files directly to S3 storage
3. **File Management**: View, download, or delete uploaded files
4. **Data Processing**: Files are automatically organized and tracked
5. **Investor Access**: Grant specific investors access to your data

### Chatbot System
- **Tab-Specific Experts**: Each analysis tab has a specialized AI analyst with domain expertise
- **Universal Assistant**: Sidebar chatbot for general business and investment questions
- **Smart Suggestions**: Context-aware quick questions tailored to each data type
- **Data-Grounded Responses**: All insights backed by specific data points and metrics

## 🏛️ Architecture Overview

### Core Components

- **`DatabaseManager`**: SQLite operations and user/company management
- **`AuthManager`**: Login/registration functionality with session management
- **`S3ConfigManager`**: AWS S3 configuration and connection management
- **`S3JSONReader`**: Company-specific JSON file discovery and reading from S3
- **`JSONSchemaAnalyzer`**: Automatic schema detection and analysis type inference
- **`DynamicDashboardGenerator`**: Runtime dashboard generation based on detected patterns
- **`OpenAIChatbot`**: Enhanced chatbot with tab-specific expertise and comprehensive responses
- **`CacheManager`**: Performance optimization with TTL-based caching

### Data Flow Architecture

```
User Login → User Type Detection → Data Source Routing
     ↓                              ↓
Investor Path                 Investee Path
     ↓                              ↓
S3 Data Discovery          File Upload Interface
     ↓                              ↓
Schema Analysis            S3 File Storage  
     ↓                              ↓
Dynamic Dashboard          File Management
     ↓
AI-Powered Insights
```

### Database Schema
- `users`: User authentication and type management
- `companies`: Company information for investees
- `investor_companies`: Many-to-many investor-company relationships
- `uploaded_files`: File metadata and S3 key tracking

## 📊 Supported Data Formats

### JSON Structure Support (Auto-Detection)
- **Quarterly Data**: Customer-level revenue with Q3/Q4 comparisons and growth analysis
- **Revenue Bridge**: Expansion, contraction, churn, and new customer revenue data
- **Geographic Data**: Country/region-wise revenue distribution and market analysis
- **Customer Data**: Portfolio concentration and customer performance metrics
- **Monthly Data**: Time-series revenue with variance and seasonal analysis
- **Custom Formats**: Automatic adaptation to any JSON structure with intelligent pattern detection

### File Upload Support (Investees)
- Excel files (.xlsx, .xls) with automatic data type detection
- PDF documents (.pdf) for reports and documentation
- Markdown files (.md) for structured documentation
- Direct S3 storage with comprehensive metadata tracking

## Cloud Deployment

Deploy on Streamlit Cloud:

1. **Fork this repository** to your GitHub account
2. **Go to [share.streamlit.io](https://share.streamlit.io)**
3. **Connect your GitHub account** and select this repository
4. **Configure secrets** in the Streamlit Cloud dashboard:
   ```toml
   OPENAI_API_KEY = "your-openai-api-key"
   aws_access_key_id = "your-aws-access-key"
   aws_secret_access_key = "your-aws-secret-key"
   s3_bucket_name = "json-for-streamlit"
   ```
5. **Deploy** - Your app will be available at `https://yourapp.streamlit.app`

## 🔧 Advanced Configuration

### Two-Bucket S3 Architecture
```python
# Investor Analytics (JSON data reading)
S3_BUCKET_NAME = "json-for-streamlit"           # For processed JSON analytics data
S3_REGION = "eu-north-1"
S3_PREFIX = "data/"

# Investee File Storage (raw file uploads)  
S3_FILE_STORAGE_BUCKET = "file-storage-bucket"  # For raw file uploads
S3_FILE_STORAGE_REGION = "eu-north-1"
S3_FILE_STORAGE_PREFIX = "uploads"
```

### AI Configuration
- **Model**: GPT-4o (latest OpenAI model)
- **Max Tokens**: 3000 for comprehensive responses  
- **Temperature**: 0.4 for balanced creativity and accuracy
- **Response Format**: 2-3 paragraphs with bullet points and specific data references

### Performance Optimization
- **Multi-level caching** with TTL-based invalidation
- **Efficient S3 file discovery** with pattern-based filtering
- **Optimized data processing** pipelines for large datasets

## 📁 Project Structure

```
├── app.py                          # Main Streamlit application (single-file architecture)
├── app_backup.py                   # Backup version
├── requirements.txt                # Python dependencies  
├── packages.txt                    # System packages for deployment
├── CLAUDE.md                       # AI development instructions
├── README.md                       # Project documentation
├── revenue_analytics.db            # SQLite database (auto-created)
├── .streamlit/
│   ├── config.toml                # Streamlit configuration
│   └── secrets.toml               # Local secrets (not committed)
├── .env                           # Environment variables (not committed)
└── zenalyst ai.jpg                # Company logo
```

## 🔒 Security Features

- **Password Hashing**: SHA-256 encryption for user passwords
- **Session Management**: Secure session-based authentication  
- **Data Isolation**: Company-level data separation with foreign keys
- **SSL Support**: Configurable SSL verification for corporate environments
- **API Key Security**: Environment-based API key management

## 🧪 Testing

Test the application by:

1. **Local Testing**: `streamlit run app.py`
2. **User Flow Testing**: Test both investor and investee workflows
3. **Data Upload Testing**: Upload various file formats and sizes
4. **AI Integration Testing**: Verify chatbot responses with valid OpenAI API key
5. **S3 Integration Testing**: Test with configured S3 buckets
6. **Multi-User Testing**: Verify data isolation between different users/companies

## 📦 Dependencies

### Core Dependencies
```
streamlit>=1.28.0          # Web application framework
pandas>=2.0.0              # Data processing and Excel reading
plotly>=5.15.0             # Interactive visualizations
openai>=1.0.0              # GPT-4o API integration
boto3>=1.26.0              # AWS S3 integration
s3fs>=2023.6.0             # S3 filesystem interface
numpy>=1.24.0              # Numerical computations
```

### System Requirements
- Python 3.8+
- SQLite3 (built-in)
- Internet connection for AI features

## 🚀 Technology Stack

- **🎨 Frontend**: Streamlit with custom CSS styling
- **📊 Data Processing**: Pandas, NumPy for data manipulation
- **📈 Visualizations**: Plotly Express & Graph Objects for interactive charts
- **🤖 AI**: OpenAI GPT-4o for advanced natural language processing
- **☁️ Cloud Storage**: AWS S3 for scalable file storage
- **🗄️ Database**: SQLite for user management and metadata
- **🔐 Authentication**: Custom session-based authentication system
- **🚀 Deployment**: Streamlit Cloud, AWS-compatible

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Development Guidelines
- Follow existing code structure and naming conventions
- Add comprehensive docstrings for new functions
- Test both investor and investee user flows
- Ensure AI responses are data-grounded and comprehensive

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support & Contact

- **📧 Email**: support@zenalyst.ai
- **🐛 Issues**: [GitHub Issues](https://github.com/Zenalyst-ai/dynamic_streamlit_demo/issues)
- **📚 Documentation**: [Project Wiki](https://github.com/Zenalyst-ai/dynamic_streamlit_demo/wiki)
- **💬 Discussions**: [GitHub Discussions](https://github.com/Zenalyst-ai/dynamic_streamlit_demo/discussions)

## 🔮 Roadmap

- [ ] **Real-time Data Streaming** from multiple financial data sources
- [ ] **Advanced ML Models** for predictive revenue analytics and forecasting
- [ ] **Mobile-Responsive Design** for on-the-go analysis
- [ ] **API Integration** with popular business intelligence and CRM tools
- [ ] **Multi-Language Support** for international investment markets
- [ ] **Advanced User Permissions** with role-based access control
- [ ] **Custom Dashboard Builder** with drag-and-drop interface
- [ ] **White-Label Solutions** for investment firms and consultancies

---

**🚀 Built with ❤️ by the Zenalyst.ai Team**

*Transforming investment analysis with AI-powered insights and dynamic data visualization*
