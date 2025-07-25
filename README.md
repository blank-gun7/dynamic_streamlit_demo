# Zenalyst.ai Revenue Analytics Dashboard

A comprehensive Streamlit-based revenue analytics dashboard with AI-powered insights, chatbot functionality, and support for both JSON and Excel data formats.

## Features

- **Multiple Analytics Views**:
  - Quarterly Revenue & QoQ Growth Analysis
  - Revenue Bridge & Churn Analysis  
  - Country-wise Revenue Analysis
  - Customer Concentration Analysis
  - Month-on-Month Revenue Analysis

- **AI-Powered Features**:
  - Executive summaries for each view using GPT-4o
  - Interactive chatbot for data-specific questions
  - Context-aware responses based on selected dataset

- **Data Sources**:
  - Original JSON files (included in repository)
  - Excel file upload with automatic validation
  - Real-time data processing and visualization

## Local Development

1. **Clone the repository**:
   ```bash
   git clone https://github.com/blank-gun7/streamlit_demo.git
   cd streamlit_demo
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env and add your OpenAI API key
   ```

4. **Run the application**:
   ```bash
   streamlit run app.py
   ```

## Cloud Deployment

This app is deployed on Streamlit Cloud. To deploy your own version:

1. **Fork this repository** to your GitHub account

2. **Go to [share.streamlit.io](https://share.streamlit.io)**

3. **Connect your GitHub account** and select this repository

4. **Configure secrets** in the Streamlit Cloud dashboard:
   - Add `OPENAI_API_KEY` with your OpenAI API key

5. **Deploy** - Your app will be available at `https://yourapp.streamlit.app`

## Configuration

### Environment Variables

- `OPENAI_API_KEY`: Your OpenAI API key for AI features

### Streamlit Secrets (for cloud deployment)

Add the following to your Streamlit Cloud secrets:

```toml
OPENAI_API_KEY = "your_openai_api_key_here"
```

## File Structure

```
├── app.py                          # Main Streamlit application
├── requirements.txt                # Python dependencies
├── .env.example                   # Environment variables template
├── .gitignore                     # Git ignore rules
├── .streamlit/
│   ├── config.toml               # Streamlit configuration
│   └── secrets.toml              # Local secrets (not committed)
├── zenalyst ai.jpg               # Company logo
└── *.json                        # Sample revenue data files
```

## Data Format

The application supports:

1. **JSON files** with revenue analytics data
2. **Excel files** (.xlsx, .xls) with similar structure
3. **Auto-detection** of data types based on content and filenames

## AI Features

- **Executive Summaries**: AI-generated insights for each analytics view
- **Interactive Chat**: Ask questions about your data with context-aware responses
- **View-Specific Analysis**: Each view has tailored prompts for relevant insights

## Technology Stack

- **Frontend**: Streamlit
- **Data Processing**: Pandas, NumPy
- **Visualizations**: Plotly Express & Graph Objects
- **AI**: OpenAI GPT-4o
- **Deployment**: Streamlit Cloud

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally
5. Submit a pull request

## License

This project is open source and available under the MIT License.

## Support

For issues or questions, please open an issue on GitHub or contact support@zenalyst.ai.
