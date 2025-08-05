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
from openai import OpenAI
import os
import boto3
import s3fs
from botocore.exceptions import NoCredentialsError, ClientError

class S3ConfigManager:
    """Manage S3 configuration and connection"""
    
    def __init__(self):
        self.aws_access_key = self._get_config("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = self._get_config("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = self._get_config("S3_BUCKET_NAME")
        self.region = self._get_config("S3_REGION", "us-east-1")
        self.prefix = self._get_config("S3_PREFIX", "")
        self.refresh_interval = int(self._get_config("DATA_REFRESH_INTERVAL", "60"))
    
    def _get_config(self, key, default=""):
        """Get configuration from environment or Streamlit secrets"""
        # Try environment first
        value = os.getenv(key)
        if value:
            return value
        
        # Try Streamlit secrets
        try:
            return st.secrets.get(key.lower(), default)
        except:
            return default
    
    def is_configured(self):
        """Check if S3 is properly configured"""
        return bool(self.aws_access_key and self.aws_secret_key and self.bucket_name)
    
    def get_s3_client(self):
        """Get configured S3 client with SSL handling"""
        if not self.is_configured():
            return None
        
        try:
            # Check if SSL verification should be disabled
            ssl_verify = self._get_config("AWS_SSL_VERIFY", "true").lower() != "false"
            
            import botocore.config
            
            # Configure with SSL settings
            config = botocore.config.Config(
                region_name=self.region,
                retries={'max_attempts': 3, 'mode': 'adaptive'},
                max_pool_connections=50
            )
            
            # Add SSL verification setting
            if not ssl_verify:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                config = botocore.config.Config(
                    region_name=self.region,
                    retries={'max_attempts': 3, 'mode': 'adaptive'},
                    max_pool_connections=50
                )
            
            return boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.region,
                config=config,
                verify=ssl_verify
            )
        except Exception as e:
            st.error(f"Failed to connect to S3: {str(e)}")
            st.info("💡 If you're behind a corporate firewall, try setting AWS_SSL_VERIFY=false in your environment")
            return None
    
    def get_s3_fs(self):
        """Get configured S3 filesystem with SSL handling"""
        if not self.is_configured():
            return None
        
        try:
            # Check if SSL verification should be disabled
            ssl_verify = self._get_config("AWS_SSL_VERIFY", "true").lower() != "false"
            
            client_kwargs = {
                'region_name': self.region
            }
            
            # Add SSL verification setting to client kwargs
            if not ssl_verify:
                client_kwargs['verify'] = False
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            return s3fs.S3FileSystem(
                key=self.aws_access_key,
                secret=self.aws_secret_key,
                client_kwargs=client_kwargs
            )
        except Exception as e:
            st.error(f"Failed to connect to S3 filesystem: {str(e)}")
            st.info("💡 If you're behind a corporate firewall, try setting AWS_SSL_VERIFY=false in your environment")
            return None

class S3DataDiscovery:
    """Discover and manage JSON files from S3 bucket"""
    
    def __init__(self, s3_config):
        self.config = s3_config
        self.s3_client = s3_config.get_s3_client()
        self.s3_fs = s3_config.get_s3_fs()
        self._file_cache = {}
        self._last_scan = None
    
    def discover_json_files(self, force_refresh=False):
        """Discover all JSON files in the S3 bucket"""
        if not self.s3_client:
            return {}
        
        # Check cache first
        if not force_refresh and self._file_cache and self._last_scan:
            cache_age = (datetime.now() - self._last_scan).total_seconds() / 60
            if cache_age < self.config.refresh_interval:
                return self._file_cache
        
        discovered_files = {}
        
        try:
            # List objects in the bucket
            paginator = self.s3_client.get_paginator('list_objects_v2')
            prefix = self.config.prefix if self.config.prefix else ""
            
            for page in paginator.paginate(Bucket=self.config.bucket_name, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    # Filter for JSON files
                    if key.lower().endswith('.json') and not key.endswith('/'):
                        file_info = {
                            'key': key,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag'].strip('"'),
                            'file_name': key.split('/')[-1],
                            'folder_path': '/'.join(key.split('/')[:-1]) if '/' in key else ''
                        }
                        discovered_files[key] = file_info
            
            self._file_cache = discovered_files
            self._last_scan = datetime.now()
            
        except Exception as e:
            ErrorHandler.handle_s3_error(e, "S3 file discovery")
            return {}
        
        return discovered_files
    
    def load_json_from_s3(self, file_key):
        """Load JSON data from S3 file with caching"""
        if not self.s3_fs:
            return None
        
        # Check cache first
        cache_key = cache_manager.get_cache_key(file_key, "s3_data")
        cached_data = cache_manager.get_analysis_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            file_path = f"{self.config.bucket_name}/{file_key}"
            with self.s3_fs.open(file_path, 'r') as f:
                data = json.load(f)
            
            # Cache the loaded data
            cache_manager.set_analysis_cache(cache_key, data)
            return data
            
        except Exception as e:
            ErrorHandler.handle_s3_error(e, f"Loading {file_key}")
            return None
    
    def get_file_categories(self, discovered_files):
        """Categorize files based on naming patterns and folder structure"""
        categories = {}
        
        for key, file_info in discovered_files.items():
            # Extract category from filename or folder
            file_name = file_info['file_name'].lower()
            folder_path = file_info['folder_path'].lower()
            
            # Default category based on folder or filename
            category = "general"
            
            # Pattern-based categorization
            if any(term in file_name for term in ['quarterly', 'qoq', 'quarter']):
                category = "quarterly"
            elif any(term in file_name for term in ['bridge', 'churn', 'retention']):
                category = "bridge"
            elif any(term in file_name for term in ['country', 'geographic', 'region']):
                category = "geographic"
            elif any(term in file_name for term in ['customer', 'concentration', 'client']):
                category = "customer"
            elif any(term in file_name for term in ['monthly', 'month', 'mom']):
                category = "monthly"
            elif folder_path:
                # Use folder name as category if no pattern matches
                category = folder_path.split('/')[-1] or "general"
            
            if category not in categories:
                categories[category] = []
            categories[category].append({**file_info, 'original_key': key})
        
        return categories

class S3JSONReader:
    """Read JSON data for companies directly from S3 bucket for investor dashboard"""
    
    def __init__(self, s3_config):
        self.config = s3_config
        self.s3_client = s3_config.get_s3_client()
        self.data_discovery = S3DataDiscovery(s3_config)
        self._data_cache = {}
        self._last_cache_time = None
    
    def get_company_data_from_s3(self, company_name):
        """Get all JSON data for a specific company from S3"""
        if not self.s3_client:
            return {}
        
        # Check cache first
        cache_key = f"company_{company_name}"
        if self._is_cache_valid(cache_key):
            return self._data_cache.get(cache_key, {})
        
        company_data = {}
        
        try:
            # Discover all JSON files
            discovered_files = self.data_discovery.discover_json_files()
            
            # Filter files that belong to this company (by folder structure or naming)
            company_files = self._filter_company_files(discovered_files, company_name)
            
            # Read and categorize the data
            for file_key, file_info in company_files.items():
                data = self.data_discovery.load_json_from_s3(file_key)
                if data:
                    # Determine data type based on filename/folder
                    data_type = self._determine_data_type(file_info)
                    company_data[data_type] = data
            
            # Cache the result
            self._data_cache[cache_key] = company_data
            self._last_cache_time = datetime.now()
            
            return company_data
            
        except Exception as e:
            ErrorHandler.handle_s3_error(e, f"Loading company data for {company_name}")
            return {}
    
    def _filter_company_files(self, discovered_files, company_name):
        """Filter files that belong to a specific company"""
        company_files = {}
        company_name_lower = company_name.lower().replace(' ', '_').replace('-', '_')
        
        for file_key, file_info in discovered_files.items():
            # Check if company name is in folder path or filename
            folder_path = file_info.get('folder_path', '').lower()
            file_name = file_info.get('file_name', '').lower()
            
            # Multiple matching strategies
            if (company_name_lower in folder_path or 
                company_name_lower in file_name or
                any(part in folder_path for part in company_name_lower.split('_')) or
                # For generic files without company names, include all
                ('company' not in folder_path and 'client' not in folder_path)):
                company_files[file_key] = file_info
        
        return company_files
    
    def _determine_data_type(self, file_info):
        """Determine data type based on file information"""
        file_name = file_info.get('file_name', '').lower()
        folder_path = file_info.get('folder_path', '').lower()
        
        # Map file patterns to data types (matching existing categories)
        if any(term in file_name for term in ['quarterly', 'qoq', 'quarter']):
            return 'quarterly_revenue'
        elif any(term in file_name for term in ['bridge', 'churn', 'retention']):
            return 'revenue_bridge'
        elif any(term in file_name for term in ['country', 'geographic', 'region']):
            return 'country_wise'
        elif any(term in file_name for term in ['customer', 'concentration', 'client']):
            return 'customer_concentration'
        elif any(term in file_name for term in ['monthly', 'month', 'mom']):
            return 'monthly_revenue'
        else:
            # Use filename without extension as type
            return file_info.get('file_name', 'general').replace('.json', '').lower()
    
    def _is_cache_valid(self, cache_key):
        """Check if cached data is still valid"""
        if (cache_key not in self._data_cache or 
            not self._last_cache_time or
            (datetime.now() - self._last_cache_time).total_seconds() > self.config.refresh_interval * 60):
            return False
        return True
    
    def get_available_companies(self):
        """Get list of companies that have data in S3"""
        try:
            discovered_files = self.data_discovery.discover_json_files()
            companies = set()
            
            for file_key, file_info in discovered_files.items():
                folder_path = file_info.get('folder_path', '')
                if folder_path:
                    # Extract company name from folder structure
                    # Assuming structure like: data/company_name/file.json
                    path_parts = folder_path.strip('/').split('/')
                    if len(path_parts) > 0:
                        company_candidate = path_parts[-1].replace('_', ' ').title()
                        companies.add(company_candidate)
            
            return list(companies)
            
        except Exception as e:
            ErrorHandler.handle_s3_error(e, "Getting available companies from S3")
            return []

class S3FileStorageManager:
    """Manage file uploads to S3 bucket for investee file storage"""
    
    def __init__(self):
        self.aws_access_key = self._get_config("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = self._get_config("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = self._get_config("S3_FILE_STORAGE_BUCKET")
        self.region = self._get_config("S3_FILE_STORAGE_REGION", "us-east-1")
        self.prefix = self._get_config("S3_FILE_STORAGE_PREFIX", "uploads")
        
    def _get_config(self, key, default=""):
        """Get configuration from environment or Streamlit secrets"""
        # Try environment first
        value = os.getenv(key)
        if value:
            return value
        
        # Try Streamlit secrets
        try:
            return st.secrets.get(key.lower(), default)
        except:
            return default
    
    def is_configured(self):
        """Check if S3 file storage is properly configured"""
        return bool(self.aws_access_key and self.aws_secret_key and self.bucket_name)
    
    def get_s3_client(self):
        """Get configured S3 client for file storage"""
        if not self.is_configured():
            return None
        
        try:
            return boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.region
            )
        except Exception as e:
            st.error(f"Failed to connect to S3 file storage: {str(e)}")
            return None
    
    def generate_file_key(self, company_id, filename):
        """Generate organized S3 key for file storage"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d")
        
        # Clean filename to remove any problematic characters
        clean_filename = filename.replace(" ", "_").replace("(", "").replace(")", "")
        
        # Create organized path: prefix/company_id/date/filename
        if self.prefix:
            return f"{self.prefix}/company_{company_id}/{timestamp}/{clean_filename}"
        else:
            return f"company_{company_id}/{timestamp}/{clean_filename}"
    
    def upload_file(self, file_obj, company_id, filename):
        """Upload a file to S3 and return the S3 key"""
        s3_client = self.get_s3_client()
        if not s3_client:
            raise Exception("S3 file storage not configured")
        
        # Generate unique S3 key
        s3_key = self.generate_file_key(company_id, filename)
        
        try:
            # Reset file pointer to beginning
            file_obj.seek(0)
            
            # Upload file to S3
            s3_client.upload_fileobj(
                file_obj,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ServerSideEncryption': 'AES256'}
            )
            
            return s3_key
            
        except Exception as e:
            raise Exception(f"Failed to upload file to S3: {str(e)}")
    
    def get_file_url(self, s3_key, expiration=3600):
        """Generate presigned URL for file download"""
        s3_client = self.get_s3_client()
        if not s3_client:
            return None
        
        try:
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            st.error(f"Failed to generate download URL: {str(e)}")
            return None
    
    def download_file_content(self, s3_key):
        """Download file content directly for Streamlit download button"""
        s3_client = self.get_s3_client()
        if not s3_client:
            return None
        
        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read()
        except Exception as e:
            st.error(f"Failed to download file: {str(e)}")
            return None
    
    def delete_file(self, s3_key):
        """Delete a file from S3"""
        s3_client = self.get_s3_client()
        if not s3_client:
            return False
        
        try:
            s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except Exception as e:
            st.error(f"Failed to delete file from S3: {str(e)}")
            return False

class JSONSchemaAnalyzer:
    """Analyze JSON structure and infer appropriate dashboard components"""
    
    def __init__(self):
        self.schema_cache = {}
    
    def analyze_json_schema(self, json_data, data_key=None):
        """Analyze JSON data structure and infer schema"""
        if not json_data:
            return None
        
        # Use global cache manager
        cache_key = cache_manager.get_cache_key(json_data, data_key)
        cached_schema = cache_manager.get_schema_cache(cache_key)
        if cached_schema:
            return cached_schema
        
        schema = {
            'data_type': self._detect_data_type(json_data),
            'structure': self._analyze_structure(json_data),
            'columns': self._analyze_columns(json_data),
            'metrics': self._identify_metrics(json_data),
            'suggested_visualizations': [],
            'confidence_score': 0
        }
        
        # Add visualization suggestions
        schema['suggested_visualizations'] = self._suggest_visualizations(schema)
        schema['confidence_score'] = self._calculate_confidence(schema)
        
        # Cache the result
        cache_manager.set_schema_cache(cache_key, schema)
        return schema
    
    def _detect_data_type(self, json_data):
        """Detect the primary data type/analysis type"""
        if not isinstance(json_data, list) or not json_data:
            return "unknown"
        
        # Sample the first few records for analysis
        sample_size = min(5, len(json_data))
        sample_data = json_data[:sample_size]
        
        # Convert to DataFrame for easier analysis
        try:
            df = pd.DataFrame(sample_data)
            columns = [col.lower() for col in df.columns]
            
            # Pattern matching for data types
            if any(term in ' '.join(columns) for term in ['quarter', 'q3', 'q4', 'qoq']):
                return "quarterly"
            elif any(term in ' '.join(columns) for term in ['churn', 'expansion', 'bridge']):
                return "bridge"
            elif any(term in ' '.join(columns) for term in ['country', 'region', 'geographic']):
                return "geographic"
            elif any(term in ' '.join(columns) for term in ['customer', 'client', 'concentration']):
                return "customer"
            elif any(term in ' '.join(columns) for term in ['month', 'monthly', 'mom']):
                return "monthly"
            else:
                return "general"
                
        except Exception:
            return "unknown"
    
    def _analyze_structure(self, json_data):
        """Analyze the structure of the JSON data"""
        if not isinstance(json_data, list):
            return {"type": "single_object", "count": 1}
        
        return {
            "type": "array_of_objects",
            "count": len(json_data),
            "sample_keys": list(json_data[0].keys()) if json_data else []
        }
    
    def _analyze_columns(self, json_data):
        """Analyze column types and characteristics"""
        if not isinstance(json_data, list) or not json_data:
            return {}
        
        try:
            df = pd.DataFrame(json_data)
            column_analysis = {}
            
            for col in df.columns:
                col_data = df[col].dropna()
                if col_data.empty:
                    continue
                
                analysis = {
                    'name': col,
                    'data_type': str(col_data.dtype),
                    'non_null_count': len(col_data),
                    'null_count': df[col].isnull().sum(),
                    'unique_count': col_data.nunique(),
                    'is_numeric': pd.api.types.is_numeric_dtype(col_data),
                    'is_datetime': pd.api.types.is_datetime64_any_dtype(col_data),
                    'sample_values': col_data.head(3).tolist()
                }
                
                # Additional analysis for numeric columns
                if analysis['is_numeric']:
                    analysis.update({
                        'min_value': float(col_data.min()),
                        'max_value': float(col_data.max()),
                        'mean_value': float(col_data.mean()),
                        'has_negative': (col_data < 0).any()
                    })
                
                column_analysis[col] = analysis
            
            return column_analysis
            
        except Exception as e:
            return {}
    
    def _identify_metrics(self, json_data):
        """Identify key metrics and KPIs from the data"""
        columns = self._analyze_columns(json_data)
        metrics = {
            'revenue_columns': [],
            'date_columns': [],
            'categorical_columns': [],
            'percentage_columns': [],
            'id_columns': []
        }
        
        for col, analysis in columns.items():
            col_lower = col.lower()
            
            # Revenue-related columns
            if any(term in col_lower for term in ['revenue', 'amount', 'value', 'price', 'cost']):
                if analysis['is_numeric']:
                    metrics['revenue_columns'].append(col)
            
            # Date columns
            elif any(term in col_lower for term in ['date', 'month', 'quarter', 'year', 'time']):
                metrics['date_columns'].append(col)
            
            # Percentage columns
            elif any(term in col_lower for term in ['percent', '%', 'rate', 'ratio']):
                metrics['percentage_columns'].append(col)
            
            # ID columns
            elif any(term in col_lower for term in ['id', 'name', 'customer', 'client']):
                metrics['id_columns'].append(col)
            
            # Categorical columns (non-numeric with reasonable unique count)
            elif not analysis['is_numeric'] and analysis['unique_count'] < len(json_data) * 0.5:
                metrics['categorical_columns'].append(col)
        
        return metrics
    
    def _suggest_visualizations(self, schema):
        """Suggest appropriate visualizations based on schema analysis"""
        suggestions = []
        metrics = schema.get('metrics', {})
        data_type = schema.get('data_type', 'general')
        
        # Revenue-focused visualizations
        if metrics.get('revenue_columns'):
            suggestions.extend(['bar_chart', 'line_chart', 'metric_cards'])
        
        # Geographic data
        if data_type == 'geographic':
            suggestions.extend(['pie_chart', 'treemap', 'bar_chart'])
        
        # Time series data
        if metrics.get('date_columns'):
            suggestions.extend(['line_chart', 'area_chart'])
        
        # Customer concentration data
        if data_type == 'customer' or metrics.get('id_columns'):
            suggestions.extend(['treemap', 'pareto_chart', 'concentration_analysis'])
        
        # Bridge/churn data
        if data_type == 'bridge':
            suggestions.extend(['waterfall_chart', 'sankey_diagram'])
        
        # Default visualizations
        if not suggestions:
            suggestions = ['table', 'bar_chart', 'summary_metrics']
        
        return suggestions
    
    def _calculate_confidence(self, schema):
        """Calculate confidence score for schema analysis"""
        score = 0
        
        # Data type detection confidence
        if schema.get('data_type') != 'unknown':
            score += 30
        
        # Column analysis confidence
        columns = schema.get('columns', {})
        if columns:
            score += min(20, len(columns) * 2)
        
        # Metrics identification confidence
        metrics = schema.get('metrics', {})
        total_metrics = sum(len(v) for v in metrics.values())
        score += min(30, total_metrics * 5)
        
        # Visualization suggestions confidence
        if schema.get('suggested_visualizations'):
            score += 20
        
        return min(100, score)

class CacheManager:
    """Simple caching system for performance optimization"""
    
    def __init__(self):
        self.analysis_cache = {}
        self.visualization_cache = {}
        self.schema_cache = {}
        self.cache_ttl = 300  # 5 minutes TTL
    
    def get_cache_key(self, data, *args):
        """Generate cache key from data and arguments"""
        import hashlib
        
        # Create a hash from data content and arguments
        if isinstance(data, list) and data:
            content_hash = hashlib.md5(str(len(data)).encode() + str(data[0]).encode()).hexdigest()[:8]
        else:
            content_hash = hashlib.md5(str(data).encode()).hexdigest()[:8]
        
        args_hash = hashlib.md5(str(args).encode()).hexdigest()[:8]
        return f"{content_hash}_{args_hash}"
    
    def get_analysis_cache(self, cache_key):
        """Get cached analysis result"""
        if cache_key in self.analysis_cache:
            cached_item = self.analysis_cache[cache_key]
            # Check TTL
            if (datetime.now() - cached_item['timestamp']).total_seconds() < self.cache_ttl:
                return cached_item['data']
            else:
                # Remove expired cache
                del self.analysis_cache[cache_key]
        return None
    
    def set_analysis_cache(self, cache_key, data):
        """Set analysis cache"""
        self.analysis_cache[cache_key] = {
            'data': data,
            'timestamp': datetime.now()
        }
    
    def get_schema_cache(self, cache_key):
        """Get cached schema analysis"""
        if cache_key in self.schema_cache:
            cached_item = self.schema_cache[cache_key]
            if (datetime.now() - cached_item['timestamp']).total_seconds() < self.cache_ttl:
                return cached_item['data']
            else:
                del self.schema_cache[cache_key]
        return None
    
    def set_schema_cache(self, cache_key, data):
        """Set schema cache"""
        self.schema_cache[cache_key] = {
            'data': data,
            'timestamp': datetime.now()
        }
    
    def clear_cache(self):
        """Clear all caches"""
        self.analysis_cache.clear()
        self.visualization_cache.clear()
        self.schema_cache.clear()

# Global cache manager
cache_manager = CacheManager()

class ErrorHandler:
    """Centralized error handling and logging"""
    
    @staticmethod
    def handle_s3_error(error, context="S3 operation"):
        """Handle S3-specific errors with user-friendly messages"""
        error_msg = str(error)
        
        if "NoCredentialsError" in error_msg:
            st.error("❌ AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
            st.info("💡 You can set these in environment variables or Streamlit secrets.")
            return "credentials_error"
        elif "AccessDenied" in error_msg:
            st.error("❌ Access denied to S3 bucket. Please check your AWS permissions.")
            return "access_denied"
        elif "NoSuchBucket" in error_msg:
            st.error("❌ S3 bucket not found. Please check the bucket name in your configuration.")
            return "bucket_not_found"
        elif "ConnectionError" in error_msg or "timeout" in error_msg.lower():
            st.warning("⚠️ Network connection issue. Falling back to local data.")
            return "network_error"
        else:
            st.error(f"❌ {context} failed: {error_msg}")
            return "unknown_error"
    
    @staticmethod
    def handle_data_error(error, context="Data processing"):
        """Handle data processing errors"""
        error_msg = str(error)
        
        if "json" in error_msg.lower():
            st.error("❌ Invalid JSON format in data file.")
            st.info("💡 Please check the JSON file structure.")
        elif "pandas" in error_msg.lower() or "dataframe" in error_msg.lower():
            st.error("❌ Data format error. Unable to process data into table format.")
        else:
            st.error(f"❌ {context}: {error_msg}")
        
        return "data_error"
    
    @staticmethod
    def handle_ai_error(error, context="AI processing"):
        """Handle AI/OpenAI API errors"""
        error_msg = str(error)
        
        if "api_key" in error_msg.lower() or "authentication" in error_msg.lower():
            st.warning("⚠️ OpenAI API key not configured or invalid. Using fallback analysis.")
        elif "rate_limit" in error_msg.lower():
            st.warning("⚠️ OpenAI API rate limit reached. Please try again later.")
        elif "quota" in error_msg.lower():
            st.warning("⚠️ OpenAI API quota exceeded. Using fallback analysis.")
        else:
            st.warning(f"⚠️ AI service temporarily unavailable: {error_msg}")
        
        return "ai_error"

class ConfigValidator:
    """Validate configuration and provide setup guidance"""
    
    @staticmethod
    def validate_s3_config(s3_config):
        """Validate S3 configuration and provide guidance"""
        validation_results = {
            'is_valid': False,
            'missing_configs': [],
            'warnings': [],
            'recommendations': []
        }
        
        # Check required configurations
        if not s3_config.aws_access_key:
            validation_results['missing_configs'].append('AWS_ACCESS_KEY_ID')
        
        if not s3_config.aws_secret_key:
            validation_results['missing_configs'].append('AWS_SECRET_ACCESS_KEY')
        
        if not s3_config.bucket_name:
            validation_results['missing_configs'].append('S3_BUCKET_NAME')
        
        # Test connection if all configs present
        if not validation_results['missing_configs']:
            try:
                s3_client = s3_config.get_s3_client()
                if s3_client:
                    # Test bucket access
                    s3_client.head_bucket(Bucket=s3_config.bucket_name)
                    validation_results['is_valid'] = True
                else:
                    validation_results['warnings'].append('Unable to create S3 client')
            except Exception as e:
                validation_results['warnings'].append(f'S3 connection test failed: {str(e)}')
        
        # Add recommendations
        if validation_results['missing_configs']:
            validation_results['recommendations'].append(
                "Set missing environment variables or add them to Streamlit secrets"
            )
        
        if not validation_results['is_valid'] and not validation_results['missing_configs']:
            validation_results['recommendations'].append(
                "Check AWS credentials and bucket permissions"
            )
        
        return validation_results
    
    @staticmethod
    def show_config_status(s3_config):
        """Display configuration status to user"""
        st.subheader("🔧 Configuration Status")
        
        validation = ConfigValidator.validate_s3_config(s3_config)
        
        # S3 Configuration Status
        with st.expander("AWS S3 Configuration", expanded=not validation['is_valid']):
            if validation['is_valid']:
                st.success("✅ S3 configuration is valid and connected")
                st.info(f"📊 Bucket: {s3_config.bucket_name}")
                st.info(f"🌍 Region: {s3_config.region}")
                if s3_config.prefix:
                    st.info(f"📁 Prefix: {s3_config.prefix}")
            else:
                if validation['missing_configs']:
                    st.error(f"❌ Missing configurations: {', '.join(validation['missing_configs'])}")
                
                if validation['warnings']:
                    for warning in validation['warnings']:
                        st.warning(f"⚠️ {warning}")
                
                if validation['recommendations']:
                    st.info("💡 Recommendations:")
                    for rec in validation['recommendations']:
                        st.info(f"   • {rec}")
                
                # Show setup instructions
                st.markdown("**Setup Instructions:**")
                st.code("""
# Environment Variables
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export S3_BUCKET_NAME="your_bucket_name"
export S3_REGION="us-east-1"  # optional
export S3_PREFIX="data/"      # optional
                """)
                
                st.markdown("**Or add to Streamlit secrets:**")
                st.code("""
# .streamlit/secrets.toml
aws_access_key_id = "your_access_key"
aws_secret_access_key = "your_secret_key"
s3_bucket_name = "your_bucket_name"
s3_region = "us-east-1"
s3_prefix = "data/"
                """)

class DynamicDashboardGenerator:
    """Generate dashboard components dynamically based on schema analysis"""
    
    def __init__(self):
        self.schema_analyzer = JSONSchemaAnalyzer()
    
    def generate_tab_layout(self, tab_name, json_data, analysis_type=None, schema=None):
        """Generate a complete tab layout based on data and schema"""
        
        # Analyze schema if not provided
        if not schema:
            schema = self.schema_analyzer.analyze_json_schema(json_data, tab_name)
        
        # Use provided analysis type or detect from schema
        if not analysis_type:
            analysis_type = schema.get('data_type', 'general') if schema else 'general'
        
        # Generate header
        st.header(f"📊 {tab_name}")
        
        # Generate dynamic executive summary
        executive_summary = generate_adaptive_executive_summary(json_data, schema, tab_name)
        with st.expander("📋 Executive Summary", expanded=True):
            st.markdown(executive_summary)
        
        # Generate metrics cards
        self._generate_metrics_section(json_data, schema)
        
        # Generate visualizations
        self._generate_visualizations(json_data, schema, analysis_type)
        
        # Generate data table
        self._generate_data_table(json_data, schema)
        
        # Add AI Chatbot with schema awareness
        st.markdown("---")
        display_chatbot_with_schema(json_data, tab_name, schema)
    
    def _show_schema_info(self, schema, json_data):
        """Show schema detection confidence and data info"""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            confidence = schema.get('confidence_score', 0)
            color = "green" if confidence > 80 else "orange" if confidence > 60 else "red"
            st.metric("Schema Confidence", f"{confidence}%")
        
        with col2:
            data_count = len(json_data) if isinstance(json_data, list) else 1
            st.metric("Data Records", f"{data_count:,}")
        
        with col3:
            columns_count = len(schema.get('columns', {}))
            st.metric("Data Columns", columns_count)
        
        with col4:
            data_type = schema.get('data_type', 'Unknown').title()
            st.metric("Detected Type", data_type)
    
    def _generate_metrics_section(self, json_data, schema):
        """Generate key metrics based on schema analysis"""
        if not schema or not isinstance(json_data, list) or not json_data:
            return
        
        st.subheader("📊 Key Metrics")
        
        metrics = schema.get('metrics', {})
        columns = schema.get('columns', {})
        
        # Generate metrics based on detected revenue columns
        revenue_cols = metrics.get('revenue_columns', [])
        if revenue_cols:
            self._generate_revenue_metrics(json_data, revenue_cols)
        
        # Generate metrics for other data types
        self._generate_general_metrics(json_data, metrics, columns)
    
    def _generate_revenue_metrics(self, json_data, revenue_cols):
        """Generate revenue-specific metrics"""
        df = pd.DataFrame(json_data)
        
        cols = st.columns(min(len(revenue_cols), 4))
        
        for i, col_name in enumerate(revenue_cols[:4]):
            if col_name in df.columns:
                with cols[i]:
                    total = df[col_name].sum()
                    avg = df[col_name].mean()
                    st.metric(
                        f"Total {col_name.replace('_', ' ').title()}", 
                        f"${total:,.2f}",
                        f"Avg: ${avg:,.2f}"
                    )
    
    def _generate_general_metrics(self, json_data, metrics, columns):
        """Generate general metrics for any data type"""
        df = pd.DataFrame(json_data)
        
        # Count metrics
        id_cols = metrics.get('id_columns', [])
        categorical_cols = metrics.get('categorical_columns', [])
        
        if id_cols or categorical_cols:
            metric_cols = st.columns(4)
            
            # Unique entities
            if id_cols:
                with metric_cols[0]:
                    unique_count = df[id_cols[0]].nunique() if id_cols[0] in df.columns else 0
                    st.metric(f"Unique {id_cols[0].replace('_', ' ').title()}", unique_count)
            
            # Categorical analysis
            if categorical_cols:
                with metric_cols[1]:
                    cat_col = categorical_cols[0]
                    if cat_col in df.columns:
                        cat_count = df[cat_col].nunique()
                        st.metric(f"Unique {cat_col.replace('_', ' ').title()}", cat_count)
    
    def _generate_visualizations(self, json_data, schema, analysis_type):
        """Smart visualization router - generates appropriate charts based on data patterns"""
        if not isinstance(json_data, list) or not json_data:
            return
        
        st.subheader("📈 Data Visualizations")
        
        df = pd.DataFrame(json_data)
        
        # Detect data patterns and route to appropriate visualization functions
        data_pattern = self._detect_data_pattern(df, analysis_type)
        
        if data_pattern == 'revenue_bridge':
            self._generate_revenue_bridge_visualizations(df)
        elif data_pattern == 'customer_analysis':
            self._generate_customer_analysis_visualizations(df)
        elif data_pattern == 'geographic':
            self._generate_geographic_visualizations(df)
        elif data_pattern == 'quarterly':
            self._generate_quarterly_visualizations(df)
        elif data_pattern == 'monthly_trends':
            self._generate_monthly_trends_visualizations(df)
        else:
            # Default fallback with pattern-based visualizations
            self._generate_default_visualizations(df, schema)
    
    def _detect_data_pattern(self, df, analysis_type):
        """Detect the type of business data based on column names and analysis type"""
        columns = [col.lower() for col in df.columns]
        
        # Bridge/Churn analysis pattern
        bridge_terms = ['expansion', 'contraction', 'churn', 'new', 'bridge', 'starting', 'ending']
        if any(term in ' '.join(columns) for term in bridge_terms):
            return 'revenue_bridge'
        
        # Customer analysis pattern  
        customer_terms = ['customer', 'client', 'company']
        revenue_terms = ['revenue', 'amount', 'value']
        if (any(term in ' '.join(columns) for term in customer_terms) and 
            any(term in ' '.join(columns) for term in revenue_terms)):
            return 'customer_analysis'
        
        # Geographic analysis pattern
        geo_terms = ['country', 'region', 'geographic', 'location']
        if any(term in ' '.join(columns) for term in geo_terms):
            return 'geographic'
        
        # Quarterly analysis pattern
        quarterly_terms = ['q3', 'q4', 'quarter', 'qoq']
        if any(term in ' '.join(columns) for term in quarterly_terms):
            return 'quarterly'
        
        # Monthly trends pattern - enhanced detection
        monthly_terms = ['month', 'monthly', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 
                        'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        
        # Check for standard monthly column patterns
        if any(term in ' '.join(columns) for term in monthly_terms):
            return 'monthly_trends'
        
        # Check for month_label + revenue pattern (common monthly data structure)  
        if 'month_label' in ' '.join(columns) and any(term in ' '.join(columns) for term in ['revenue', 'amount', 'value']):
            return 'monthly_trends'
        
        # Check for variance pattern (monthly variance analysis)
        if 'variance' in ' '.join(columns) and any(term in ' '.join(columns) for term in ['revenue', 'amount']):
            return 'monthly_trends'
        
        # Check for time-series patterns that could be monthly
        time_terms = ['date', 'time', 'period']
        if (any(term in ' '.join(columns) for term in time_terms) and 
            any(term in ' '.join(columns) for term in ['revenue', 'amount', 'value'])):
            # Additional check: if we have relatively few rows (typically monthly data has 12 rows), likely monthly
            if len(df) <= 24:  # Up to 2 years of monthly data
                return 'monthly_trends'
        
        return 'default'
    
    def _generate_revenue_bridge_visualizations(self, df):
        """Generate visualizations specifically for revenue bridge analysis"""
        st.write("### 🌊 Revenue Bridge Analysis")
        
        # Look for bridge-specific columns
        bridge_cols = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'starting' in col_lower or 'beginning' in col_lower:
                bridge_cols['starting'] = col
            elif 'expansion' in col_lower or 'upsell' in col_lower:
                bridge_cols['expansion'] = col  
            elif 'contraction' in col_lower or 'downsell' in col_lower:
                bridge_cols['contraction'] = col
            elif 'churn' in col_lower or 'lost' in col_lower:
                bridge_cols['churn'] = col
            elif 'new' in col_lower and ('customer' in col_lower or 'revenue' in col_lower):
                bridge_cols['new'] = col
            elif 'ending' in col_lower or 'final' in col_lower:
                bridge_cols['ending'] = col
        
        if len(bridge_cols) >= 3:  # Need at least 3 components for waterfall
            # Create waterfall chart
            categories = []
            values = []
            
            # Calculate totals for each category
            for key in ['starting', 'new', 'expansion', 'contraction', 'churn', 'ending']:
                if key in bridge_cols:
                    col_name = bridge_cols[key]
                    if col_name in df.columns:
                        total = df[col_name].sum()
                        categories.append(col_name.replace('_', ' ').title())
                        
                        # Make contractions and churn negative for waterfall effect
                        if key in ['contraction', 'churn']:
                            values.append(-abs(total))
                        else:
                            values.append(total)
            
            if categories and values:
                fig = go.Figure(go.Waterfall(
                    name="Revenue Bridge",
                    orientation="v",
                    measure=["absolute"] + ["relative"] * (len(categories)-2) + ["total"],
                    x=categories,
                    textposition="outside",
                    text=[f"${v:,.0f}" for v in values],
                    y=values,
                    connector={"line":{"color":"rgb(63, 63, 63)"}},
                ))
                
                fig.update_layout(
                    title="Q3 to Q4 Revenue Bridge Analysis",
                    showlegend=False,
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Add summary metrics
        cols = st.columns(4)
        revenue_cols = [col for col in df.columns if 'revenue' in col.lower()]
        for i, col in enumerate(revenue_cols[:4]):
            with cols[i]:
                total = df[col].sum()
                st.metric(col.replace('_', ' ').title(), f"${total:,.0f}")
    
    def _generate_customer_analysis_visualizations(self, df):
        """Generate visualizations for customer analysis"""
        st.write("### 👥 Customer Analysis")
        
        # Find customer and revenue columns
        customer_col = None
        revenue_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if 'customer' in col_lower or 'client' in col_lower or 'company' in col_lower:
                customer_col = col
            elif 'revenue' in col_lower or 'amount' in col_lower or 'value' in col_lower:
                revenue_col = col
        
        if customer_col and revenue_col:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top customers pie chart
                df_agg = df.groupby(customer_col)[revenue_col].sum().reset_index()
                df_top = df_agg.nlargest(8, revenue_col)
                
                fig_pie = px.pie(
                    df_top,
                    values=revenue_col,
                    names=customer_col,
                    title="Top 8 Customers by Revenue"
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                # Customer concentration bar chart
                df_sorted = df.nlargest(10, revenue_col)
                fig_bar = px.bar(
                    df_sorted,
                    x=customer_col,
                    y=revenue_col,
                    title="Top 10 Customer Revenue"
                )
                fig_bar.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_bar, use_container_width=True)
    
    def _generate_geographic_visualizations(self, df):
        """Generate visualizations for geographic analysis"""
        st.write("### 🌍 Geographic Analysis")
        
        # Find geographic and revenue columns
        geo_col = None
        revenue_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['country', 'region', 'location']):
                geo_col = col
            elif 'revenue' in col_lower or 'amount' in col_lower or 'value' in col_lower:
                revenue_col = col
        
        if geo_col and revenue_col:
            col1, col2 = st.columns(2)
            
            with col1:
                # Geographic pie chart
                df_agg = df.groupby(geo_col)[revenue_col].sum().reset_index()
                fig_pie = px.pie(
                    df_agg,
                    values=revenue_col,
                    names=geo_col,
                    title=f"Revenue by {geo_col.replace('_', ' ').title()}"
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                # Geographic bar chart
                df_sorted = df_agg.sort_values(revenue_col, ascending=False)
                fig_bar = px.bar(
                    df_sorted,
                    x=geo_col,
                    y=revenue_col,
                    title=f"Revenue Distribution by {geo_col.replace('_', ' ').title()}"
                )
                fig_bar.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_bar, use_container_width=True)
    
    def _generate_quarterly_visualizations(self, df):
        """Generate visualizations for quarterly analysis"""
        st.write("### 📊 Quarterly Growth Analysis")
        
        # Look for Q3 and Q4 columns or quarterly data
        q3_col = None
        q4_col = None
        customer_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if 'q3' in col_lower:
                q3_col = col
            elif 'q4' in col_lower:
                q4_col = col
            elif 'customer' in col_lower or 'client' in col_lower or 'company' in col_lower:
                customer_col = col
        
        if q3_col and q4_col and customer_col:
            # Calculate growth rates
            df['growth'] = ((df[q4_col] - df[q3_col]) / df[q3_col] * 100).fillna(0)
            df['growth_abs'] = df[q4_col] - df[q3_col]
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Top 10 growth customers
                df_growth = df.nlargest(10, 'growth')
                fig = px.bar(
                    df_growth,
                    x=customer_col,
                    y='growth',
                    title="Top 10 Revenue Growth (Q3 to Q4) %"
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Absolute growth
                df_abs_growth = df.nlargest(10, 'growth_abs')
                fig = px.bar(
                    df_abs_growth,
                    x=customer_col,
                    y='growth_abs',
                    title="Top 10 Absolute Revenue Growth (Q3 to Q4)"
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
    
    def _generate_monthly_trends_visualizations(self, df):
        """Generate visualizations for monthly trends - handles various data structures including JSON objects"""
        st.write("### 📈 Monthly Revenue Trends")
        
        # Add debugging information (collapsible)
        with st.expander("🔍 Debug Info (Click to expand)", expanded=False):
            st.write(f"Data shape: {df.shape}")
            st.write(f"Columns: {list(df.columns)}")
            if len(df) > 0:
                st.write("**Sample data types:**")
                for col in df.columns[:5]:  # Show first 5 columns
                    sample_val = df[col].iloc[0]
                    st.write(f"- {col}: {type(sample_val).__name__} = {str(sample_val)[:50]}{'...' if len(str(sample_val)) > 50 else ''}")
        
        # Strategy 1: Look for Month_Label + Revenue structure (common pattern)
        if 'Month_Label' in df.columns and 'Revenue' in df.columns:
            self._create_month_label_visualizations(df)
            return
        
        # Strategy 2: Look for individual month columns (jan, feb, etc.)
        month_cols = [col for col in df.columns if any(month in col.lower() 
                     for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 
                                  'jul', 'aug', 'sep', 'oct', 'nov', 'dec'])]
        
        if month_cols:
            self._create_individual_month_visualizations(df, month_cols)
            return
        
        # Strategy 3: Handle JSON objects or nested data structures
        if self._has_json_objects(df):
            self._create_json_object_visualizations(df)
            return
        
        # Strategy 4: Look for time-series data (date + revenue)
        date_cols = [col for col in df.columns if any(term in col.lower() 
                    for term in ['date', 'time', 'month', 'period'])]
        revenue_cols = [col for col in df.columns if any(term in col.lower() 
                       for term in ['revenue', 'amount', 'value'])]
        
        if date_cols and revenue_cols:
            self._create_timeseries_visualizations(df, date_cols[0], revenue_cols[0])
            return
        
        # Strategy 5: Fallback - show helpful message and raw data
        st.info("📊 Monthly data pattern not automatically detected. The system will use default visualizations.")
        
        with st.expander("📋 View Raw Data", expanded=False):
            st.dataframe(df.head(10))
            
        with st.expander("🔧 Technical Details", expanded=False):
            st.write("**Data Types:**")
            for col in df.columns:
                sample_val = df[col].iloc[0] if len(df) > 0 else None
                st.write(f"- {col}: {type(sample_val).__name__} = {str(sample_val)[:100]}{'...' if len(str(sample_val)) > 100 else ''}")
        
        # Try to create a basic visualization anyway
        if len(df.columns) >= 2:
            st.write("**Attempting basic visualization:**")
            try:
                # Use first column as x, second as y
                fig = px.bar(df.head(12), x=df.columns[0], y=df.columns[1], 
                           title=f"{df.columns[1]} by {df.columns[0]}")
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Could not create basic visualization: {str(e)}")
    
    def _create_month_label_visualizations(self, df):
        """Create visualizations for Month_Label + Revenue structure"""
        st.write("**Data Structure:** Month_Label + Revenue format detected")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Monthly revenue trend
            fig = px.line(
                df, 
                x='Month_Label', 
                y='Revenue',
                title='Monthly Revenue Trend',
                markers=True
            )
            fig.update_layout(xaxis_tickangle=-45)
            fig.update_traces(line=dict(width=3), marker=dict(size=8))
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Monthly variance if available
            if 'Variance in amount' in df.columns:
                colors = ['green' if x >= 0 else 'red' for x in df['Variance in amount']]
                fig = px.bar(
                    df, 
                    x='Month_Label', 
                    y='Variance in amount',
                    title='Monthly Revenue Variance',
                    color=df['Variance in amount'],
                    color_continuous_scale=['red', 'green']
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                # Revenue bar chart as alternative
                fig = px.bar(
                    df, 
                    x='Month_Label', 
                    y='Revenue',
                    title='Monthly Revenue (Bar Chart)'
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        
        # Summary metrics
        cols = st.columns(4)
        with cols[0]:
            total_revenue = df['Revenue'].sum()
            st.metric("Total Revenue", f"${total_revenue:,.2f}")
        with cols[1]:
            avg_monthly = df['Revenue'].mean()
            st.metric("Average Monthly", f"${avg_monthly:,.2f}")
        with cols[2]:
            max_month = df.loc[df['Revenue'].idxmax()]
            st.metric("Best Month", f"{max_month['Month_Label']}")
        with cols[3]:
            if 'Variance in %' in df.columns:
                latest_growth = df['Variance in %'].iloc[-1]
                st.metric("Latest Growth", f"{latest_growth:.1f}%")
    
    def _create_individual_month_visualizations(self, df, month_cols):
        """Create visualizations for individual month columns"""
        st.write("**Data Structure:** Individual month columns detected")
        
        # Create monthly totals
        monthly_totals = {}
        for col in month_cols:
            month_name = col.split('_')[-1] if '_' in col else col
            monthly_totals[month_name] = df[col].sum()
        
        if monthly_totals:
            months_df = pd.DataFrame(list(monthly_totals.items()), columns=['Month', 'Revenue'])
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.line(
                    months_df,
                    x='Month',
                    y='Revenue',
                    title="Monthly Revenue Trends",
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    months_df,
                    x='Month',
                    y='Revenue',
                    title="Monthly Revenue (Bar Chart)"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    def _has_json_objects(self, df):
        """Check if dataframe contains JSON objects that need special handling"""
        for col in df.columns:
            if len(df) > 0:
                sample_val = df[col].iloc[0]
                if isinstance(sample_val, (dict, list)):
                    return True
                # Check if string might be JSON
                if isinstance(sample_val, str) and (sample_val.startswith('{') or sample_val.startswith('[')):
                    try:
                        json.loads(sample_val)
                        return True
                    except:
                        pass
        return False
    
    def _create_json_object_visualizations(self, df):
        """Handle JSON objects or nested data structures"""
        st.write("**Data Structure:** JSON objects detected - attempting to parse")
        
        # Try to flatten JSON objects
        flattened_data = []
        
        for idx, row in df.iterrows():
            flat_row = {}
            for col, val in row.items():
                if isinstance(val, dict):
                    # Flatten dictionary
                    for k, v in val.items():
                        flat_row[f"{col}_{k}"] = v
                elif isinstance(val, str) and (val.startswith('{') or val.startswith('[')):
                    # Try to parse JSON string
                    try:
                        parsed = json.loads(val)
                        if isinstance(parsed, dict):
                            for k, v in parsed.items():
                                flat_row[f"{col}_{k}"] = v
                        else:
                            flat_row[col] = parsed
                    except:
                        flat_row[col] = val
                else:
                    flat_row[col] = val
            flattened_data.append(flat_row)
        
        if flattened_data:
            flattened_df = pd.DataFrame(flattened_data)
            st.write("**Flattened Data Structure:**")
            st.write(f"New columns: {list(flattened_df.columns)}")
            
            # Try to find revenue/amount data in flattened structure
            revenue_cols = [col for col in flattened_df.columns if any(term in col.lower() 
                           for term in ['revenue', 'amount', 'value', 'total'])]
            
            if revenue_cols:
                # Create simple visualization with flattened data
                fig = px.bar(
                    flattened_df.head(12),  # Show first 12 rows (likely months)
                    y=revenue_cols[0],
                    title=f"Monthly Data: {revenue_cols[0]}"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Could not find revenue data in flattened JSON objects")
                st.dataframe(flattened_df.head())
        else:
            st.error("Failed to parse JSON objects")
    
    def _create_timeseries_visualizations(self, df, date_col, revenue_col):
        """Create visualizations for time-series data"""
        st.write("**Data Structure:** Time-series format detected")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(
                df,
                x=date_col,
                y=revenue_col,
                title=f"{revenue_col} Over Time",
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                df,
                x=date_col,
                y=revenue_col,
                title=f"{revenue_col} by {date_col}"
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    def _generate_default_visualizations(self, df, schema):
        """Generate default visualizations when no specific pattern is detected"""
        st.write("### 📊 Data Analysis")
        
        # Try to create meaningful default visualizations
        metrics = schema.get('metrics', {}) if schema else {}
        
        # Create visualization columns
        viz_cols = st.columns(2)
        viz_count = 0
        
        # Try pie chart first
        with viz_cols[0]:
            if self._create_pie_chart(df, metrics):
                viz_count += 1
        
        # Then bar chart
        with viz_cols[1]:
            if self._create_bar_chart(df, metrics):
                viz_count += 1
        
        # If we have room, add more visualizations
        if viz_count < 2:
            if viz_count == 0:
                with viz_cols[0]:
                    self._create_enhanced_table(df)
            else:
                with viz_cols[1]:
                    self._create_metric_cards(df, metrics)
    
    def _create_visualization(self, df, viz_type, metrics, analysis_type):
        """Create a specific visualization type"""
        try:
            if viz_type == 'bar_chart':
                return self._create_bar_chart(df, metrics)
            elif viz_type == 'line_chart':
                return self._create_line_chart(df, metrics)
            elif viz_type == 'pie_chart':
                return self._create_pie_chart(df, metrics)
            elif viz_type == 'metric_cards':
                return self._create_metric_cards(df, metrics)
            elif viz_type == 'treemap':
                return self._create_treemap(df, metrics)
            elif viz_type == 'waterfall_chart':
                return self._create_waterfall_chart(df, metrics)
            elif viz_type == 'table':
                return self._create_enhanced_table(df)
            else:
                # Default fallback
                return self._create_bar_chart(df, metrics)
        except Exception as e:
            st.error(f"Error creating {viz_type}: {str(e)}")
            return False
    
    def _create_bar_chart(self, df, metrics):
        """Create a bar chart visualization"""
        revenue_cols = metrics.get('revenue_columns', [])
        id_cols = metrics.get('id_columns', [])
        
        if revenue_cols and id_cols:
            x_col = id_cols[0]
            y_col = revenue_cols[0]
            
            if x_col in df.columns and y_col in df.columns:
                # Sort and take top 10 for readability
                df_sorted = df.nlargest(10, y_col)
                
                fig = px.bar(
                    df_sorted, 
                    x=x_col, 
                    y=y_col,
                    title=f"Top 10 {x_col.replace('_', ' ').title()} by {y_col.replace('_', ' ').title()}"
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
                return True
        
        return False
    
    def _create_line_chart(self, df, metrics):
        """Create a line chart for time series data"""
        date_cols = metrics.get('date_columns', [])
        revenue_cols = metrics.get('revenue_columns', [])
        
        if date_cols and revenue_cols:
            x_col = date_cols[0]
            y_col = revenue_cols[0]
            
            if x_col in df.columns and y_col in df.columns:
                fig = px.line(
                    df, 
                    x=x_col, 
                    y=y_col,
                    title=f"{y_col.replace('_', ' ').title()} Over Time",
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)
                return True
        
        return False
    
    def _create_pie_chart(self, df, metrics):
        """Create a pie chart for categorical data"""
        categorical_cols = metrics.get('categorical_columns', [])
        revenue_cols = metrics.get('revenue_columns', [])
        
        if categorical_cols and revenue_cols:
            names_col = categorical_cols[0]
            values_col = revenue_cols[0]
            
            if names_col in df.columns and values_col in df.columns:
                # Take top 8 categories for readability
                df_agg = df.groupby(names_col)[values_col].sum().reset_index()
                df_top = df_agg.nlargest(8, values_col)
                
                fig = px.pie(
                    df_top,
                    values=values_col,
                    names=names_col,
                    title=f"{values_col.replace('_', ' ').title()} by {names_col.replace('_', ' ').title()}"
                )
                st.plotly_chart(fig, use_container_width=True)
                return True
        
        return False
    
    def _create_metric_cards(self, df, metrics):
        """Create metric cards summary"""
        revenue_cols = metrics.get('revenue_columns', [])
        
        if revenue_cols:
            st.subheader("Summary Metrics")
            cols = st.columns(min(len(revenue_cols), 3))
            
            for i, col_name in enumerate(revenue_cols[:3]):
                if col_name in df.columns:
                    with cols[i]:
                        total = df[col_name].sum()
                        count = df[col_name].count()
                        st.metric(
                            col_name.replace('_', ' ').title(),
                            f"${total:,.2f}" if 'revenue' in col_name.lower() else f"{total:,.0f}",
                            f"{count} records"
                        )
            return True
        
        return False
    
    def _create_treemap(self, df, metrics):
        """Create a treemap visualization"""
        id_cols = metrics.get('id_columns', [])
        revenue_cols = metrics.get('revenue_columns', [])
        
        if id_cols and revenue_cols:
            path_col = id_cols[0]
            values_col = revenue_cols[0]
            
            if path_col in df.columns and values_col in df.columns:
                # Take top 15 for readability
                df_top = df.nlargest(15, values_col)
                
                fig = px.treemap(
                    df_top,
                    path=[path_col],
                    values=values_col,
                    title=f"{values_col.replace('_', ' ').title()} Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
                return True
        
        return False
    
    def _create_waterfall_chart(self, df, metrics):
        """Create a waterfall chart for bridge analysis"""
        # Look for bridge-specific columns
        bridge_cols = [col for col in df.columns if any(term in col.lower() 
                      for term in ['expansion', 'contraction', 'churn', 'new'])]
        
        if bridge_cols:
            st.subheader("Revenue Bridge Analysis")
            st.info("Waterfall chart would be displayed here with bridge data")
            return True
        
        return False
    
    def _create_enhanced_table(self, df):
        """Create an enhanced data table"""
        st.subheader("Data Table")
        st.dataframe(df.head(100), use_container_width=True)
        return True
    
    def _generate_data_table(self, json_data, schema):
        """Generate the data table section"""
        if not isinstance(json_data, list) or not json_data:
            return
        
        st.subheader("📋 Data Details")
        
        df = pd.DataFrame(json_data)
        
        # Add filters if we have categorical columns
        metrics = schema.get('metrics', {}) if schema else {}
        categorical_cols = metrics.get('categorical_columns', [])
        
        if categorical_cols:
            st.write("**Filters:**")
            filter_cols = st.columns(min(len(categorical_cols), 3))
            
            filtered_df = df.copy()
            
            for i, col in enumerate(categorical_cols[:3]):
                if col in df.columns:
                    with filter_cols[i]:
                        unique_values = ['All'] + list(df[col].unique())
                        selected = st.selectbox(f"Filter by {col}", unique_values, key=f"filter_{col}")
                        
                        if selected != 'All':
                            filtered_df = filtered_df[filtered_df[col] == selected]
            
            st.dataframe(filtered_df, use_container_width=True)
        else:
            st.dataframe(df.head(100), use_container_width=True)

# Display function for chatbot
def display_chatbot(data, view_title):
    """Display chatbot interface for data analysis"""
    st.subheader("💬 AI Data Analyst")
    st.markdown("Ask questions about the data, trends, insights, or get analysis recommendations.")
    
    # Initialize chatbot if not exists
    if f"chatbot_{view_title}" not in st.session_state:
        st.session_state[f"chatbot_{view_title}"] = OpenAIChatbot()
    
    # Initialize chat history
    chat_key = f"chat_history_{view_title}"
    if chat_key not in st.session_state:
        st.session_state[chat_key] = []
    
    # Suggestion buttons
    suggestions = [
        "What are the key insights from this data?",
        "Show me the top performers",
        "What trends do you see?",
        "Any concerning patterns?",
        "Recommend next steps"
    ]
    
    st.markdown("**Quick Questions:**")
    cols = st.columns(len(suggestions))
    for i, suggestion in enumerate(suggestions):
        with cols[i]:
            if st.button(suggestion[:15] + "...", key=f"suggest_{view_title}_{i}"):
                st.session_state[f"pending_question_{chat_key}"] = suggestion
                st.rerun()
    
    # Chat input
    user_question = st.chat_input(f"Ask about your {view_title} data...", key=f"chat_input_{view_title}")
    
    # Check for pending question from buttons
    pending_key = f"pending_question_{chat_key}"
    if pending_key in st.session_state:
        user_question = st.session_state[pending_key]
        del st.session_state[pending_key]
    
    # Process user question
    if user_question:
        # Add user message to chat history
        st.session_state[chat_key].append({"role": "user", "content": user_question})
        
        # Generate AI response
        try:
            with st.spinner("🤖 Analyzing your data..."):
                response = st.session_state[f"chatbot_{view_title}"].get_response(
                    user_question, view_title.lower(), data, ""
                )
                
                # Add AI response to chat history
                st.session_state[chat_key].append({"role": "assistant", "content": response})
                
        except Exception as e:
            error_msg = f"❌ Error: {str(e)}"
            st.session_state[chat_key].append({"role": "assistant", "content": error_msg})
    
    # Display chat history
    if st.session_state[chat_key]:
        st.markdown("### 💬 Chat History")
        for i, message in enumerate(reversed(st.session_state[chat_key][-10:])):  # Show last 10 messages
            if message["role"] == "user":
                st.markdown(f"**You:** {message['content']}")
            else:
                st.markdown(f"**AI:** {message['content']}")
            st.markdown("---")

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
    
    # Add AI Chatbot
    st.markdown("---")
    display_chatbot_with_schema(data, view_title)

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
    
    revenue_categories = ['Sep Revenue', 'New Revenue', 'Expansion Revenue', 
                         'Contraction Revenue', 'Churned Revenue', 'Oct Revenue']
    
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
        textposition="outside",
        y=values,
        connector={"line": {"color": "rgb(63, 63, 63)", "width": 2}},
        increasing={"marker": {"color": "#2E8B57"}},  # Sea green for positive
        decreasing={"marker": {"color": "#DC143C"}},  # Crimson for negative  
        totals={"marker": {"color": "#4682B4"}},      # Steel blue for totals
    ))
    
    fig.update_layout(
        title={
            'text': "💰 Interactive Revenue Bridge: Q3 to Q4",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18}
        },
        showlegend=False,
        height=500,
        xaxis={'title': 'Revenue Components'},
        yaxis={'title': 'Revenue ($)', 'tickformat': '$,.0f'},
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.subheader("Customer-wise Revenue Bridge")
    st.dataframe(df, use_container_width=True)
    
    # Add AI Chatbot
    st.markdown("---")
    display_chatbot_with_schema(data, view_title)

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
    
    # Add AI Chatbot
    st.markdown("---")
    display_chatbot_with_schema(data, view_title)

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
        top_5_pct = (top_5_revenue/total_revenue)*100 if total_revenue > 0 else 0
        top_10_pct = (top_10_revenue/total_revenue)*100 if total_revenue > 0 else 0
        st.metric("Top 5 Customers %", f"{top_5_pct:.1f}%")
        st.metric("Top 10 Customers %", f"{top_10_pct:.1f}%")
    
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
    
    # Add AI Chatbot
    st.markdown("---")
    display_chatbot_with_schema(data, view_title)

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
    
    # Add AI Chatbot
    st.markdown("---")
    display_chatbot_with_schema(data, view_title)

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
        
        # Uploaded files table for S3 file storage tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS uploaded_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER,
                original_filename TEXT NOT NULL,
                s3_key TEXT NOT NULL,
                file_type TEXT NOT NULL,
                file_size INTEGER,
                upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    
    def remove_investor_company_connection(self, investor_id, company_id):
        """Remove connection between investor and company"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM investor_companies WHERE investor_id = ? AND company_id = ?",
                (investor_id, company_id)
            )
            conn.commit()
            return cursor.rowcount > 0  # Returns True if row was deleted
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
    
    # File metadata management methods
    def save_uploaded_file(self, company_id, original_filename, s3_key, file_type, file_size):
        """Save uploaded file metadata to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO uploaded_files 
               (company_id, original_filename, s3_key, file_type, file_size) 
               VALUES (?, ?, ?, ?, ?)""",
            (company_id, original_filename, s3_key, file_type, file_size)
        )
        file_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return file_id
    
    def get_uploaded_files(self, company_id):
        """Get all uploaded files for a company"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """SELECT id, original_filename, s3_key, file_type, file_size, upload_timestamp
               FROM uploaded_files 
               WHERE company_id = ? 
               ORDER BY upload_timestamp DESC""",
            (company_id,)
        )
        files = cursor.fetchall()
        conn.close()
        return files
    
    def delete_uploaded_file(self, file_id):
        """Delete uploaded file metadata from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Get S3 key before deleting for cleanup
        cursor.execute("SELECT s3_key FROM uploaded_files WHERE id = ?", (file_id,))
        result = cursor.fetchone()
        s3_key = result[0] if result else None
        
        # Delete the record
        cursor.execute("DELETE FROM uploaded_files WHERE id = ?", (file_id,))
        conn.commit()
        conn.close()
        return s3_key
    
    def get_file_by_id(self, file_id):
        """Get specific file metadata by ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """SELECT id, company_id, original_filename, s3_key, file_type, file_size, upload_timestamp
               FROM uploaded_files 
               WHERE id = ?""",
            (file_id,)
        )
        file_data = cursor.fetchone()
        conn.close()
        return file_data

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
                # Calculate growth metrics
                df['Growth_Amount'] = df[q4_col] - df[q3_col]
                df['Growth_Rate'] = ((df[q4_col] - df[q3_col]) / df[q3_col] * 100).fillna(0)
                
                # Get top 10 by absolute growth
                top_growth = df.nlargest(10, 'Growth_Amount')
                
                fig2 = px.bar(top_growth, 
                             x='Growth_Amount', 
                             y=df.columns[0],
                             title="🚀 Top 10 Revenue Growth (Q3 to Q4)",
                             orientation='h',
                             color='Growth_Rate',
                             color_continuous_scale='RdYlGn',
                             hover_data={'Growth_Rate': ':.1f%'})
                fig2.update_layout(
                    xaxis_title="Revenue Growth ($)",
                    yaxis_title="Customer"
                )
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

def load_dynamic_json_analyses(s3_config=None, use_s3=False, force_refresh=False):
    """Load JSON analyses from S3 bucket or local files with dynamic detection"""
    
    if use_s3 and s3_config and s3_config.is_configured():
        return load_analyses_from_s3(s3_config, force_refresh=force_refresh)
    else:
        return load_analyses_from_local()

def load_analyses_from_s3(s3_config, force_refresh=False):
    """Load and categorize JSON files from S3 bucket"""
    try:
        s3_discovery = S3DataDiscovery(s3_config)
        schema_analyzer = JSONSchemaAnalyzer()
        
        # Force refresh if requested
        discovered_files = s3_discovery.discover_json_files(force_refresh=force_refresh)
        
        # Debug information
        if force_refresh:
            st.info(f"🔍 S3 Discovery: Found {len(discovered_files)} files (forced refresh)")
        else:
            st.info(f"🔍 S3 Discovery: Found {len(discovered_files)} files (cached)")
            
        if not discovered_files:
            st.warning("No JSON files found in S3 bucket")
            return {}
        
        # Get file categories from discovery
        file_categories = s3_discovery.get_file_categories(discovered_files)
        
        analyses = {}
        
        for category, files in file_categories.items():
            category_data = []
            
            for file_info in files:
                try:
                    # Load JSON data from S3
                    json_data = s3_discovery.load_json_from_s3(file_info['original_key'])
                    
                    if json_data:
                        # Analyze schema and enhance categorization
                        schema = schema_analyzer.analyze_json_schema(json_data, file_info['original_key'])
                        
                        # Use schema-detected type if confidence is high
                        if schema and schema['confidence_score'] > 60:
                            detected_category = schema['data_type']
                            if detected_category != 'unknown' and detected_category != 'general':
                                category = detected_category
                        
                        # Store data with metadata
                        file_data = {
                            'data': json_data,
                            'metadata': file_info,
                            'schema': schema,
                            'source': 's3'
                        }
                        
                        # Group by detected category
                        if category not in analyses:
                            analyses[category] = []
                        analyses[category].append(file_data)
                        
                        st.success(f"✅ Loaded {file_info['file_name']} (detected as {category})")
                    
                except Exception as e:
                    st.error(f"❌ Error loading {file_info['file_name']}: {str(e)}")
        
        # Flatten data for backward compatibility if needed
        simplified_analyses = {}
        for category, file_list in analyses.items():
            if len(file_list) == 1:
                # Single file - use just the data
                simplified_analyses[category] = file_list[0]['data']
            else:
                # Multiple files - keep structure or merge if appropriate
                simplified_analyses[category] = [item['data'] for item in file_list]
        
        # Debug information for tab creation
        st.info(f"📊 Tab creation: Will create {len(simplified_analyses)} tabs: {list(simplified_analyses.keys())}")
        
        return simplified_analyses
        
    except Exception as e:
        st.error(f"Error loading from S3: {str(e)}")
        return {}

def load_analyses_from_local():
    """Load the 5 real JSON files from local filesystem (fallback)"""
    
    json_files = {
        "quarterly": "A._Quarterly_Revenue_and_QoQ_growth.json",
        "bridge": "B._Revenue_Bridge_and_Churned_Analysis.json", 
        "geographic": "C._Country_wise_Revenue_Analysis.json",
        "customer": "E._Customer_concentration_analysis.json",
        "monthly": "F._Month_on_Month_Revenue_analysis.json"
    }
    
    analyses = {}
    for key, filename in json_files.items():
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                analyses[key] = json.load(f)
            st.success(f"✅ Loaded {filename}")
        except Exception as e:
            st.error(f"❌ Error loading {filename}: {str(e)}")
            # Fallback to empty list if file can't be loaded
            analyses[key] = []
    
    return analyses

# Legacy function for backward compatibility
def load_real_json_analyses():
    """Legacy function - redirects to local loading"""
    return load_analyses_from_local()

def generate_ai_executive_summary(json_data, analysis_type, schema=None):
    """Generate AI-powered executive summary using OpenAI with dynamic schema awareness"""
    
    # Initialize OpenAI client
    api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("openai_api_key", "")
    if not api_key:
        return generate_fallback_summary(json_data, analysis_type)
    
    try:
        client = OpenAI(api_key=api_key)
        
        # Prepare data context (limit size for API)
        data_sample = json_data[:50] if isinstance(json_data, list) and len(json_data) > 50 else json_data
        data_context = json.dumps(data_sample, indent=2, default=str)[:8000]  # Limit context size
        
        # Generate schema-aware prompt
        if schema:
            prompt = generate_dynamic_prompt(data_context, analysis_type, schema)
        else:
            # Fallback to static prompts
            prompt = generate_static_prompt(data_context, analysis_type)
        
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a world-class financial analyst and business intelligence expert with 15+ years of experience in revenue operations, customer analytics, and strategic business planning. Provide actionable insights with specific metrics and recommendations."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1500,
            temperature=0.2
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        return generate_fallback_summary(json_data, analysis_type)

def generate_dynamic_prompt(data_context, analysis_type, schema):
    """Generate dynamic prompts based on schema analysis"""
    
    metrics = schema.get('metrics', {})
    columns = schema.get('columns', {})
    confidence = schema.get('confidence_score', 0)
    suggested_viz = schema.get('suggested_visualizations', [])
    
    # Extract key column information
    revenue_cols = metrics.get('revenue_columns', [])
    date_cols = metrics.get('date_columns', [])
    id_cols = metrics.get('id_columns', [])
    categorical_cols = metrics.get('categorical_columns', [])
    
    # Build dynamic context description
    context_desc = f"This dataset contains {len(columns)} columns with {len(json.loads(data_context)) if isinstance(json.loads(data_context), list) else 1} records."
    
    if revenue_cols:
        context_desc += f" Key revenue columns: {', '.join(revenue_cols)}."
    if date_cols:
        context_desc += f" Time-based columns: {', '.join(date_cols)}."
    if id_cols:
        context_desc += f" Identifier columns: {', '.join(id_cols)}."
    if categorical_cols:
        context_desc += f" Categorical dimensions: {', '.join(categorical_cols)}."
    
    # Generate analysis-specific sections based on detected patterns
    analysis_sections = []
    
    if revenue_cols:
        analysis_sections.append("""
## 📈 Financial Performance Analysis
- Calculate total and average values for revenue metrics
- Identify top performers and key contributors
- Analyze revenue distribution patterns""")
    
    if date_cols and revenue_cols:
        analysis_sections.append("""
## 📅 Temporal Analysis
- Identify trends and patterns over time
- Highlight seasonal variations or growth periods
- Assess consistency and volatility""")
    
    if categorical_cols:
        analysis_sections.append("""
## 🎯 Segmentation Analysis
- Break down performance by key segments
- Identify high-performing categories
- Assess concentration and diversification""")
    
    if id_cols and revenue_cols:
        analysis_sections.append("""
## 🏆 Performance Ranking
- Rank entities by key performance metrics
- Identify outliers and exceptional cases
- Assess competitive positioning""")
    
    # Risk and opportunity section
    analysis_sections.append("""
## ⚠️ Risks & Opportunities
- Flag potential risks based on data patterns
- Identify growth opportunities and optimization areas
- Assess data quality and completeness issues""")
    
    # Strategic recommendations
    analysis_sections.append("""
## 🚀 Strategic Recommendations
- Provide actionable next steps based on findings
- Suggest optimization strategies
- Recommend areas for deeper investigation""")
    
    # Combine into full prompt
    prompt = f"""You are analyzing a {analysis_type} dataset with automatically detected schema.

{context_desc}

Data Context:
{data_context}

Schema Confidence: {confidence}%
Suggested Visualizations: {', '.join(suggested_viz)}

Please provide a comprehensive executive summary with the following sections:
{''.join(analysis_sections)}

Focus on the actual data patterns you observe and provide specific, actionable insights based on the metrics and dimensions available in this dataset."""
    
    return prompt

def generate_static_prompt(data_context, analysis_type):
    """Generate static prompts for known analysis types (fallback)"""
    
    prompts = {
        "quarterly": f"""You are analyzing Q3 to Q4 quarterly revenue performance data. This dataset contains customer-level revenue data showing Quarter 3 Revenue, Quarter 4 Revenue, Variance (absolute change), and Percentage of Variance (growth rate).

Data Context:
{data_context}

Provide a comprehensive executive summary analyzing customer growth patterns, revenue variance, and business performance.""",

        "general": f"""You are analyzing a business dataset to provide strategic insights.

Data Context:
{data_context}

Please provide a comprehensive analysis with key insights, trends, and recommendations based on the available data."""
    }
    
    return prompts.get(analysis_type, prompts["general"])

def generate_schema_aware_chatbot_response(question, json_data, analysis_type, schema=None):
    """Generate chatbot responses with schema awareness"""
    
    api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("openai_api_key", "")
    if not api_key:
        return f"I'd be happy to help analyze your {analysis_type} data! However, OpenAI integration is not available right now."
    
    try:
        client = OpenAI(api_key=api_key)
        
        # Prepare context with schema information
        context_info = ""
        if schema:
            metrics = schema.get('metrics', {})
            columns = schema.get('columns', {})
            
            context_info = f"Dataset has {len(columns)} columns. "
            if metrics.get('revenue_columns'):
                context_info += f"Revenue columns: {', '.join(metrics['revenue_columns'])}. "
            if metrics.get('categorical_columns'):
                context_info += f"Categories: {', '.join(metrics['categorical_columns'])}. "
        
        # Sample data for context
        data_sample = json_data[:10] if isinstance(json_data, list) and len(json_data) > 10 else json_data
        data_context = json.dumps(data_sample, indent=2, default=str)[:2000]
        
        prompt = f"""You are analyzing {analysis_type} data. {context_info}

Question: {question}

Data sample:
{data_context}

Provide a helpful, specific answer based on the data. Include numbers and insights where relevant."""
        
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a data analyst assistant. Provide clear, specific answers based on the data provided."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.3
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        return f"Sorry, I encountered an error: {str(e)}"

# Update the OpenAIChatbot class to use schema-aware responses
class OpenAIChatbot:
    def __init__(self, data, data_type, schema=None):
        self.data = data
        self.data_type = data_type
        self.schema = schema
        self.api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("openai_api_key", "")
        
    def get_response(self, question):
        """Get AI response with schema awareness"""
        if self.api_key:
            return generate_schema_aware_chatbot_response(question, self.data, self.data_type, self.schema)
        else:
            return self.get_fallback_response(question)
    
    def get_fallback_response(self, question):
        """Fallback response when OpenAI is not available"""
        if self.schema:
            metrics = self.schema.get('metrics', {})
            if metrics.get('revenue_columns'):
                return f"I can help you analyze the revenue data in columns: {', '.join(metrics['revenue_columns'])}. OpenAI integration is currently unavailable."
        
        return f"I can help you analyze {self.data_type} data. Try asking about totals, averages, or top performers."

# Update display_chatbot to use schema-aware chatbot
def display_universal_chatbot():
    """Display universal AI assistant in sidebar for general business questions"""
    st.sidebar.subheader("🤖 AI Assistant")
    st.sidebar.caption("General business & investment questions")
    
    # Initialize universal chatbot session state
    if "universal_chat_history" not in st.session_state:
        st.session_state.universal_chat_history = []
    
    # Quick action buttons
    st.sidebar.write("**Quick Questions:**")
    col1, col2 = st.sidebar.columns(2)
    
    quick_questions = [
        "Market trends?",
        "Key metrics?",
        "Growth analysis",
        "Risk factors"
    ]
    
    question_clicked = None
    for i, question in enumerate(quick_questions):
        if i % 2 == 0:
            with col1:
                if st.button(question, key=f"quick_{i}", help=f"Ask: {question}"):
                    question_clicked = question
        else:
            with col2:
                if st.button(question, key=f"quick_{i}", help=f"Ask: {question}"):
                    question_clicked = question
    
    # Chat input
    user_question = st.sidebar.text_input(
        "Ask anything about business, markets, or investments:",
        key="universal_chat_input",
        placeholder="e.g., What are key SaaS metrics?"
    )
    
    # Process question (either from input or quick button)
    question_to_process = question_clicked or user_question
    
    if st.sidebar.button("Send", key="universal_send") or question_clicked:
        if question_to_process and question_to_process.strip():
            # Get response from universal chatbot
            response = get_universal_chatbot_response(question_to_process)
            
            # Add to chat history
            st.session_state.universal_chat_history.append({
                "question": question_to_process,
                "answer": response
            })
            
            # Clear input if it was typed (not from quick button)
            if not question_clicked:
                st.session_state.universal_chat_input = ""
            
            st.rerun()
    
    # Display chat history (last 3 exchanges to save space)
    if st.session_state.universal_chat_history:
        st.sidebar.write("**Recent Conversations:**")
        for exchange in st.session_state.universal_chat_history[-3:]:
            with st.sidebar.container():
                st.write(f"**Q:** {exchange['question']}")
                st.write(f"**A:** {exchange['answer'][:200]}{'...' if len(exchange['answer']) > 200 else ''}")
                st.markdown("---")
        
        # Clear chat history button
        if st.sidebar.button("Clear History", key="clear_universal_chat"):
            st.session_state.universal_chat_history = []
            st.rerun()

def get_universal_chatbot_response(question):
    """Get response from universal AI assistant"""
    api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("openai_api_key", "")
    
    if not api_key:
        return "⚠️ AI Assistant unavailable. OpenAI API key not configured."
    
    try:
        client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": """You are a senior business consultant and investment advisor with expertise across multiple domains including:
                - SaaS and subscription business models
                - Revenue operations and financial metrics  
                - Market analysis and competitive intelligence
                - Investment analysis and portfolio management
                - Business strategy and growth planning

RESPONSE STYLE: 
- Provide concise, actionable insights (2-4 sentences)
- Focus on practical business advice
- Include specific metrics or benchmarks when relevant
- Maintain professional, executive-level guidance
- Avoid overly technical jargon"""},
                {"role": "user", "content": question}
            ],
            max_tokens=300,
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"⚠️ Error getting response: {str(e)}"

def display_chatbot_with_schema(data, view_title, schema=None):
    """Display chatbot interface with schema-aware responses"""
    st.subheader("💬 AI Data Analyst")
    st.markdown("Ask questions about the data, trends, insights, or get analysis recommendations.")
    
    # Initialize schema-aware chatbot (using the correct constructor)
    chatbot = OpenAIChatbot()
    
    # Enhanced suggestion buttons based on schema and data type
    if schema:
        metrics = schema.get('metrics', {})
        suggestions = generate_schema_based_suggestions(metrics, view_title)
    else:
        # Fallback suggestions based on view title
        if 'quarterly' in view_title.lower():
            suggestions = [
                "Which customers had the highest Q3 to Q4 growth?",
                "What are the top revenue growth percentages?",
                "Which customers are showing declining revenue?",
                "What's the overall revenue trend?"
            ]
        elif 'bridge' in view_title.lower():
            suggestions = [
                "Which customers churned and what was the revenue impact?",
                "What expansion revenue was generated?",
                "What's our net revenue retention rate?",
                "Which customers are at risk of churn?"
            ]
        elif 'geographic' in view_title.lower():
            suggestions = [
                "Which countries generate the most revenue?",
                "What's our geographic concentration risk?",
                "Which markets have expansion opportunities?",
                "How is revenue distributed across regions?"
            ]
        elif 'customer' in view_title.lower():
            suggestions = [
                "Who are our top revenue-generating customers?",
                "What's our customer concentration risk?",
                "Which customers contribute most to total revenue?",
                "What's the revenue distribution across customers?"
            ]
        elif 'monthly' in view_title.lower():
            suggestions = [
                "Which months had the highest revenue?",
                "What are the seasonal revenue patterns?",
                "What's the month-over-month growth trend?",
                "Are there any concerning monthly trends?"
            ]
        else:
            suggestions = [
                "What are the key insights from this data?",
                "Show me the top performers",
                "What trends do you see?",
                "Any recommendations?"
            ]
    
    st.markdown("**💡 Quick Questions:**")
    suggestion_cols = st.columns(min(len(suggestions), 2))
    
    for i, suggestion in enumerate(suggestions[:4]):
        with suggestion_cols[i % 2]:
            if st.button(suggestion, key=f"suggest_{view_title}_{i}"):
                st.session_state[f'pending_question_{view_title}'] = suggestion
    
    # Chat interface
    chat_key = f"chat_history_{view_title}"
    if chat_key not in st.session_state:
        st.session_state[chat_key] = []
    
    # Handle pending questions from suggestion buttons
    pending_key = f'pending_question_{view_title}'
    if pending_key in st.session_state:
        question = st.session_state[pending_key]
        del st.session_state[pending_key]
        
        # Generate dynamic executive summary for this data
        executive_summary = generate_adaptive_executive_summary(data, schema, view_title)
        
        response = chatbot.get_response(question, view_title, data, executive_summary)
        st.session_state[chat_key].append({"question": question, "answer": response})
        st.rerun()
    
    # Regular chat input
    question = st.text_input("Ask a question about this data:", key=f"question_{view_title}")
    
    if st.button("Send", key=f"send_{view_title}") and question:
        # Generate dynamic executive summary for this data
        executive_summary = generate_adaptive_executive_summary(data, schema, view_title)
        
        response = chatbot.get_response(question, view_title, data, executive_summary)
        st.session_state[chat_key].append({"question": question, "answer": response})
        st.rerun()
    
    # Display chat history
    if st.session_state[chat_key]:
        st.markdown("---")
        st.subheader("💬 Conversation")
        
        for i, chat in enumerate(reversed(st.session_state[chat_key][-3:])):
            with st.container():
                st.markdown(f"**You:** {chat['question']}")
                st.markdown(f"**AI:** {chat['answer']}")
                if i < len(st.session_state[chat_key][-3:]) - 1:
                    st.markdown("---")

def generate_schema_based_suggestions(metrics, analysis_type):
    """Generate question suggestions based on schema metrics"""
    suggestions = []
    
    if metrics.get('revenue_columns'):
        suggestions.extend([
            "What's the total revenue across all segments?",
            "Which entities have the highest revenue?"
        ])
    
    if metrics.get('categorical_columns'):
        suggestions.extend([
            f"Break down performance by {metrics['categorical_columns'][0]}",
            "What are the key segment differences?"
        ])
    
    if metrics.get('date_columns'):
        suggestions.extend([
            "What trends do you see over time?",
            "Identify any seasonal patterns"
        ])
    
    if not suggestions:
        suggestions = [
            "What are the key insights?",
            "Show me the top performers",
            "Any recommendations?",
            "What patterns do you notice?"
        ]
    
    return suggestions[:4]

def generate_ai_executive_summary_old(json_data, analysis_type):
    """Legacy function for generating executive summaries"""
    # Initialize OpenAI client
    api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("openai_api_key", "")
    if not api_key:
        return generate_fallback_summary(json_data, analysis_type)
    
    try:
        client = OpenAI(api_key=api_key)
        
        # Prepare data context (limit size for API)
        data_sample = json_data[:50] if isinstance(json_data, list) and len(json_data) > 50 else json_data
        data_context = json.dumps(data_sample, indent=2, default=str)[:8000]  # Limit context size
        
        # Create analysis-specific prompts
        prompts = {
            "quarterly": f"""You are analyzing Q3 to Q4 quarterly revenue performance data. This dataset contains customer-level revenue data showing Quarter 3 Revenue, Quarter 4 Revenue, Variance (absolute change), and Percentage of Variance (growth rate).

Data Context:
{data_context}

Provide a comprehensive executive summary analyzing customer growth patterns, revenue variance, and business performance:

## 📈 Key Performance Insights
- Identify top 3 critical findings from customer revenue analysis with specific metrics
- Calculate total revenue growth between Q3 and Q4 using actual numbers
- Analyze customer segmentation by growth performance (high performers vs. declining customers)

## 🎯 Growth Analysis & Trends  
- Highlight best performing customers with exact growth percentages and revenue figures
- Identify customers with highest absolute revenue gains
- Assess overall portfolio momentum and growth distribution patterns

## ⚠️ Risk Assessment & Challenges
- Flag customers with significant revenue decline or negative variance
- Identify volatility patterns and potential retention risks
- Assess revenue concentration and customer dependency risks

## 🚀 Strategic Recommendations
- Prioritize customer expansion opportunities based on growth trends
- Suggest retention strategies for declining accounts
- Recommend revenue optimization tactics based on variance analysis""",

            "bridge": f"""You are a revenue operations expert. Analyze this revenue bridge data showing customer expansion, contraction, and churn patterns.

Data Context:
{data_context}

Create a professional executive summary with:

## Key Insights
- Revenue retention and expansion patterns
- Customer behavior analysis (expansion vs churn)
- Net revenue retention indicators

## Performance Highlights
- Top expanding customers and revenue amounts
- Healthy expansion revenue patterns
- Customer growth momentum

## Risk Factors
- Churn patterns and at-risk customers
- Revenue contraction concerns

## Strategic Recommendations
- Customer success and retention strategies
- Expansion revenue optimization opportunities""",

            "geographic": f"""You are a market expansion strategist. Analyze this geographic revenue distribution data across countries and regions.

Data Context:
{data_context}

Create a professional executive summary with:

## Key Insights
- Revenue concentration by geography
- Top performing markets with specific revenue amounts
- Market penetration patterns

## Performance Highlights
- Strongest revenue markets and growth opportunities
- Geographic diversification status
- International market performance

## Risk Factors
- Geographic concentration risks
- Underperforming markets

## Strategic Recommendations
- Market expansion priorities
- Geographic diversification strategies""",

            "customer": f"""You are a customer portfolio analyst. Analyze this customer concentration and portfolio data.

Data Context:
{data_context}

Create a professional executive summary with:

## Key Insights
- Customer concentration risk assessment
- Portfolio diversification analysis
- Key customer dependencies

## Performance Highlights
- Top revenue contributors
- Customer segment performance
- Portfolio health indicators

## Risk Factors
- Concentration risks and dependencies
- Customer portfolio vulnerabilities

## Strategic Recommendations
- Portfolio optimization strategies
- Customer diversification opportunities""",

            "monthly": f"""You are a business intelligence analyst. Analyze this monthly revenue trend and seasonality data.

Data Context:
{data_context}

Create a professional executive summary with:

## Key Insights
- Monthly growth patterns and trends
- Seasonal variations and consistency
- Revenue momentum analysis

## Performance Highlights
- Best performing months and growth rates
- Trend consistency and predictability
- Revenue acceleration patterns

## Risk Factors
- Volatility concerns and declining trends
- Seasonal risks

## Strategic Recommendations
- Growth forecasting and planning insights
- Seasonal optimization strategies"""
        }
        
        prompt = prompts.get(analysis_type, f"Analyze this {analysis_type} data and provide business insights.")
        
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a world-class financial analyst and business intelligence expert with 15+ years of experience in revenue operations, customer analytics, and strategic business planning. Provide actionable insights with specific metrics and recommendations."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1500,
            temperature=0.2
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        return generate_fallback_summary(json_data, analysis_type)

def generate_fallback_summary(json_data, analysis_type):
    """Fallback summary generation when AI is not available"""
    
    if analysis_type == "quarterly":
        if not json_data:
            return "No quarterly data available for analysis."
            
        total_customers = len(json_data)
        positive_growth = len([c for c in json_data if c.get('Percentage of Variance', 0) and c['Percentage of Variance'] > 0])
        top_performers = sorted([c for c in json_data if c.get('Percentage of Variance') is not None], 
                               key=lambda x: x.get('Percentage of Variance', 0), reverse=True)[:3]
        
        # Calculate percentage
        growth_percentage = (positive_growth/total_customers*100) if total_customers > 0 else 0
        top_performer_growth = top_performers[0].get('Percentage of Variance', 0) if top_performers else 0
        
        top_performer_name = top_performers[0]['Customer Name'] if top_performers else 'N/A'
        
        # Pre-calculate formatted strings to avoid f-string syntax issues
        growth_percentage_str = f"{growth_percentage:.1f}%"
        top_performer_growth_str = f"{top_performer_growth:.1f}%"
        
        summary = f"""## Key Insights
- Analyzed {total_customers} customers across Q3 to Q4 performance
- {positive_growth} customers ({growth_percentage_str}) showed positive growth
- Top performer: {top_performer_name} with {top_performer_growth_str} growth

## Performance Highlights
- Strong momentum in gaming and agency segments
- Mixed performance across geographic regions

## Strategic Recommendations
- Focus on replicating success patterns of top performers
- Investigate factors behind customer growth variance
"""
        return summary.strip()
    
    elif analysis_type == "bridge":
        if not json_data:
            return "No revenue bridge data available for analysis."
            
        total_customers = len(json_data)
        expansion_customers = len([c for c in json_data if c.get('Expansion Revenue', 0) > 0])
        total_expansion = sum(c.get('Expansion Revenue', 0) for c in json_data)
        
        # Pre-calculate expansion percentage and revenue to avoid f-string syntax issues
        expansion_pct = (expansion_customers/total_customers*100) if total_customers > 0 else 0
        expansion_pct_str = f"{expansion_pct:.1f}%"
        total_expansion_str = f"${total_expansion:,.2f}"
        
        summary = f"""
        ## Key Insights
        - {total_customers} customers analyzed for retention and expansion patterns
        - {expansion_customers} customers ({expansion_pct_str}) generated expansion revenue
        - Total expansion revenue: {total_expansion_str}
        
        ## Performance Highlights
        - Customer retention showing healthy expansion patterns
        - Positive revenue bridge dynamics
        
        ## Strategic Recommendations
        - Strengthen customer success programs
        - Focus on expansion revenue opportunities
        """
        return summary.strip()
    
    elif analysis_type == "geographic":
        if not json_data:
            return "No geographic data available for analysis."
            
        total_countries = len(json_data)
        total_revenue = sum(c.get('Yearly Revenue', 0) for c in json_data)
        top_countries = sorted(json_data, key=lambda x: x.get('Yearly Revenue', 0), reverse=True)[:5]
        
        # Pre-calculate formatted revenue strings to avoid f-string syntax issues
        total_revenue_str = f"${total_revenue:,.2f}"
        top_market_revenue = top_countries[0].get('Yearly Revenue', 0) if top_countries else 0
        top_market_revenue_str = f"${top_market_revenue:,.2f}"
        top_market_name = top_countries[0]['Country'] if top_countries else 'N/A'
        
        summary = f"""
        ## Key Insights
        - Revenue tracked across {total_countries} countries/regions
        - Total annual revenue: {total_revenue_str}
        - Top market: {top_market_name} ({top_market_revenue_str})
        
        ## Performance Highlights
        - Strong performance in India, Canada, and England markets
        - Opportunities for expansion in underserved regions
        
        ## Strategic Recommendations
        - Prioritize high-performing geographic markets
        - Develop market entry strategies for untapped regions
        """
        return summary.strip()
    
    # Default fallback for other types
    return f"""
    ## Key Insights
    - {len(json_data) if isinstance(json_data, list) else 'Multiple'} data points analyzed
    - Comprehensive analysis available for strategic decision making
    
    ## Strategic Recommendations
    - Review detailed data for specific insights
    - Consider trends and patterns for business optimization
    """

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
            self.client = OpenAI(api_key=self.api_key)
        else:
            self.client = None
    
    def get_response(self, user_question, tab_type, json_data, executive_summary):
        """Get context-aware response from OpenAI based on tab and full JSON data"""
        if not self.client:
            return "⚠️ OpenAI API key not configured. Please set OPENAI_API_KEY environment variable."
        
        # Create context-specific prompts for each tab with full JSON data
        context_prompts = {
            "quarterly": f"""You are a world-class financial analyst specializing in quarterly revenue performance and growth analysis with deep expertise in SaaS metrics and customer growth patterns.

            Executive Summary: {executive_summary}
            
            ANALYSIS FOCUS: Provide detailed analysis covering:
            • Customer-specific performance metrics and growth trajectories
            • Quarter-over-quarter growth patterns and variance analysis  
            • Seasonal impact assessment and trend identification
            • Top/bottom performer identification with specific metrics
            • Risk assessment for declining accounts and growth opportunities
            • Strategic recommendations for revenue optimization
            
            DATA ACCESS: Complete quarterly revenue dataset including customer names, Q3/Q4 revenue figures, absolute variance, percentage changes, and growth trajectories. Reference specific customers, dollar amounts, and percentages in your analysis.
            
            RESPONSE REQUIREMENTS: 
            • Reference specific customers by name with their exact revenue figures
            • Cite precise percentage changes and dollar amounts from the data
            • Identify top and bottom performers with specific metrics
            • Provide data-driven recommendations with supporting numbers
            • Use the format: "Customer X shows Y% growth ($Z revenue)" for specificity
            • Never make general statements without specific data backing.""",
            
            "bridge": f"""You are a senior revenue operations expert specializing in revenue bridge analysis, customer lifecycle management, and churn dynamics with extensive experience in subscription business models.

            Executive Summary: {executive_summary}
            
            ANALYSIS FOCUS: Provide comprehensive analysis covering:
            • Revenue bridge component analysis (expansion, contraction, churn, new)
            • Customer retention patterns and at-risk account identification
            • Expansion revenue opportunities and upselling potential
            • Churn analysis with specific customer impact assessment
            • Net revenue retention calculations and benchmarking
            • Strategic recommendations for revenue operations optimization
            
            DATA ACCESS: Complete revenue bridge dataset including churned revenue by customer, new customer revenue, expansion/upselling revenue, contraction amounts, and customer-specific transitions. Reference specific customers and dollar impacts.
            
            RESPONSE REQUIREMENTS:
            • Cite specific churned revenue amounts and customer names
            • Reference exact expansion/contraction figures with customer examples  
            • Calculate and state net revenue retention with supporting data
            • Identify at-risk customers with specific revenue impact numbers
            • Provide retention strategies based on actual customer patterns from the data.""",
            
            "geographic": f"""You are an expert market expansion strategist and international business development specialist with deep knowledge of global revenue optimization and geographic market analysis.

            Executive Summary: {executive_summary}
            
            ANALYSIS FOCUS: Provide strategic analysis covering:
            • Country-wise revenue performance and market penetration
            • Geographic diversification assessment and concentration risks
            • Market opportunity identification and expansion priorities
            • Currency/regional economic impact analysis
            • Competitive positioning by geography
            • International growth strategy recommendations
            
            DATA ACCESS: Complete geographic revenue dataset showing country-specific performance including revenue figures by region, market concentration data, and growth patterns across different international markets.
            
            RESPONSE REQUIREMENTS:
            • State exact revenue figures for each country/region mentioned
            • Calculate market concentration percentages with specific numbers
            • Rank countries by revenue performance with actual dollar amounts
            • Identify expansion opportunities with supporting revenue data
            • Reference specific countries and their contribution percentages.""",
            
            "customer": f"""You are a strategic customer success executive and portfolio risk analyst with extensive experience in customer concentration management, account strategy, and revenue diversification.

            Executive Summary: {executive_summary}
            
            ANALYSIS FOCUS: Provide comprehensive analysis covering:
            • Customer concentration risk assessment and portfolio diversification
            • Individual customer performance metrics and revenue contribution analysis
            • High-value account identification and strategic account management priorities
            • Customer segmentation based on revenue size and growth potential  
            • Risk mitigation strategies for over-concentrated customer dependencies
            • Account expansion opportunities and customer lifetime value optimization
            
            DATA ACCESS: Complete customer portfolio dataset including customer names, total revenue contributions, concentration percentages, account sizes, and performance metrics. Reference specific customer names and revenue figures.
            
            RESPONSE REQUIREMENTS:
            • Name specific customers with their exact revenue contributions
            • Calculate concentration risk percentages with supporting data
            • Identify top revenue contributors with dollar amounts and percentages
            • Assess customer diversification using actual portfolio numbers
            • Recommend account strategies based on specific customer performance data.""",
            
            "monthly": f"""You are a senior business intelligence analyst and revenue forecasting expert specializing in time-series analysis, seasonal business patterns, and monthly performance optimization.

            Executive Summary: {executive_summary}
            
            ANALYSIS FOCUS: Provide comprehensive analysis covering:
            • Month-over-month revenue trend analysis and pattern recognition
            • Seasonal variation identification and business cycle assessment
            • Growth trajectory forecasting and momentum analysis
            • Monthly variance analysis and performance consistency evaluation
            • Revenue seasonality impact and planning recommendations
            • Predictive insights for upcoming periods based on historical patterns
            
            DATA ACCESS: Complete monthly revenue dataset showing month-by-month performance, growth rates, variance analysis, and seasonal patterns. Reference specific months, revenue figures, and growth percentages.
            
            RESPONSE REQUIREMENTS:
            • Reference specific months with exact revenue figures and growth rates
            • Calculate month-over-month percentage changes with supporting numbers
            • Identify seasonal patterns with specific revenue data points
            • Cite highest/lowest performing months with actual dollar amounts
            • Provide forecasting insights based on historical data trends from the dataset."""
        }
        
        system_prompt = context_prompts.get(tab_type, "You are a financial analyst helping with investment analysis.")
        
        # Include comprehensive data context for better analysis
        if isinstance(json_data, list):
            # For list data, include more samples and summary statistics
            sample_data = json_data[:10]  # Increased from 5 to 10 samples
            total_records = len(json_data)
            data_summary = f"DATASET OVERVIEW: Total records: {total_records}\n"
            
            # Add column information if available
            if json_data:
                columns = list(json_data[0].keys()) if isinstance(json_data[0], dict) else []
                data_summary += f"Available columns: {', '.join(columns)}\n"
            
            data_context = f"{data_summary}\nSAMPLE DATA (First 10 records):\n{json.dumps(sample_data, indent=2)[:3000]}..."
        else:
            data_context = f"COMPLETE DATASET:\n{json.dumps(json_data, indent=2)[:3000]}..."
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": """You are a world-class senior investment analyst and revenue operations expert with 15+ years of experience in financial metrics, customer segmentation, and business intelligence. 

CRITICAL DATA ADHERENCE RULES:
- ONLY use information from the provided dataset - DO NOT add external information or assumptions
- ALWAYS cite specific customers, revenue figures, dollar amounts, and percentages from the actual data
- Reference exact data points, customer names, and metrics from the provided dataset
- If data is not available in the dataset, explicitly state "This information is not available in the provided data"
- Ground every insight in the actual numbers and facts from the dataset

RESPONSE FORMAT REQUIREMENTS:
- Provide comprehensive analysis in 2-3 well-structured paragraphs
- Use bullet points for key insights with specific data references
- Include exact metrics, percentages, and dollar figures from the actual data
- Reference specific customer names and performance figures
- Offer actionable business recommendations based solely on the provided data patterns
- Maintain professional, executive-level analysis quality
- Start responses with specific data observations before providing insights

EXAMPLE RESPONSE STRUCTURE:
"Based on the provided data, [specific customer/metric observation]. Key findings include: • [Specific data point with numbers] • [Another specific metric with customer names] • [Concrete recommendation based on data patterns]"

DO NOT provide vague or generic responses. Every statement must be backed by specific data from the provided dataset."""},
                    {"role": "system", "content": system_prompt},
                    {"role": "system", "content": data_context},
                    {"role": "user", "content": user_question}
                ],
                max_tokens=3000,
                temperature=0.4
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"⚠️ Error getting response: {str(e)}"

def create_beautiful_tab_layout(tab_name, json_data, tab_type):
    """Create beautiful layout for each analysis tab with enhanced display functions"""
    
    # Convert JSON to DataFrame for the display functions
    df = pd.DataFrame(json_data) if json_data else pd.DataFrame()
    
    # Generate AI-powered executive summary first (with schema if available)
    schema_analyzer = JSONSchemaAnalyzer()
    schema = schema_analyzer.analyze_json_schema(json_data, tab_type)
    executive_summary = generate_ai_executive_summary(json_data, tab_type, schema)
    
    # Call appropriate display function based on tab type
    if tab_type == "quarterly" and not df.empty:
        display_quarterly_analysis(df, json_data, "Quarterly Revenue")
        
    elif tab_type == "bridge" and not df.empty:
        display_churn_analysis(df, json_data, "Revenue Bridge")
        
    elif tab_type == "geographic" and not df.empty:
        display_country_analysis(df, json_data, "Country Analysis")
        
    elif tab_type == "customer" and not df.empty:
        display_customer_concentration_analysis(df, json_data, "Customer Concentration")
        
    elif tab_type == "monthly" and not df.empty:
        display_month_on_month_analysis(df, json_data, "Monthly Analysis")
        
    else:
        # Fallback for empty data
        st.header(f"📊 {tab_name}")
        st.warning("No data available for this analysis.")
        
        # Still show executive summary
        st.markdown("---")
        with st.expander("📋 Executive Summary", expanded=True):
            st.markdown(executive_summary if executive_summary else "No summary available.")

def create_beautiful_tab_layout_old(tab_name, json_data, tab_type):
    """Create beautiful layout for each analysis tab with charts and chatbot using real JSON data"""
    
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
    
    # Generate AI-powered executive summary
    executive_summary = generate_ai_executive_summary(json_data, tab_type)
    
    # Header
    st.header(f"📊 {tab_name}")
    
    # Data-specific visualizations based on real JSON structure (MOVED UP)
    if tab_type == "quarterly" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Calculate metrics from real data
        total_customers = len(json_data)
        positive_growth = len([c for c in json_data if c.get('Percentage of Variance', 0) and c['Percentage of Variance'] > 0])
        avg_growth = sum(c.get('Percentage of Variance', 0) for c in json_data if c.get('Percentage of Variance') is not None) / max(1, len([c for c in json_data if c.get('Percentage of Variance') is not None]))
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Customers", total_customers)
        with col2:
            st.metric("Positive Growth", f"{positive_growth}/{total_customers}")
        with col3:
            st.metric("Avg Growth Rate", f"{avg_growth:.1f}%")
        with col4:
            growth_rate = (positive_growth/total_customers*100) if total_customers > 0 else 0
            st.metric("Growth Rate", f"{growth_rate:.1f}%")
        
        # Top performers chart
        df = pd.DataFrame(json_data)
        top_performers = df.nlargest(10, 'Percentage of Variance')
        if not top_performers.empty:
            fig = px.bar(top_performers, x='Customer Name', y='Percentage of Variance',
                        title="📈 Top 10 Customer Growth Performers (Q3 to Q4)",
                        color='Percentage of Variance', color_continuous_scale='RdYlGn')
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "bridge" and json_data:
        st.header("🔄 Revenue Bridge & Churn Analysis")
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(json_data)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_churned = df['Churned Revenue'].sum() if 'Churned Revenue' in df.columns else 0
            st.metric("Total Churned Revenue", f"${total_churned:,.2f}")
            
        with col2:
            total_new = df['New Revenue'].sum() if 'New Revenue' in df.columns else 0
            st.metric("Total New Revenue", f"${total_new:,.2f}")
            
        with col3:
            total_expansion = df['Expansion Revenue'].sum() if 'Expansion Revenue' in df.columns else 0
            st.metric("Total Expansion Revenue", f"${total_expansion:,.2f}")
        
        # Revenue bridge waterfall chart
        st.subheader("Revenue Bridge Analysis")
        
        # Handle different possible column names and calculate totals
        q3_total = df['Quarter 3 Revenue'].sum() if 'Quarter 3 Revenue' in df.columns else (df['Q3 Revenue'].sum() if 'Q3 Revenue' in df.columns else 0)
        q4_total = df['Quarter 4 Revenue'].sum() if 'Quarter 4 Revenue' in df.columns else (df['Q4 Revenue'].sum() if 'Q4 Revenue' in df.columns else 0)
        new_total = df['New Revenue'].sum() if 'New Revenue' in df.columns else 0
        expansion_total = df['Expansion Revenue'].sum() if 'Expansion Revenue' in df.columns else 0
        contraction_total = df['Contraction Revenue'].sum() if 'Contraction Revenue' in df.columns else 0
        churned_total = df['Churned Revenue'].sum() if 'Churned Revenue' in df.columns else 0
        
        revenue_categories = ['Starting Revenue', 'New Revenue', 'Expansion Revenue', 
                             'Contraction Revenue', 'Churned Revenue', 'Ending Revenue']
        
        values = [q3_total, new_total, expansion_total, -contraction_total, -churned_total, q4_total]
        
        fig = go.Figure(go.Waterfall(
            name="Revenue Bridge",
            orientation="v",
            measure=["absolute", "relative", "relative", "relative", "relative", "total"],
            x=revenue_categories,
            text=[f"${v:,.0f}" for v in values],
            y=values,
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        ))
        
        fig.update_layout(title="Revenue Bridge: Quarter 3 to Quarter 4", showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.subheader("Customer-wise Revenue Bridge")
        st.dataframe(df, use_container_width=True)
    
    elif tab_type == "geographic" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Calculate geographic metrics
        total_countries = len(json_data)
        total_revenue = sum(c.get('Yearly Revenue', 0) for c in json_data)
        top_country = max(json_data, key=lambda x: x.get('Yearly Revenue', 0))
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Countries", total_countries)
        with col2:
            st.metric("Total Revenue", f"${total_revenue:,.0f}")
        with col3:
            st.metric("Top Market", top_country.get('Country', 'N/A'))
        with col4:
            st.metric("Top Revenue", f"${top_country.get('Yearly Revenue', 0):,.0f}")
        
        # Geographic charts
        df = pd.DataFrame(json_data)
        col1, col2 = st.columns(2)
        
        with col1:
            top_10 = df.nlargest(10, 'Yearly Revenue')
            fig = px.pie(top_10, values='Yearly Revenue', names='Country',
                       title="🌍 Top 10 Countries by Revenue")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(top_10, x='Country', y='Yearly Revenue',
                       title="📈 Revenue by Country (Top 10)",
                       color='Yearly Revenue', color_continuous_scale='Blues')
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    elif tab_type == "customer" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Customer analysis metrics (structure depends on actual JSON)
        total_customers = len(json_data)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Customers", total_customers)
        with col2:
            st.metric("Analysis Type", "Concentration")
        with col3:
            st.metric("Risk Assessment", "Available")
        with col4:
            st.metric("Data Points", len(json_data))
    
    elif tab_type == "monthly" and json_data:
        st.markdown("### 🎯 Key Metrics")
        
        # Monthly analysis metrics (structure depends on actual JSON)
        total_months = len(json_data) if isinstance(json_data, list) else 12
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Months", total_months)
        with col2:
            st.metric("Trend Analysis", "Available")
        with col3:
            st.metric("Data Points", len(json_data))
        with col4:
            st.metric("Seasonality", "Detected")
    
    # Executive Summary Section (MOVED DOWN after charts)
    st.markdown("---")
    with st.expander("📋 Executive Summary", expanded=True):
        st.markdown(executive_summary)
    
    # Enhanced chatbot interface with suggestion buttons
    st.markdown("---")
    st.markdown("### 💬 AI Data Analyst")
    st.markdown("Ask questions about the data, trends, insights, or get analysis recommendations.")
    
    # Initialize chatbot
    if f"chatbot_{tab_type}" not in st.session_state:
        st.session_state[f"chatbot_{tab_type}"] = OpenAIChatbot()
    
    # Initialize chat history for this specific tab
    chat_key = f"chat_history_{tab_type}"
    if chat_key not in st.session_state:
        st.session_state[chat_key] = []
    
    # Quick suggestion buttons
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📊 Key Insights", key=f"insights_{tab_type}"):
            st.session_state[f"pending_question_{chat_key}"] = "What are the key insights from this data?"
    with col2:
        if st.button("📈 Trends", key=f"trends_{tab_type}"):
            st.session_state[f"pending_question_{chat_key}"] = "What trends can you identify in this data?"
    with col3:
        if st.button("💡 Recommendations", key=f"recommendations_{tab_type}"):
            st.session_state[f"pending_question_{chat_key}"] = "What recommendations do you have based on this analysis?"
    
    # Chat input
    user_question = st.chat_input(f"Ask about your {tab_name} data...", key=f"chat_input_{tab_type}")
    
    # Check for pending question from buttons
    pending_key = f"pending_question_{chat_key}"
    if pending_key in st.session_state:
        user_question = st.session_state[pending_key]
        del st.session_state[pending_key]
    
    # Process user question
    if user_question:
        # Add user message to chat history
        st.session_state[chat_key].append({"role": "user", "content": user_question})
        
        # Generate AI response
        try:
            with st.spinner("🤖 Analyzing your data..."):
                response = st.session_state[f"chatbot_{tab_type}"].get_response(
                    user_question, tab_type, json_data, executive_summary
                )
                
                # Add AI response to chat history
                st.session_state[chat_key].append({"role": "assistant", "content": response})
                
        except Exception as e:
            error_msg = f"❌ Error: {str(e)}"
            st.session_state[chat_key].append({"role": "assistant", "content": error_msg})
    
    # Display chat history
    if st.session_state[chat_key]:
        st.markdown("### 💬 Chat History")
        for message in st.session_state[chat_key]:
            with st.chat_message(message["role"]):
                st.write(message["content"])
    else:
        st.info("👋 Start a conversation by asking a question or clicking one of the suggestion buttons above!")

def show_beautiful_analysis_interface(db, company_id, company_name):
    """Show the beautiful analysis interface with 5 tabs and OpenAI chatbots"""
    
    # Add company branding
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<h1 style='text-align: center; color: #1f77b4;'> Zenalyst.ai</h1>", unsafe_allow_html=True)
        st.markdown(f"<h3 style='text-align: center; color: #666;'>📊 {company_name} - Investment Analysis</h3>", unsafe_allow_html=True)
    
    # Back button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col3:
        if st.button("← Back to Portfolio"):
            # Clean up session state
            for key in list(st.session_state.keys()):
                if key.startswith(('show_analysis', 'analyzing_company', 'analysis_complete', 'analysis_results')):
                    del st.session_state[key]
            st.rerun()
    
    st.markdown("---")
    
    # Initialize S3 configuration
    s3_config = S3ConfigManager()
    
    # Show configuration status in sidebar
    with st.sidebar:
        ConfigValidator.show_config_status(s3_config)
    
    # Check if analysis is already completed
    if not hasattr(st.session_state, f'analysis_complete_{company_id}'):
        st.info("🚀 Starting comprehensive LLM analysis of your investment data...")
        
        # Show processing animation
        with st.container():
            st.subheader("🔄 Processing Investment Analysis")
            show_processing_animation()
        
        # Load data dynamically from S3 or local files
        use_s3 = s3_config.is_configured()
        # Check if we should force refresh (after refresh button click)
        force_refresh = st.session_state.get('force_s3_refresh', False)
        if force_refresh:
            st.session_state['force_s3_refresh'] = False  # Reset flag
        analysis_results = load_dynamic_json_analyses(s3_config, use_s3, force_refresh=force_refresh)
        
        # Mark analysis as complete and store results
        st.session_state[f'analysis_complete_{company_id}'] = True
        st.session_state[f'analysis_results_{company_id}'] = analysis_results
        st.session_state[f'use_s3_{company_id}'] = use_s3
        st.rerun()
    
    # Get analysis results
    analysis_results = st.session_state[f'analysis_results_{company_id}']
    use_s3 = st.session_state.get(f'use_s3_{company_id}', False)
    
    st.success("✅ Analysis Complete! Explore the detailed insights below:")
    
    # Show data source indicator with performance controls
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if use_s3:
            st.info(f"📊 Data loaded from S3 bucket: {s3_config.bucket_name}")
        else:
            st.info("📁 Data loaded from local files")
    
    with col2:
        if use_s3:
            if st.button("🔄 Refresh Data"):
                # Force refresh S3 discovery cache
                st.info("🔄 Forcing S3 re-scan for new files...")
                
                # Set flag to force refresh on next data load
                st.session_state['force_s3_refresh'] = True
                
                # Clear all cache layers
                cache_manager.clear_cache()
                
                # Clear more comprehensive session state keys for this company
                keys_to_remove = []
                for key in st.session_state.keys():
                    if any(key.startswith(prefix) for prefix in [
                        f'analysis_complete_{company_id}',
                        f'analysis_results_{company_id}',
                        f'use_s3_{company_id}',
                        f'chat_history_{company_id}',
                        'discovered_files',
                        'file_cache'
                    ]):
                        keys_to_remove.append(key)
                
                for key in keys_to_remove:
                    del st.session_state[key]
                    
                st.success("✅ Cache cleared! Re-scanning S3 for new files...")
                st.rerun()
        else:
            if st.button("🗑️ Clear Cache"):
                cache_manager.clear_cache()
                st.success("Cache cleared!")
    
    # Create dynamic tabs based on available data
    if not analysis_results:
        st.warning("No analysis data available")
        return
    
    # Generate tab names and emojis dynamically
    tab_config = {
        "quarterly": "📊 Quarterly Revenue",
        "bridge": "🌉 Revenue Bridge", 
        "geographic": "🌍 Geographic Analysis",
        "customer": "👥 Customer Analysis",
        "monthly": "📈 Monthly Trends",
        "general": "📋 General Analysis"
    }
    
    # Create tabs for available data
    available_categories = list(analysis_results.keys())
    tab_names = []
    
    for category in available_categories:
        if category in tab_config:
            tab_names.append(tab_config[category])
        else:
            # Dynamic naming for unknown categories
            tab_names.append(f"📊 {category.replace('_', ' ').title()}")
    
    if not tab_names:
        st.error("No valid data categories found")
        return
    
    # Create the dynamic tabs
    tabs = st.tabs(tab_names)
    
    # Initialize dynamic dashboard generator
    dashboard_generator = DynamicDashboardGenerator()
    
    # Generate content for each tab dynamically
    for i, (category, data) in enumerate(analysis_results.items()):
        with tabs[i]:
            try:
                # Extract data if it's wrapped in metadata structure
                if isinstance(data, list) and data and isinstance(data[0], dict) and 'data' in data[0]:
                    # S3 structure with metadata
                    actual_data = data[0]['data']
                    schema = data[0].get('schema')
                else:
                    # Direct data structure
                    actual_data = data
                    schema = None
                
                # Use dynamic dashboard generator or fallback to existing layout
                if actual_data:
                    if category in ["quarterly", "bridge", "geographic", "customer", "monthly"]:
                        # Use existing specialized layouts for known types
                        create_beautiful_tab_layout(
                            tab_names[i].replace("📊 ", "").replace("🌉 ", "").replace("🌍 ", "").replace("👥 ", "").replace("📈 ", ""),
                            actual_data,
                            category
                        )
                    else:
                        # Use dynamic generator for new/unknown data types
                        dashboard_generator.generate_tab_layout(
                            tab_names[i].replace("📊 ", "").replace("🌉 ", "").replace("🌍 ", "").replace("👥 ", "").replace("📈 ", ""),
                            actual_data,
                            category,
                            schema
                        )
                else:
                    st.warning(f"No data available for {category}")
                    
            except Exception as e:
                st.error(f"Error displaying {category}: {str(e)}")
                st.write("Debug info:")
                st.write(f"Data type: {type(data)}")
                if isinstance(data, list) and data:
                    st.write(f"First item type: {type(data[0])}")
                    st.write(f"First item keys: {data[0].keys() if isinstance(data[0], dict) else 'Not a dict'}")
    
    # Footer actions with working downloads
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📄 Generate Full Report", type="primary"):
            # Generate comprehensive PDF report
            pdf_data = generate_pdf_report(analysis_results, company_name)
            if pdf_data:
                st.download_button(
                    label="📥 Download PDF Report",
                    data=pdf_data,
                    file_name=f"{company_name}_Investment_Analysis_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf"
                )
            else:
                st.error("❌ Error generating PDF report")
    
    with col2:
        if st.button("💾 Save Analysis"):
            # Generate analysis JSON export
            json_data = save_analysis_as_json(analysis_results, company_name)
            if json_data:
                st.download_button(
                    label="📥 Download Analysis Data",
                    data=json_data,
                    file_name=f"{company_name}_Analysis_{datetime.now().strftime('%Y%m%d')}.json",
                    mime="application/json"
                )
            else:
                st.error("❌ Error generating analysis file")

def generate_pdf_report(analysis_results, company_name):
    """Generate downloadable PDF report with all analysis"""
    try:
        from reportlab.lib.pagesizes import letter, A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib import colors
        from io import BytesIO
        
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4)
        styles = getSampleStyleSheet()
        story = []
        
        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=1  # Center alignment
        )
        
        story.append(Paragraph("Zenalyst.ai", title_style))
        story.append(Paragraph(f"{company_name} - Investment Analysis Report", styles['Heading2']))
        story.append(Paragraph(f"Generated on {datetime.now().strftime('%B %d, %Y')}", styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Executive Summary Section
        story.append(Paragraph("Executive Summary", styles['Heading2']))
        
        for tab_name, data in [
            ("Quarterly Revenue Analysis", analysis_results.get("quarterly", [])),
            ("Revenue Bridge Analysis", analysis_results.get("bridge", [])),
            ("Geographic Analysis", analysis_results.get("geographic", [])),
            ("Customer Analysis", analysis_results.get("customer", [])),
            ("Monthly Trends Analysis", analysis_results.get("monthly", []))
        ]:
            if data:
                story.append(Paragraph(tab_name, styles['Heading3']))
                
                # Generate summary for this section
                if tab_name == "Quarterly Revenue Analysis":
                    total_customers = len(data)
                    positive_growth = len([c for c in data if c.get('Percentage of Variance', 0) and c['Percentage of Variance'] > 0])
                    summary_text = f"Analyzed {total_customers} customers with {positive_growth} showing positive growth ({positive_growth/total_customers*100:.1f}%)"
                elif tab_name == "Geographic Analysis":
                    total_countries = len(data)
                    total_revenue = sum(c.get('Yearly Revenue', 0) for c in data)
                    summary_text = f"Revenue tracked across {total_countries} countries with total revenue of ${total_revenue:,.2f}"
                else:
                    summary_text = f"Comprehensive analysis of {len(data)} data points providing strategic insights"
                
                story.append(Paragraph(summary_text, styles['Normal']))
                story.append(Spacer(1, 12))
        
        # Build PDF
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()
        
    except ImportError:
        # Fallback: Create simple text report if reportlab not available
        report_content = f"""
ZENALYST.AI - INVESTMENT ANALYSIS REPORT
{company_name}
Generated on {datetime.now().strftime('%B %d, %Y')}

=== EXECUTIVE SUMMARY ===

Quarterly Revenue Analysis:
- {len(analysis_results.get('quarterly', []))} customers analyzed
- Comprehensive growth and variance analysis

Revenue Bridge Analysis:
- {len(analysis_results.get('bridge', []))} customer retention patterns
- Expansion and churn analysis

Geographic Analysis:
- {len(analysis_results.get('geographic', []))} countries/regions
- Market performance and opportunities

Customer Analysis:
- {len(analysis_results.get('customer', []))} customer concentration data
- Portfolio diversification assessment

Monthly Trends Analysis:
- {len(analysis_results.get('monthly', []))} months of data
- Seasonal patterns and forecasting

=== DETAILED ANALYSIS ===
Full analysis data and insights available in the interactive dashboard.

Report generated by Zenalyst.ai Investment Analytics Platform
"""
        return report_content.encode('utf-8')
    except Exception as e:
        st.error(f"Error generating report: {str(e)}")
        return None

def save_analysis_as_json(analysis_results, company_name):
    """Save analysis as downloadable JSON with metadata"""
    try:
        analysis_export = {
            "company_name": company_name,
            "generated_timestamp": datetime.now().isoformat(),
            "generated_by": "Zenalyst.ai Investment Analytics",
            "analysis_data": analysis_results,
            "summary_statistics": {
                "quarterly_customers": len(analysis_results.get("quarterly", [])),
                "bridge_customers": len(analysis_results.get("bridge", [])),
                "geographic_markets": len(analysis_results.get("geographic", [])),
                "customer_records": len(analysis_results.get("customer", [])),
                "monthly_periods": len(analysis_results.get("monthly", []))
            }
        }
        
        return json.dumps(analysis_export, indent=2, default=str)
    except Exception as e:
        st.error(f"Error saving analysis: {str(e)}")
        return None

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
    
    # Universal AI Assistant in Sidebar
    st.sidebar.markdown("---")
    display_universal_chatbot()
    st.sidebar.markdown("---")
    
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
            col1, col2 = st.columns([4, 1])
            with col1:
                st.write(f"• {investor[1]}")
            with col2:
                if st.button("❌", key=f"remove_investor_{investor[0]}_{company_id}", help="Remove this connection"):
                    if db.remove_investor_company_connection(investor[0], company_id):
                        st.success(f"Removed connection with {investor[1]}")
                        st.rerun()
                    else:
                        st.error("Failed to remove connection")
    
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
    
    st.subheader("📁 Upload Files")
    
    # Initialize S3 file storage manager
    s3_storage = S3FileStorageManager()
    
    # File upload section - now supports multiple file types
    uploaded_files = st.file_uploader(
        "Upload your files", 
        type=['xlsx', 'xls', 'pdf', 'md'], 
        accept_multiple_files=True,
        help="Upload Excel files, PDF documents, or Markdown files for secure storage"
    )
    
    if uploaded_files:
        # Check if S3 storage is configured
        if not s3_storage.is_configured():
            st.error("🚫 S3 file storage is not configured. Please contact your administrator.")
            st.info("Required configuration: S3_FILE_STORAGE_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        else:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, uploaded_file in enumerate(uploaded_files):
                try:
                    file_name = uploaded_file.name
                    file_size = uploaded_file.size
                    file_type = file_name.split('.')[-1].lower()
                    
                    status_text.text(f"Uploading {file_name}...")
                    progress_bar.progress((i) / len(uploaded_files))
                    
                    # Upload file to S3
                    s3_key = s3_storage.upload_file(uploaded_file, company_id, file_name)
                    
                    # Save file metadata to database
                    file_id = db.save_uploaded_file(
                        company_id=company_id,
                        original_filename=file_name,
                        s3_key=s3_key,
                        file_type=file_type,
                        file_size=file_size
                    )
                    
                    progress_bar.progress((i + 1) / len(uploaded_files))
                    st.success(f"✅ {file_name} uploaded successfully!")
                    
                except Exception as e:
                    st.error(f"❌ Error uploading {uploaded_file.name}: {str(e)}")
                    
            status_text.text("Upload complete!")
            progress_bar.empty()
            status_text.empty()
    
    # Display uploaded files
    st.subheader("📂 Your Uploaded Files")
    uploaded_files_data = db.get_uploaded_files(company_id)
    
    if uploaded_files_data:
        # Initialize session state for delete confirmations
        if "delete_confirm" not in st.session_state:
            st.session_state.delete_confirm = {}
        
        for file_data in uploaded_files_data:
            file_id, original_filename, s3_key, file_type, file_size, upload_timestamp = file_data
            
            # Simple layout: filename, type, delete button
            col1, col2, col3 = st.columns([4, 1, 1])
            
            with col1:
                # File icon and name only
                icon = "📊" if file_type in ['xlsx', 'xls'] else "📄" if file_type == 'pdf' else "📝"
                st.write(f"{icon} **{original_filename}**")
            
            with col2:
                st.write(f"`.{file_type}`")
            
            with col3:
                # Delete button with confirmation
                delete_key = f"delete_{file_id}"
                
                if st.session_state.delete_confirm.get(delete_key, False):
                    # Confirmation buttons
                    if st.button("✅ Confirm", key=f"yes_{file_id}", help="Confirm deletion", type="primary"):
                        if s3_storage.delete_file(s3_key):
                            db.delete_uploaded_file(file_id)
                            st.session_state.delete_confirm[delete_key] = False
                            st.rerun()
                    
                    if st.button("❌ Cancel", key=f"no_{file_id}", help="Cancel deletion"):
                        st.session_state.delete_confirm[delete_key] = False
                        st.rerun()
                else:
                    # Regular delete button
                    if st.button("🗑️ Delete", key=delete_key, help=f"Delete {original_filename}"):
                        st.session_state.delete_confirm[delete_key] = True
                        st.rerun()
    else:
        st.info("No files uploaded yet.")

def investor_dashboard(db):
    st.title("💼 Investor Portfolio Dashboard")
    
    # Initialize S3 configuration and JSON reader for investor analytics
    s3_config = S3ConfigManager()
    s3_json_reader = S3JSONReader(s3_config)
    
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
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.write(f"• {comp[1]}")
            with col2:
                if st.button(f"Analyze", key=f"analyze_{comp[0]}"):
                    st.session_state.analyzing_company_id = comp[0]
                    st.session_state.analyzing_company_name = comp[1]
                    st.session_state.show_analysis = True
                    st.rerun()
            with col3:
                if st.button("❌", key=f"remove_company_{comp[0]}_{st.session_state.user_id}", help="Remove from portfolio"):
                    if db.remove_investor_company_connection(st.session_state.user_id, comp[0]):
                        st.success(f"Removed {comp[1]} from portfolio")
                        st.rerun()
                    else:
                        st.error("Failed to remove company")
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
        
        # Get company data from S3 instead of SQLite
        if not s3_config.is_configured():
            st.error("🚫 S3 analytics bucket is not configured. Please check your S3 settings.")
            st.info("Required: S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
            return
            
        with st.spinner("Loading data from S3..."):
            company_data = s3_json_reader.get_company_data_from_s3(selected_company_name)
        
        if not company_data:
            st.warning(f"No data available for {selected_company_name} in S3 bucket.")
            st.info("💡 Make sure JSON files for this company are uploaded to the S3 bucket 'json-for-streamlit'")
            return
        
        # Dynamic Dashboard Generation - adapts to any JSON structure
        if not company_data:
            st.warning("No data files found for this company in S3.")
            return
            
        # Initialize dynamic dashboard generator
        dashboard_generator = DynamicDashboardGenerator()
        schema_analyzer = JSONSchemaAnalyzer()
        
        # Create dynamic tabs based on available data files
        tab_names = []
        tab_schemas = {}
        
        for data_key, json_data in company_data.items():
            if json_data:  # Only create tabs for non-empty data
                try:
                    # Generate user-friendly tab name
                    tab_name = data_key.replace('_', ' ').title()
                    tab_names.append(tab_name)
                    
                    # Analyze schema for this data
                    schema = schema_analyzer.analyze_json_schema(json_data, data_key)
                    tab_schemas[tab_name] = {
                        'data': json_data,
                        'schema': schema,
                        'key': data_key
                    }
                except Exception as e:
                    st.error(f"❌ Error processing {data_key}: {str(e)}")
                    continue
        
        if not tab_names:
            st.warning("No valid data found in the JSON files.")
            return
            
        # Create dynamic tabs
        tabs = st.tabs(tab_names)
        
        # Generate content for each tab dynamically
        for i, tab_name in enumerate(tab_names):
            with tabs[i]:
                tab_info = tab_schemas[tab_name]
                json_data = tab_info['data']
                schema = tab_info['schema']
                data_key = tab_info['key']
                
                try:
                    # Generate complete tab layout using existing dynamic system
                    dashboard_generator.generate_tab_layout(
                        tab_name=tab_name,
                        json_data=json_data,
                        schema=schema
                    )
                    
                    # Chatbot is handled by display_chatbot_with_schema() at the end of each tab
                
                except Exception as e:
                    st.error(f"❌ Error displaying {tab_name}: {str(e)}")
                    st.write("💡 Please check the data format or contact support if this issue persists.")
    
def generate_adaptive_executive_summary(json_data, schema, tab_name):
    """Generate dynamic executive summary for any JSON data structure"""
    if not json_data or not isinstance(json_data, list) or len(json_data) == 0:
        return f"Analysis summary for {tab_name} - No data available for detailed analysis."
    
    try:
        df = pd.DataFrame(json_data)
        data_patterns = detect_business_patterns(json_data, schema, tab_name)
        
        # Generate summary based on detected patterns
        if 'revenue_bridge' in data_patterns:
            return generate_revenue_bridge_summary(df, tab_name)
        elif 'customer_analysis' in data_patterns:
            return generate_customer_analysis_summary(df, tab_name)
        elif 'geographic' in data_patterns:
            return generate_geographic_summary(df, tab_name)
        elif 'quarterly' in data_patterns:
            return generate_quarterly_summary(df, tab_name)
        elif 'time_series' in data_patterns:
            return generate_time_series_summary(df, tab_name)
        else:
            return generate_generic_business_summary(df, tab_name)
            
    except Exception as e:
        return f"Executive Summary for {tab_name}: Data contains {len(json_data)} records. Detailed analysis available through AI chat."

def detect_business_patterns(json_data, schema, tab_name):
    """Detect business data patterns for appropriate summary generation"""
    patterns = []
    
    if not json_data or not isinstance(json_data, list):
        return patterns
    
    # Analyze column names
    df = pd.DataFrame(json_data)
    columns = [col.lower() for col in df.columns]
    tab_lower = tab_name.lower()
    
    # Revenue bridge detection
    if any(term in ' '.join(columns) for term in ['new revenue', 'expansion', 'churn', 'contraction']) or 'bridge' in tab_lower:
        patterns.append('revenue_bridge')
    
    # Customer analysis detection
    elif any(term in ' '.join(columns) for term in ['customer name', 'customer', 'client']) or 'customer' in tab_lower:
        patterns.append('customer_analysis')
    
    # Geographic detection
    elif any(term in ' '.join(columns) for term in ['country', 'region', 'state', 'location']) or 'country' in tab_lower or 'geographic' in tab_lower:
        patterns.append('geographic')
    
    # Quarterly detection
    elif any(term in ' '.join(columns) for term in ['quarter 3', 'quarter 4', 'q3', 'q4']) or 'quarterly' in tab_lower:
        patterns.append('quarterly')
    
    # Time series detection
    elif any(term in ' '.join(columns) for term in ['month', 'date', 'time', 'year']) or 'monthly' in tab_lower:
        patterns.append('time_series')
    
    return patterns

def generate_revenue_bridge_summary(df, tab_name):
    """Generate summary for revenue bridge data"""
    try:
        # Find revenue columns
        revenue_cols = [col for col in df.columns if 'revenue' in col.lower()]
        if len(revenue_cols) >= 2:
            q3_col = next((col for col in revenue_cols if 'quarter 3' in col.lower() or 'q3' in col.lower()), revenue_cols[0])
            q4_col = next((col for col in revenue_cols if 'quarter 4' in col.lower() or 'q4' in col.lower()), revenue_cols[-1])
            
            q3_total = df[q3_col].sum()
            q4_total = df[q4_col].sum()
            growth = q4_total - q3_total
            growth_pct = (growth / q3_total * 100) if q3_total > 0 else 0
            
            return f"""**{tab_name} Executive Summary**

🎯 **Revenue Performance**: Q3 to Q4 revenue {'increased' if growth > 0 else 'decreased'} by ${abs(growth):,.0f} ({growth_pct:+.1f}%)

📊 **Key Metrics**:
• Q3 Revenue: ${q3_total:,.0f}
• Q4 Revenue: ${q4_total:,.0f}
• Net Change: ${growth:+,.0f}

🔍 **Analysis**: Revenue bridge analysis shows detailed flow from Q3 to Q4 with expansion, contraction, and churn components."""
        else:
            return f"**{tab_name} Executive Summary**: Revenue bridge data with {len(df)} records available for analysis."
    except:
        return f"**{tab_name} Executive Summary**: Revenue analysis data available for detailed review."

def generate_customer_analysis_summary(df, tab_name):
    """Generate summary for customer analysis data"""
    try:
        revenue_col = next((col for col in df.columns if 'revenue' in col.lower()), None)
        customer_col = next((col for col in df.columns if 'customer' in col.lower() or 'client' in col.lower()), None)
        
        if revenue_col and customer_col:
            total_revenue = df[revenue_col].sum()
            total_customers = len(df)
            avg_revenue = total_revenue / total_customers if total_customers > 0 else 0
            top_customer = df.loc[df[revenue_col].idxmax()]
            top_5_revenue = df.nlargest(5, revenue_col)[revenue_col].sum()
            concentration = (top_5_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            return f"""**{tab_name} Executive Summary**

💰 **Revenue Overview**: ${total_revenue:,.0f} across {total_customers} customers

🏆 **Top Performance**:
• Largest Customer: {top_customer[customer_col]} (${top_customer[revenue_col]:,.0f})
• Average Revenue per Customer: ${avg_revenue:,.0f}
• Top 5 Customer Concentration: {concentration:.1f}%

⚠️ **Risk Assessment**: {'High' if concentration > 80 else 'Medium' if concentration > 60 else 'Low'} concentration risk"""
        else:
            return f"**{tab_name} Executive Summary**: Customer analysis with {len(df)} records available."
    except:
        return f"**{tab_name} Executive Summary**: Customer data analysis available for detailed review."

def generate_geographic_summary(df, tab_name):
    """Generate summary for geographic data"""
    try:
        country_col = next((col for col in df.columns if 'country' in col.lower() or 'region' in col.lower()), None)
        revenue_col = next((col for col in df.columns if 'revenue' in col.lower()), None)
        
        if country_col and revenue_col:
            total_revenue = df[revenue_col].sum()
            countries_count = df[country_col].nunique()
            top_country = df.loc[df[revenue_col].idxmax()]
            top_3_revenue = df.nlargest(3, revenue_col)[revenue_col].sum()
            top_3_share = (top_3_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            return f"""**{tab_name} Executive Summary**

🌍 **Global Revenue**: ${total_revenue:,.0f} across {countries_count} markets

🥇 **Market Leaders**:
• Top Market: {top_country[country_col]} (${top_country[revenue_col]:,.0f})
• Top 3 Markets Share: {top_3_share:.1f}% of total revenue

📈 **Geographic Insights**: Revenue distribution analysis shows market concentration and expansion opportunities."""
        else:
            return f"**{tab_name} Executive Summary**: Geographic analysis with {len(df)} markets."
    except:
        return f"**{tab_name} Executive Summary**: Geographic revenue analysis available."

def generate_quarterly_summary(df, tab_name):
    """Generate summary for quarterly data"""
    try:
        q3_col = next((col for col in df.columns if 'quarter 3' in col.lower() or 'q3' in col.lower()), None)
        q4_col = next((col for col in df.columns if 'quarter 4' in col.lower() or 'q4' in col.lower()), None)
        
        if q3_col and q4_col:
            q3_total = df[q3_col].sum()
            q4_total = df[q4_col].sum()
            growth = q4_total - q3_total
            growth_pct = (growth / q3_total * 100) if q3_total > 0 else 0
            
            # Growth leaders
            df['growth'] = df[q4_col] - df[q3_col]
            top_growth = df.nlargest(1, 'growth').iloc[0]
            
            return f"""**{tab_name} Executive Summary**

📊 **Quarterly Performance**: {'Strong growth' if growth_pct > 10 else 'Moderate growth' if growth_pct > 0 else 'Decline'} from Q3 to Q4

🎯 **Key Results**:
• Q4 vs Q3 Growth: {growth_pct:+.1f}% (${growth:+,.0f})
• Total Q4 Revenue: ${q4_total:,.0f}
• Best Performer: {top_growth.iloc[0] if len(df.columns) > 0 else 'N/A'}

💡 **Insights**: Detailed quarter-over-quarter analysis reveals performance patterns and growth opportunities."""
        else:
            return f"**{tab_name} Executive Summary**: Quarterly performance data for {len(df)} entities."
    except:
        return f"**{tab_name} Executive Summary**: Quarterly analysis available for detailed review."

def generate_time_series_summary(df, tab_name):
    """Generate summary for time series data"""
    try:
        date_col = next((col for col in df.columns if 'month' in col.lower() or 'date' in col.lower()), None)
        revenue_col = next((col for col in df.columns if 'revenue' in col.lower() or 'amount' in col.lower()), None)
        
        if date_col and revenue_col:
            total_revenue = df[revenue_col].sum()
            periods = len(df)
            avg_period = total_revenue / periods if periods > 0 else 0
            
            # Growth calculation
            if periods >= 2:
                first_period = df[revenue_col].iloc[0]
                last_period = df[revenue_col].iloc[-1]
                total_growth = ((last_period - first_period) / first_period * 100) if first_period > 0 else 0
            else:
                total_growth = 0
            
            return f"""**{tab_name} Executive Summary**

📈 **Time Series Performance**: {periods} periods totaling ${total_revenue:,.0f}

🎯 **Trend Analysis**:
• Average per Period: ${avg_period:,.0f}
• Overall Growth: {total_growth:+.1f}%
• Data Span: {periods} time periods

💡 **Insights**: Time-based analysis reveals trends, seasonality, and growth patterns."""
        else:
            return f"**{tab_name} Executive Summary**: Time series data with {len(df)} data points."
    except:
        return f"**{tab_name} Executive Summary**: Time-based analysis available for review."

def generate_generic_business_summary(df, tab_name):
    """Generate generic business summary for unknown data types"""
    try:
        records = len(df)
        columns = len(df.columns)
        
        # Find numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            total_value = df[numeric_cols].sum().sum()
            avg_value = total_value / records if records > 0 else 0
            
            return f"""**{tab_name} Executive Summary**

📋 **Dataset Overview**: {records} records with {columns} attributes

📊 **Key Metrics**:
• Total Records: {records:,}
• Data Attributes: {columns}
• Numeric Data Points: {len(numeric_cols)}

💡 **Available Analysis**: Comprehensive data exploration and insights available through AI chat interface."""
        else:
            return f"""**{tab_name} Executive Summary**

📋 **Dataset**: {records} records with {columns} attributes available for analysis.

💡 **Insights**: Detailed analysis and insights available through the AI chat interface."""
    except:
        return f"**{tab_name} Executive Summary**: Business data analysis available for detailed review."

def _generate_dynamic_suggestions(schema, tab_name):
    """Generate relevant question suggestions based on schema analysis"""
    suggestions = []
    
    if not schema:
        return ["What insights can you provide about this data?"]
    
    data_type = schema.get('data_type', 'general')
    columns = schema.get('columns', {})
    
    # Generate suggestions based on detected data patterns
    if data_type == 'time_series' or any('date' in col_info.get('type', '') for col_info in columns.values()):
        suggestions.extend([
            "What are the key trends in this time series data?",
            "Show me the growth pattern over time",
            "What are the highest and lowest periods?"
        ])
    
    if any('revenue' in col.lower() for col in columns.keys()):
        suggestions.extend([
            "What is the total revenue and growth rate?",
            "Show me revenue performance analysis",
            "What factors influenced revenue changes?"
        ])
    
    if any('variance' in col.lower() for col in columns.keys()):
        suggestions.extend([
            "Explain the variance patterns",
            "What caused the significant variances?",
            "How does variance impact overall performance?"
        ])
    
    # Generic suggestions based on data type
    if data_type == 'categorical':
        suggestions.extend([
            f"What are the key categories in {tab_name}?",
            "Show me the distribution breakdown"
        ])
    elif data_type == 'numerical':
        suggestions.extend([
            "What are the key statistics and metrics?",
            "Show me the correlation analysis"
        ])
    
    # Default suggestions
    if not suggestions:
        suggestions = [
            f"What insights can you provide about {tab_name}?",
            "Summarize the key findings",
            "What are the most important metrics?"
        ]
    
    return suggestions[:5]  # Return top 5 suggestions

if __name__ == "__main__":
    main()