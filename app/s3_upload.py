import os
import logging
from typing import Optional, Tuple
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

log = logging.getLogger('s3_upload')

class S3UploadError(Exception):
    """Custom exception for S3 upload errors"""
    pass

class S3Uploader:
    """Handles S3 upload operations"""
    
    def __init__(self, bucket: str, region: str = 'us-east-1', prefix: str = '', endpoint_url: Optional[str] = None):
        """
        Initialize S3 uploader
        
        Args:
            bucket: S3 bucket name
            region: AWS region
            prefix: Optional prefix for S3 keys
            endpoint_url: Optional custom S3 endpoint (for S3-compatible services)
        """
        self.bucket = bucket
        self.region = region
        self.prefix = prefix
        self.endpoint_url = endpoint_url
        
        if not self.bucket:
            raise S3UploadError("S3_BUCKET is not configured")
        
        try:
            # Initialize S3 client
            client_kwargs = {'region_name': self.region}
            if self.endpoint_url:
                client_kwargs['endpoint_url'] = self.endpoint_url
            
            self.s3_client = boto3.client('s3', **client_kwargs)
            
            # Verify credentials by attempting to access the bucket
            self.s3_client.head_bucket(Bucket=self.bucket)
            log.info(f"S3 uploader initialized for bucket: {self.bucket}")
            
        except NoCredentialsError:
            raise S3UploadError("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        except PartialCredentialsError:
            raise S3UploadError("Incomplete AWS credentials. Please set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                raise S3UploadError(f"S3 bucket '{self.bucket}' does not exist")
            elif error_code == '403':
                raise S3UploadError(f"Access denied to S3 bucket '{self.bucket}'. Check your permissions")
            else:
                raise S3UploadError(f"S3 error: {error_code} - {str(e)}")
        except Exception as e:
            raise S3UploadError(f"Failed to initialize S3 client: {str(e)}")
    
    def upload_file(self, file_path: str, s3_key: str) -> Tuple[bool, str, Optional[str]]:
        """
        Upload a file to S3
        
        Args:
            file_path: Local file path to upload
            s3_key: S3 key (path) for the uploaded file
        
        Returns:
            Tuple of (success: bool, message: str, s3_url: Optional[str])
        """
        if not os.path.exists(file_path):
            log.error(f"File not found: {file_path}")
            return False, f"File not found: {file_path}", None
        
        if not os.path.isfile(file_path):
            log.error(f"Path is not a file: {file_path}")
            return False, f"Path is not a file: {file_path}", None
        
        # Build full S3 key with prefix
        if self.prefix:
            full_s3_key = f"{self.prefix.rstrip('/')}/{s3_key.lstrip('/')}"
        else:
            full_s3_key = s3_key.lstrip('/')
        
        try:
            # Get file size for logging
            file_size = os.path.getsize(file_path)
            log.info(f"Uploading {file_path} ({file_size} bytes) to s3://{self.bucket}/{full_s3_key}")
            
            # Upload file
            self.s3_client.upload_file(
                file_path,
                self.bucket,
                full_s3_key,
                ExtraArgs={'ContentType': self._guess_content_type(file_path)}
            )
            
            # Generate S3 URL
            if self.endpoint_url:
                s3_url = f"{self.endpoint_url}/{self.bucket}/{full_s3_key}"
            else:
                s3_url = f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{full_s3_key}"
            
            log.info(f"Successfully uploaded to {s3_url}")
            return True, "Upload successful", s3_url
            
        except ClientError as e:
            error_msg = f"S3 upload error: {str(e)}"
            log.error(error_msg)
            return False, error_msg, None
        except Exception as e:
            error_msg = f"Unexpected error during upload: {str(e)}"
            log.error(error_msg)
            return False, error_msg, None
    
    def _guess_content_type(self, file_path: str) -> str:
        """Guess content type based on file extension"""
        ext = os.path.splitext(file_path)[1].lower()
        content_types = {
            '.mp4': 'video/mp4',
            '.mkv': 'video/x-matroska',
            '.webm': 'video/webm',
            '.avi': 'video/x-msvideo',
            '.mov': 'video/quicktime',
            '.flv': 'video/x-flv',
            '.wmv': 'video/x-ms-wmv',
            '.m4v': 'video/x-m4v',
            '.mp3': 'audio/mpeg',
            '.m4a': 'audio/mp4',
            '.opus': 'audio/opus',
            '.wav': 'audio/wav',
            '.flac': 'audio/flac',
            '.ogg': 'audio/ogg',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.webp': 'image/webp',
        }
        return content_types.get(ext, 'application/octet-stream')


def create_s3_uploader(config) -> Optional[S3Uploader]:
    """
    Create S3Uploader instance from config
    
    Args:
        config: Config object with S3 settings
    
    Returns:
        S3Uploader instance or None if S3 is not configured
    """
    if not config.S3_BUCKET:
        return None
    
    try:
        return S3Uploader(
            bucket=config.S3_BUCKET,
            region=config.S3_REGION,
            prefix=config.S3_PREFIX,
            endpoint_url=config.S3_ENDPOINT_URL if config.S3_ENDPOINT_URL else None
        )
    except S3UploadError as e:
        log.error(f"Failed to create S3 uploader: {str(e)}")
        raise
