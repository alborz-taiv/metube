#!/usr/bin/env python3
# pylint: disable=no-member,method-hidden

import os
import sys
import asyncio
from pathlib import Path
from aiohttp import web
from aiohttp.log import access_logger
import ssl
import socket
import socketio
import logging
import json
import pathlib
import re
from watchfiles import DefaultFilter, Change, awatch
from dotenv import load_dotenv

from ytdl import DownloadQueueNotifier, DownloadQueue
from yt_dlp.version import __version__ as yt_dlp_version
from s3_upload import create_s3_uploader, S3UploadError

# Load environment variables from .env file
load_dotenv()

log = logging.getLogger('main')

def parseLogLevel(logLevel):
    match logLevel:
        case 'DEBUG':
            return logging.DEBUG
        case 'INFO':
            return logging.INFO
        case 'WARNING':
            return logging.WARNING
        case 'ERROR':
            return logging.ERROR
        case 'CRITICAL':
            return logging.CRITICAL
        case _:
            return None

# Configure logging before Config() uses it so early messages are not dropped.
# Only configure if no handlers are set (avoid clobbering hosting app settings).
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=parseLogLevel(os.environ.get('LOGLEVEL', 'INFO')) or logging.INFO)

class Config:
    _DEFAULTS = {
        'DOWNLOAD_DIR': '.',
        'AUDIO_DOWNLOAD_DIR': '%%DOWNLOAD_DIR',
        'TEMP_DIR': '%%DOWNLOAD_DIR',
        'DOWNLOAD_DIRS_INDEXABLE': 'false',
        'CUSTOM_DIRS': 'true',
        'CREATE_CUSTOM_DIRS': 'true',
        'CUSTOM_DIRS_EXCLUDE_REGEX': r'(^|/)[.@].*$',
        'DELETE_FILE_ON_TRASHCAN': 'false',
        'STATE_DIR': '.',
        'URL_PREFIX': '',
        'PUBLIC_HOST_URL': 'download/',
        'PUBLIC_HOST_AUDIO_URL': 'audio_download/',
        'OUTPUT_TEMPLATE': '%(title)s.%(ext)s',
        'OUTPUT_TEMPLATE_CHAPTER': '%(title)s - %(section_number)02d - %(section_title)s.%(ext)s',
        'OUTPUT_TEMPLATE_PLAYLIST': '%(playlist_title)s/%(title)s.%(ext)s',
        'DEFAULT_OPTION_PLAYLIST_ITEM_LIMIT' : '0',
        'YTDL_OPTIONS': '{}',
        'YTDL_OPTIONS_FILE': '',
        'ROBOTS_TXT': '',
        'HOST': '0.0.0.0',
        'PORT': '8081',
        'HTTPS': 'false',
        'CERTFILE': '',
        'KEYFILE': '',
        'BASE_DIR': '',
        'DEFAULT_THEME': 'auto',
        'MAX_CONCURRENT_DOWNLOADS': 3,
        'LOGLEVEL': 'INFO',
        'ENABLE_ACCESSLOG': 'false',
        'S3_BUCKET': '',
        'S3_REGION': 'us-east-1',
        'S3_PREFIX': '',
        'S3_ENDPOINT_URL': '',
        'COOKIES_FROM_BROWSER': '',
        'SEGMENT_DURATION': '30',
    }

    _BOOLEAN = ('DOWNLOAD_DIRS_INDEXABLE', 'CUSTOM_DIRS', 'CREATE_CUSTOM_DIRS', 'DELETE_FILE_ON_TRASHCAN', 'HTTPS', 'ENABLE_ACCESSLOG')

    def __init__(self):
        for k, v in self._DEFAULTS.items():
            setattr(self, k, os.environ.get(k, v))

        for k, v in self.__dict__.items():
            if isinstance(v, str) and v.startswith('%%'):
                setattr(self, k, getattr(self, v[2:]))
            if k in self._BOOLEAN:
                if v not in ('true', 'false', 'True', 'False', 'on', 'off', '1', '0'):
                    log.error(f'Environment variable "{k}" is set to a non-boolean value "{v}"')
                    sys.exit(1)
                setattr(self, k, v in ('true', 'True', 'on', '1'))

        if not self.URL_PREFIX.endswith('/'):
            self.URL_PREFIX += '/'

        # Convert relative addresses to absolute addresses to prevent the failure of file address comparison
        if self.YTDL_OPTIONS_FILE and self.YTDL_OPTIONS_FILE.startswith('.'):
            self.YTDL_OPTIONS_FILE = str(Path(self.YTDL_OPTIONS_FILE).resolve())

        success,_ = self.load_ytdl_options()
        if not success:
            sys.exit(1)

    def load_ytdl_options(self) -> tuple[bool, str]:
        try:
            self.YTDL_OPTIONS = json.loads(os.environ.get('YTDL_OPTIONS', '{}'))
            assert isinstance(self.YTDL_OPTIONS, dict)
        except (json.decoder.JSONDecodeError, AssertionError):
            msg = 'Environment variable YTDL_OPTIONS is invalid'
            log.error(msg)
            return (False, msg)

        if not self.YTDL_OPTIONS_FILE:
            return (True, '')

        log.info(f'Loading yt-dlp custom options from "{self.YTDL_OPTIONS_FILE}"')
        if not os.path.exists(self.YTDL_OPTIONS_FILE):
            msg = f'File "{self.YTDL_OPTIONS_FILE}" not found'
            log.error(msg)
            return (False, msg)
        try:
            with open(self.YTDL_OPTIONS_FILE) as json_data:
                opts = json.load(json_data)
            assert isinstance(opts, dict)
        except (json.decoder.JSONDecodeError, AssertionError):
            msg = 'YTDL_OPTIONS_FILE contents is invalid'
            log.error(msg)
            return (False, msg)

        self.YTDL_OPTIONS.update(opts)
        return (True, '')

config = Config()
# Align root logger level with Config (keeps a single source of truth).
# This re-applies the log level after Config loads, in case LOGLEVEL was
# overridden by config file settings or differs from the environment variable.
logging.getLogger().setLevel(parseLogLevel(str(config.LOGLEVEL)) or logging.INFO)

class ObjectSerializer(json.JSONEncoder):
    def default(self, obj):
        # First try to use __dict__ for custom objects
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        # Convert iterables (generators, dict_items, etc.) to lists
        # Exclude strings and bytes which are also iterable
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes)):
            try:
                return list(obj)
            except:
                pass
        # Fall back to default behavior
        return json.JSONEncoder.default(self, obj)

serializer = ObjectSerializer()
app = web.Application()
sio = socketio.AsyncServer(cors_allowed_origins='*')
sio.attach(app, socketio_path=config.URL_PREFIX + 'socket.io')
routes = web.RouteTableDef()

class Notifier(DownloadQueueNotifier):
    async def added(self, dl):
        log.info(f"Notifier: Download added - {dl.title}")
        await sio.emit('added', serializer.encode(dl))

    async def updated(self, dl):
        log.debug(f"Notifier: Download updated - {dl.title}")
        await sio.emit('updated', serializer.encode(dl))

    async def completed(self, dl):
        log.info(f"Notifier: Download completed - {dl.title}")
        await sio.emit('completed', serializer.encode(dl))

    async def canceled(self, id):
        log.info(f"Notifier: Download canceled - {id}")
        await sio.emit('canceled', serializer.encode(id))

    async def cleared(self, id):
        log.info(f"Notifier: Download cleared - {id}")
        await sio.emit('cleared', serializer.encode(id))

dqueue = DownloadQueue(config, Notifier())
app.on_startup.append(lambda app: dqueue.initialize())

class FileOpsFilter(DefaultFilter):
    def __call__(self, change_type: int, path: str) -> bool:
        # Check if this path matches our YTDL_OPTIONS_FILE
        if path != config.YTDL_OPTIONS_FILE:
            return False

        # For existing files, use samefile comparison to handle symlinks correctly
        if os.path.exists(config.YTDL_OPTIONS_FILE):
            try:
                if not os.path.samefile(path, config.YTDL_OPTIONS_FILE):
                    return False
            except (OSError, IOError):
                # If samefile fails, fall back to string comparison
                if path != config.YTDL_OPTIONS_FILE:
                    return False

        # Accept all change types for our file: modified, added, deleted
        return change_type in (Change.modified, Change.added, Change.deleted)

def get_options_update_time(success=True, msg=''):
    result = {
        'success': success,
        'msg': msg,
        'update_time': None
    }

    # Only try to get file modification time if YTDL_OPTIONS_FILE is set and file exists
    if config.YTDL_OPTIONS_FILE and os.path.exists(config.YTDL_OPTIONS_FILE):
        try:
            result['update_time'] = os.path.getmtime(config.YTDL_OPTIONS_FILE)
        except (OSError, IOError) as e:
            log.warning(f"Could not get modification time for {config.YTDL_OPTIONS_FILE}: {e}")
            result['update_time'] = None

    return result

async def watch_files():
    async def _watch_files():
        async for changes in awatch(config.YTDL_OPTIONS_FILE, watch_filter=FileOpsFilter()):
            success, msg = config.load_ytdl_options()
            result = get_options_update_time(success, msg)
            await sio.emit('ytdl_options_changed', serializer.encode(result))

    log.info(f'Starting Watch File: {config.YTDL_OPTIONS_FILE}')
    asyncio.create_task(_watch_files())

if config.YTDL_OPTIONS_FILE:
    app.on_startup.append(lambda app: watch_files())

@routes.post(config.URL_PREFIX + 'add')
async def add(request):
    log.info("Received request to add download")
    post = await request.json()
    log.info(f"Request data: {post}")
    url = post.get('url')
    quality = post.get('quality')
    if not url or not quality:
        log.error("Bad request: missing 'url' or 'quality'")
        raise web.HTTPBadRequest()
    format = post.get('format')
    folder = post.get('folder')
    custom_name_prefix = post.get('custom_name_prefix')
    playlist_item_limit = post.get('playlist_item_limit')
    auto_start = post.get('auto_start')
    split_by_chapters = post.get('split_by_chapters')
    chapter_template = post.get('chapter_template')

    if custom_name_prefix is None:
        custom_name_prefix = ''
    if auto_start is None:
        auto_start = True
    if playlist_item_limit is None:
        playlist_item_limit = config.DEFAULT_OPTION_PLAYLIST_ITEM_LIMIT
    if split_by_chapters is None:
        split_by_chapters = False
    if chapter_template is None:
        chapter_template = config.OUTPUT_TEMPLATE_CHAPTER

    playlist_item_limit = int(playlist_item_limit)

    status = await dqueue.add(url, quality, format, folder, custom_name_prefix, playlist_item_limit, auto_start, split_by_chapters, chapter_template)
    return web.Response(text=serializer.encode(status))

@routes.post(config.URL_PREFIX + 'delete')
async def delete(request):
    post = await request.json()
    ids = post.get('ids')
    where = post.get('where')
    if not ids or where not in ['queue', 'done']:
        log.error("Bad request: missing 'ids' or incorrect 'where' value")
        raise web.HTTPBadRequest()
    status = await (dqueue.cancel(ids) if where == 'queue' else dqueue.clear(ids))
    log.info(f"Download delete request processed for ids: {ids}, where: {where}")
    return web.Response(text=serializer.encode(status))

@routes.post(config.URL_PREFIX + 'start')
async def start(request):
    post = await request.json()
    ids = post.get('ids')
    log.info(f"Received request to start pending downloads for ids: {ids}")
    status = await dqueue.start_pending(ids)
    return web.Response(text=serializer.encode(status))

@routes.post(config.URL_PREFIX + 'upload_to_s3')
async def upload_to_s3(request):
    post = await request.json()
    ids = post.get('ids')
    if not ids:
        log.error("Bad request: missing 'ids'")
        raise web.HTTPBadRequest()
    
    log.info(f"Received request to upload {len(ids)} files to S3")
    
    # Check if S3 is configured
    if not config.S3_BUCKET:
        log.error("S3 upload requested but S3_BUCKET is not configured")
        return web.Response(
            text=serializer.encode({
                'status': 'error',
                'msg': 'S3 is not configured. Please set S3_BUCKET environment variable.'
            })
        )
    
    # Create S3 uploader
    try:
        s3_uploader = create_s3_uploader(config)
        if not s3_uploader:
            raise S3UploadError("Failed to create S3 uploader")
    except S3UploadError as e:
        log.error(f"S3 uploader initialization failed: {str(e)}")
        return web.Response(
            text=serializer.encode({
                'status': 'error',
                'msg': f'S3 configuration error: {str(e)}'
            })
        )
    
    results = []
    
    for download_id in ids:
        if not dqueue.done.exists(download_id):
            log.warn(f'Requested S3 upload for non-existent download {download_id}')
            results.append({
                'id': download_id,
                'status': 'error',
                'msg': 'Download not found in completed queue'
            })
            continue
        
        dl = dqueue.done.get(download_id)
        
        # Check if download was successful
        if dl.info.status != 'finished':
            log.warn(f'Requested S3 upload for failed download {download_id}')
            results.append({
                'id': download_id,
                'status': 'error',
                'msg': f'Download status is {dl.info.status}, not finished'
            })
            continue
        
        # Check if we have a filename
        if not hasattr(dl.info, 'filename') or not dl.info.filename:
            log.warn(f'Download {download_id} has no filename')
            results.append({
                'id': download_id,
                'status': 'error',
                'msg': 'No filename available for this download'
            })
            continue
        
        # Build full file path
        dldirectory, error_message = dqueue._DownloadQueue__calc_download_path(
            dl.info.quality, dl.info.format, dl.info.folder
        )
        if error_message:
            results.append({
                'id': download_id,
                'status': 'error',
                'msg': error_message['msg']
            })
            continue
        
        file_path = os.path.join(dldirectory, dl.info.filename)
        
        # Build S3 key with video ID prefix to prevent collisions
        s3_filename = f"{dl.info.id}_{dl.info.filename}"
        if dl.info.folder:
            s3_key = f"{dl.info.folder}/{s3_filename}"
        else:
            s3_key = s3_filename
        
        # Upload main file
        success, msg, s3_url = s3_uploader.upload_file(file_path, s3_key)
        
        result = {
            'id': download_id,
            'title': dl.info.title,
            'status': 'success' if success else 'error',
            'msg': msg,
            'filename': dl.info.filename
        }
        
        if success:
            result['s3_url'] = s3_url
            
            # Delete local file after successful upload
            try:
                os.remove(file_path)
                log.info(f"Deleted local file after S3 upload: {file_path}")
            except Exception as e:
                log.warning(f"Failed to delete local file {file_path}: {str(e)}")
            
            # Upload chapter files if they exist
            if hasattr(dl.info, 'chapter_files') and dl.info.chapter_files:
                chapter_results = []
                for chapter_file in dl.info.chapter_files:
                    chapter_filename = chapter_file['filename']
                    chapter_path = os.path.join(dldirectory, chapter_filename)
                    
                    # Build S3 key for chapter file with video ID prefix
                    chapter_s3_filename = f"{dl.info.id}_{chapter_filename}"
                    if dl.info.folder:
                        chapter_s3_key = f"{dl.info.folder}/{chapter_s3_filename}"
                    else:
                        chapter_s3_key = chapter_s3_filename
                    
                    ch_success, ch_msg, ch_s3_url = s3_uploader.upload_file(chapter_path, chapter_s3_key)
                    
                    # Delete chapter file after successful upload
                    if ch_success:
                        try:
                            os.remove(chapter_path)
                            log.info(f"Deleted local chapter file after S3 upload: {chapter_path}")
                        except Exception as e:
                            log.warning(f"Failed to delete chapter file {chapter_path}: {str(e)}")
                    
                    chapter_results.append({
                        'filename': chapter_filename,
                        'status': 'success' if ch_success else 'error',
                        'msg': ch_msg,
                        's3_url': ch_s3_url if ch_success else None
                    })
                
                result['chapter_files'] = chapter_results
        
        results.append(result)
    
    # Summary
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'error')
    
    log.info(f"S3 upload complete: {successful} succeeded, {failed} failed")
    
    return web.Response(text=serializer.encode({
        'status': 'ok',
        'results': results,
        'summary': {
            'total': len(results),
            'successful': successful,
            'failed': failed
        }
    }))

@routes.get(config.URL_PREFIX + 'history')
async def history(request):
    history = { 'done': [], 'queue': [], 'pending': []}

    for _, v in dqueue.queue.saved_items():
        history['queue'].append(v)
    for _, v in dqueue.done.saved_items():
        history['done'].append(v)
    for _, v in dqueue.pending.saved_items():
        history['pending'].append(v)

    log.info("Sending download history")
    return web.Response(text=serializer.encode(history))

@sio.event
async def connect(sid, environ):
    log.info(f"Client connected: {sid}")
    await sio.emit('all', serializer.encode(dqueue.get()), to=sid)
    await sio.emit('configuration', serializer.encode(config), to=sid)
    if config.CUSTOM_DIRS:
        await sio.emit('custom_dirs', serializer.encode(get_custom_dirs()), to=sid)
    if config.YTDL_OPTIONS_FILE:
        await sio.emit('ytdl_options_changed', serializer.encode(get_options_update_time()), to=sid)

def get_custom_dirs():
    def recursive_dirs(base):
        path = pathlib.Path(base)

        # Converts PosixPath object to string, and remove base/ prefix
        def convert(p):
            s = str(p)
            if s.startswith(base):
                s = s[len(base):]

            if s.startswith('/'):
                s = s[1:]

            return s

        # Include only directories which do not match the exclude filter
        def include_dir(d):
            if len(config.CUSTOM_DIRS_EXCLUDE_REGEX) == 0:
                return True
            else:
                return re.search(config.CUSTOM_DIRS_EXCLUDE_REGEX, d) is None

        # Recursively lists all subdirectories of DOWNLOAD_DIR
        dirs = list(filter(include_dir, map(convert, path.glob('**/'))))

        return dirs

    download_dir = recursive_dirs(config.DOWNLOAD_DIR)

    audio_download_dir = download_dir
    if config.DOWNLOAD_DIR != config.AUDIO_DOWNLOAD_DIR:
        audio_download_dir = recursive_dirs(config.AUDIO_DOWNLOAD_DIR)

    return {
        "download_dir": download_dir,
        "audio_download_dir": audio_download_dir
    }

@routes.get(config.URL_PREFIX)
def index(request):
    response = web.FileResponse(os.path.join(config.BASE_DIR, 'ui/dist/metube/browser/index.html'))
    if 'metube_theme' not in request.cookies:
        response.set_cookie('metube_theme', config.DEFAULT_THEME)
    return response

@routes.get(config.URL_PREFIX + 'robots.txt')
def robots(request):
    if config.ROBOTS_TXT:
        response = web.FileResponse(os.path.join(config.BASE_DIR, config.ROBOTS_TXT))
    else:
        response = web.Response(
            text="User-agent: *\nDisallow: /download/\nDisallow: /audio_download/\n"
        )
    return response

@routes.get(config.URL_PREFIX + 'version')
def version(request):
    return web.json_response({
        "yt-dlp": yt_dlp_version,
        "version": os.getenv("METUBE_VERSION", "dev")
    })

if config.URL_PREFIX != '/':
    @routes.get('/')
    def index_redirect_root(request):
        return web.HTTPFound(config.URL_PREFIX)

    @routes.get(config.URL_PREFIX[:-1])
    def index_redirect_dir(request):
        return web.HTTPFound(config.URL_PREFIX)

routes.static(config.URL_PREFIX + 'download/', config.DOWNLOAD_DIR, show_index=config.DOWNLOAD_DIRS_INDEXABLE)
routes.static(config.URL_PREFIX + 'audio_download/', config.AUDIO_DOWNLOAD_DIR, show_index=config.DOWNLOAD_DIRS_INDEXABLE)
routes.static(config.URL_PREFIX, os.path.join(config.BASE_DIR, 'ui/dist/metube/browser'))
try:
    app.add_routes(routes)
except ValueError as e:
    if 'ui/dist/metube/browser' in str(e):
        raise RuntimeError('Could not find the frontend UI static assets. Please run `node_modules/.bin/ng build` inside the ui folder') from e
    raise e

# https://github.com/aio-libs/aiohttp/pull/4615 waiting for release
# @routes.options(config.URL_PREFIX + 'add')
async def add_cors(request):
    return web.Response(text=serializer.encode({"status": "ok"}))

app.router.add_route('OPTIONS', config.URL_PREFIX + 'add', add_cors)

async def on_prepare(request, response):
    if 'Origin' in request.headers:
        response.headers['Access-Control-Allow-Origin'] = request.headers['Origin']
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'

app.on_response_prepare.append(on_prepare)
 
def supports_reuse_port():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.close()
        return True
    except (AttributeError, OSError):
        return False

def isAccessLogEnabled():
    if config.ENABLE_ACCESSLOG:
        return access_logger
    else:
        return None

if __name__ == '__main__':
    logging.getLogger().setLevel(parseLogLevel(config.LOGLEVEL) or logging.INFO)
    log.info(f"Listening on {config.HOST}:{config.PORT}")

    if config.HTTPS:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(certfile=config.CERTFILE, keyfile=config.KEYFILE)
        web.run_app(app, host=config.HOST, port=int(config.PORT), reuse_port=supports_reuse_port(), ssl_context=ssl_context, access_log=isAccessLogEnabled())
    else:
        web.run_app(app, host=config.HOST, port=int(config.PORT), reuse_port=supports_reuse_port(), access_log=isAccessLogEnabled())
