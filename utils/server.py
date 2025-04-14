"""
This module provides a simple HTTP server to serve files from a specified directory.
"""
import os
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import errno
from pathlib import Path
from functools import partial # Ensure partial is imported
from urllib.parse import unquote, urlparse # Added for path handling
import posixpath # Added for path handling in translate_path

_original_cwd = os.getcwd()


class DirectorySpecificHandler(SimpleHTTPRequestHandler):
    """Handler that serves files from a specific directory"""

    def __init__(self, *args, directory=None, **kwargs):
        if directory is None:
            directory = os.getcwd()
        self.directory = str(Path(directory).resolve())
        # Pass directory to super() if supported (Python 3.7+)
        super().__init__(*args, directory=self.directory, **kwargs)

    def translate_path(self, path):
        """Translate URL path to local filesystem path respecting the directory."""
        # Use superclass method first if possible (might handle some cases)
        # path_translated_by_super = super().translate_path(path) # Behavior varies by version

        # Re-implement based on self.directory for consistency
        path = posixpath.normpath(unquote(path))
        words = path.split('/')
        words = filter(None, words)
        request_path = self.directory # Start with our target directory
        for word in words:
            # Disallow navigating up or accessing special directories
            if os.path.dirname(word) or word in (os.curdir, os.pardir):
                continue
            request_path = os.path.join(request_path, word)
        return request_path


def serve_docs(index_path, port=8000):
    """
    Serve files from the directory containing the index.html file.
    Allows reusing the address quickly after stopping.

    Args:
        index_path (str or Path): Path to index.html file (file:// prefix is handled).
        port (int): Port to serve on (default: 8000).

    Returns:
        tuple: (HTTPServer, threading.Thread) or (None, None) if server fails to start.
    """
    try:
        if isinstance(index_path, str) and index_path.startswith("file://"):
            parsed = urlparse(index_path)
            index_path_obj = Path(unquote(parsed.path))
            if os.name == 'nt' and str(index_path_obj).startswith('/'):
                 index_path_obj = Path(str(index_path_obj)[1:]) # Handle /C:/ paths
        else:
            index_path_obj = Path(index_path)
        doc_directory = index_path_obj.resolve().parent
    except Exception as e:
         print(f"Error processing index path '{index_path}': {e}")
         return None, None

    if not doc_directory.is_dir():
        print(f"Error: Directory not found or invalid: {doc_directory}")
        return None, None

    doc_directory_str = str(doc_directory)
    Handler = partial(SimpleHTTPRequestHandler, directory=doc_directory_str)
    # If using custom handler for older Python or specific needs:
    # Handler = partial(DirectorySpecificHandler, directory=doc_directory_str)

    server = None
    try:
        # --- IMPORTANT FIX ---
        # Allow immediate reuse of the port after closing (bypass TIME_WAIT state).
        HTTPServer.allow_reuse_address = True
        # -------------------

        server = HTTPServer(('localhost', port), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()

        url = f"http://localhost:{port}/" # Serve directory root

        print(f"Serving directory '{doc_directory_str}' containing '{index_path_obj.name}'")
        print(f"Access documentation at: {url}")
        print(f"Server running on port {port}. Run stop_server(server, thread) to stop.")

        webbrowser.open(url)
        return server, thread

    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            print(f"Error: Port {port} is already in use (possibly by another application).")
        else:
            print(f"Error starting server: {e}")
        if server:
            server.server_close()
        return None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        if server:
            server.server_close()
        return None, None


def stop_server(server, thread):
    """
    Stops the web server gracefully.

    Args:
        server (HTTPServer): The server instance returned by serve_docs.
        thread (threading.Thread): The thread instance returned by serve_docs.
    """
    if server is None or thread is None:
        print("Server or thread instance is None. Cannot stop (maybe it failed to start?).")
        return

    print(f"Shutting down server on port {server.server_port}...")
    try:
        server.shutdown()  # Signal server thread to stop listening
        thread.join(timeout=5) # Wait for thread to finish
        if thread.is_alive():
            print("Warning: Server thread did not exit cleanly after 5 seconds.")
        server.server_close()  # Release the socket
        print(f"Server on port {server.server_port} stopped and port freed.")
    except Exception as e:
        print(f"Error during server shutdown: {e}")